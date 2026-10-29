#include "planning/hive_field_conversion.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static constexpr const char *FIELD = "__hive_field";

// LazyInteger and LazyLong: an optional sign and digits, a fraction dropped ('12.9' is 12)
static constexpr const char *INTEGER_PARSE = R"(
CASE WHEN regexp_full_match(__hive_field, '[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)')
THEN TRY_CAST(regexp_replace(__hive_field, '^([+-]?)([0-9]*).*$', '\10\2') AS %s) END)";

// LazyDouble: Java's parseDouble; LazyUtils.isNumberMaybe makes 4 characters starting with N NULL
static constexpr const char *FLOAT_PARSE = R"(
CASE WHEN regexp_full_match(__hive_field,
	'[\x00-\x20]*[+-]?(NaN|Infinity|([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?[fFdD]?)[\x00-\x20]*')
	AND NOT regexp_full_match(__hive_field, '(?s)[Nn]...')
THEN TRY_CAST(regexp_replace(__hive_field, '^[\x00-\x20]*(.*?)[fFdD]?[\x00-\x20]*$', '\1') AS %s) END)";

static constexpr const char *BOOLEAN_PARSE = R"(
CASE lower(__hive_field) WHEN 'true' THEN true WHEN 'false' THEN false END)";

// LazyDate: trimmed, cut at the first space, y-M-d resolved leniently (2024-02-30 is 2024-03-01)
static constexpr const char *DATE_PARSE = R"(
CASE WHEN strlen(__hive_field) >= 8
	AND regexp_full_match(__hive_field, '(?s)[\x00-\x20]*[0-9]{1,10}-[0-9]{1,2}-[0-9]{1,2}([\x00-\x20]*| .*)')
THEN TRY(CAST(make_date(CAST(regexp_extract(__hive_field, '([0-9]+)-([0-9]+)-([0-9]+)', 1) AS BIGINT), 1, 1)
	+ to_months(CAST(regexp_extract(__hive_field, '([0-9]+)-([0-9]+)-([0-9]+)', 2) AS INTEGER) - 1)
	+ to_days(CAST(regexp_extract(__hive_field, '([0-9]+)-([0-9]+)-([0-9]+)', 3) AS INTEGER) - 1) AS DATE)) END)";

// LazyTimestamp: y-M-d H:m:s[.f] resolved leniently, else ISO yyyy-MM-ddTHH:mm[:ss[.f]]
static constexpr const char *TIMESTAMP_PARSE = R"(
CASE WHEN strlen(__hive_field) >= 8 AND regexp_full_match(__hive_field,
	'[\x00-\x20]*[0-9]{1,10}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\.[0-9]{1,9})?[\x00-\x20]*')
THEN TRY(CAST(make_date(CAST(regexp_extract(__hive_field, '([0-9]+)-([0-9]+)-([0-9]+)', 1) AS BIGINT), 1, 1) AS TIMESTAMP)
	+ to_months(CAST(regexp_extract(__hive_field, '([0-9]+)-([0-9]+)-([0-9]+)', 2) AS INTEGER) - 1)
	+ to_days(CAST(regexp_extract(__hive_field, '([0-9]+)-([0-9]+)-([0-9]+)', 3) AS INTEGER) - 1)
	+ to_hours(CAST(regexp_extract(__hive_field, ' ([0-9]+):([0-9]+):([0-9]+)', 1) AS BIGINT))
	+ to_minutes(CAST(regexp_extract(__hive_field, ' ([0-9]+):([0-9]+):([0-9]+)', 2) AS BIGINT))
	+ to_seconds(CAST(regexp_extract(__hive_field, ' ([0-9]+):([0-9]+):([0-9]+)', 3) AS BIGINT))
	+ to_microseconds(CAST(left(regexp_extract(__hive_field, '\.([0-9]+)', 1) || '000000', 6) AS BIGINT)))
WHEN strlen(__hive_field) >= 8 AND regexp_full_match(__hive_field,
	'[\x00-\x20]*[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{0,9})?)?[\x00-\x20]*')
THEN TRY_CAST(regexp_replace(__hive_field, '^[\x00-\x20]*(.*?)[\x00-\x20]*$', '\1') AS TIMESTAMP) END)";

static string ConversionSQL(HiveFieldConversion conversion, const LogicalType &type) {
	switch (conversion) {
	case HiveFieldConversion::LAZY_SIMPLE_INTEGER:
		return StringUtil::Format(INTEGER_PARSE, type.ToString());
	case HiveFieldConversion::LAZY_SIMPLE_FLOAT:
		return StringUtil::Format(FLOAT_PARSE, type.ToString());
	case HiveFieldConversion::LAZY_SIMPLE_BOOLEAN:
		return BOOLEAN_PARSE;
	case HiveFieldConversion::LAZY_SIMPLE_DATE:
		return DATE_PARSE;
	case HiveFieldConversion::LAZY_SIMPLE_TIMESTAMP:
		return TIMESTAMP_PARSE;
	default:
		throw InternalException("Hive field conversion %d is not a LazySimpleSerDe parse",
		                        static_cast<int>(conversion));
	}
}

static bool IsField(const ParsedExpression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::COLUMN_REF &&
	       expr.Cast<ColumnRefExpression>().GetColumnName() == FIELD;
}

static void ReplaceField(unique_ptr<ParsedExpression> &expr, const ParsedExpression &field) {
	if (IsField(*expr)) {
		expr = field.Copy();
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceField(child, field); });
}

static unique_ptr<ParsedExpression> ParseConversion(HiveFieldConversion conversion, const LogicalType &type) {
	return Parser::GetBuiltinParser().ParseSingleExpression(ConversionSQL(conversion, type));
}

unique_ptr<ParsedExpression> HiveFieldConversionExpression(HiveFieldConversion conversion, const LogicalType &type,
                                                           const ParsedExpression &field) {
	auto expr = ParseConversion(conversion, type);
	ReplaceField(expr, field);
	return expr;
}

namespace {

class HiveFieldBinder : public ExpressionBinder {
public:
	HiveFieldBinder(Binder &binder, ClientContext &context, const Expression &field)
	    : ExpressionBinder(binder, context), field(field) {
	}

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override {
		if (IsField(*expr_ptr)) {
			return BindResult(field.Copy());
		}
		return ExpressionBinder::BindExpression(expr_ptr, depth, root_expression);
	}

private:
	const Expression &field;
};

} // namespace

unique_ptr<Expression> BindHiveFieldConversion(ClientContext &context, HiveFieldConversion conversion,
                                               const LogicalType &type, const Expression &field) {
	auto expr = ParseConversion(conversion, type);
	auto binder = Binder::CreateBinder(context);
	HiveFieldBinder field_binder(*binder, context, field);
	LogicalType result_type;
	auto result = field_binder.Bind(expr, &result_type);
	if (result->GetReturnType() != type) {
		result = BoundCastExpression::AddCastToType(context, std::move(result), type);
	}
	return result;
}

} // namespace duckdb
