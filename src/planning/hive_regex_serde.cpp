#include "planning/hive_regex_serde.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "planning/hive_multi_file_reader.hpp"
#include "re2/re2.h"

namespace duckdb {

using duckdb_re2::RE2;
using duckdb_re2::StringPiece;

static constexpr const char *HIVE_REGEX_GROUP_FUNCTION = "hive_regex_group";

//! The types RegexSerDe converts a group to; anything else is refused when the table is bound, like Hive does
static bool IsRegexSerDeType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
		return true;
	default:
		return false;
	}
}

shared_ptr<HiveRegexSerDe> HiveRegexSerDe::Create(const HiveScanInfo &info) {
	if (info.regex.empty()) {
		throw BinderException("Hive table %s uses RegexSerDe but has no 'input.regex' SerDe property", info.Describe());
	}
	idx_t data_columns = 0;
	for (idx_t i = 0; i < info.names.size(); i++) {
		auto &name = info.names[i].GetIdentifierName();
		if (info.GetPartitionKeyIndex(name) != DConstants::INVALID_INDEX) {
			continue;
		}
		if (!IsRegexSerDeType(info.types[i])) {
			throw NotImplementedException("Hive table %s uses RegexSerDe, which reads columns of primitive types only, "
			                              "but column '%s' is %s",
			                              info.Describe(), name, info.types[i].ToString());
		}
		data_columns++;
	}
	// Hive compiles the regex with Pattern.DOTALL, and with CASE_INSENSITIVE for 'input.regex.case.insensitive'
	RE2::Options options;
	options.set_dot_nl(true);
	options.set_case_sensitive(!info.regex_case_insensitive);
	options.set_log_errors(false);
	auto result = make_shared_ptr<HiveRegexSerDe>();
	result->regex = make_shared_ptr<RE2>(info.regex, options);
	if (!result->regex->ok()) {
		throw BinderException("The 'input.regex' of Hive table %s can not be compiled: %s", info.Describe(),
		                      result->regex->error());
	}
	auto groups = NumericCast<idx_t>(result->regex->NumberOfCapturingGroups());
	if (groups != data_columns) {
		throw BinderException("The 'input.regex' of Hive table %s has %d capture groups, but the table has %d data "
		                      "columns: RegexSerDe reads one group per column",
		                      info.Describe(), groups, data_columns);
	}
	return result;
}

//! Write the groups of every line: a line the regex does not match is NULL in every result, a group that did not take
//! part in the match is NULL, and a group that does not convert to its result's type is NULL
static void ExtractGroups(ClientContext &context, const RE2 &regex, Vector &lines, idx_t count,
                          const vector<idx_t> &groups, const vector<reference<Vector>> &results) {
	D_ASSERT(groups.size() == results.size());
	// a group is written as a string, into the result itself when it is VARCHAR, and cast afterwards otherwise;
	// BOOLEAN is the exception, which Hive reads with Boolean.valueOf: 'true' in any case, everything else false
	vector<unique_ptr<Vector>> strings(results.size());
	vector<optional_idx> string_writer(results.size());
	vector<optional_idx> bool_writer(results.size());
	{
		vector<VectorWriter<string_t>> string_writers;
		vector<VectorWriter<bool>> bool_writers;
		for (idx_t i = 0; i < results.size(); i++) {
			auto &result = results[i].get();
			auto &type = result.GetType();
			if (type.id() == LogicalTypeId::BOOLEAN) {
				bool_writer[i] = bool_writers.size();
				bool_writers.push_back(FlatVector::Writer<bool>(result, count));
				continue;
			}
			string_writer[i] = string_writers.size();
			if (type.id() == LogicalTypeId::VARCHAR) {
				string_writers.push_back(FlatVector::Writer<string_t>(result, count));
				continue;
			}
			strings[i] = make_uniq<Vector>(LogicalType::VARCHAR, count);
			string_writers.push_back(FlatVector::Writer<string_t>(*strings[i], count));
		}
		auto group_count = regex.NumberOfCapturingGroups();
		vector<StringPiece> pieces(NumericCast<idx_t>(group_count) + 1);
		auto values = lines.Values<string_t>();
		for (auto entry : values) {
			// the groups point into the line, which holds a short string itself: it must outlive them
			string_t line;
			bool matched = false;
			if (entry.IsValid()) {
				line = entry.GetValue();
				// RegexSerDe matches the whole line (Matcher.matches)
				matched = regex.Match(StringPiece(line.GetData(), line.GetSize()), 0, line.GetSize(), RE2::ANCHOR_BOTH,
				                      pieces.data(), group_count + 1);
			}
			for (idx_t i = 0; i < results.size(); i++) {
				auto &piece = pieces[groups[i]];
				if (bool_writer[i].IsValid()) {
					auto &writer = bool_writers[bool_writer[i].GetIndex()];
					if (!matched) {
						writer.WriteNull();
						continue;
					}
					writer.WriteValue(piece.data() && piece.size() == 4 &&
					                  StringUtil::CIEquals(string(piece.data(), piece.size()), "true"));
					continue;
				}
				auto &writer = string_writers[string_writer[i].GetIndex()];
				if (!matched || !piece.data()) {
					writer.WriteNull();
					continue;
				}
				writer.WriteValue(string_t(piece.data(), NumericCast<uint32_t>(piece.size())));
			}
		}
	}
	for (idx_t i = 0; i < results.size(); i++) {
		if (!strings[i]) {
			continue;
		}
		// Hive reads a group that does not parse as NULL; with an error message the cast does the same per row
		string error;
		VectorOperations::TryCast(context, *strings[i], results[i].get(), count, &error);
	}
}

struct HiveRegexGroupData : public FunctionData {
	HiveRegexGroupData(shared_ptr<RE2> regex_p, idx_t group_p) : regex(std::move(regex_p)), group(group_p) {
	}
	shared_ptr<RE2> regex;
	idx_t group;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<HiveRegexGroupData>(regex, group);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<HiveRegexGroupData>();
		return regex == other.regex && group == other.group;
	}
};

//! One group of every line; HiveMultiFileReader::FinalizeChunk evaluates all groups of a scan with one match instead
static void HiveRegexGroupFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<HiveRegexGroupData>();
	ExtractGroups(state.GetContext(), *data.regex, args.data[0], args.size(), {data.group}, {result});
}

unique_ptr<Expression> HiveRegexSerDe::GroupExpression(idx_t group, const LogicalType &type, idx_t line_index) const {
	D_ASSERT(group > 0 && group <= NumericCast<idx_t>(regex->NumberOfCapturingGroups()));
	ScalarFunction function(HIVE_REGEX_GROUP_FUNCTION, {LogicalType::VARCHAR}, type, HiveRegexGroupFunction);
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, line_index));
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(children),
	                                          make_uniq<HiveRegexGroupData>(regex, group));
}

bool HiveRegexSerDe::IsGroupExpression(const Expression &expr, idx_t &group, idx_t &line_index) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &function = expr.Cast<BoundFunctionExpression>();
	auto data = dynamic_cast<const HiveRegexGroupData *>(function.BindInfo().get());
	if (!data) {
		return false;
	}
	group = data->group;
	line_index = function.GetChildren()[0]->Cast<BoundReferenceExpression>().Index();
	return true;
}

void HiveRegexSerDe::Extract(ClientContext &context, Vector &lines, idx_t count, const vector<idx_t> &groups,
                             const vector<reference<Vector>> &results) const {
	ExtractGroups(context, *regex, lines, count, groups, results);
}

} // namespace duckdb
