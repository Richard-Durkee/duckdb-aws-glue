#include "core/serde/hive_serde.hpp"

#include "core/helpers.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

LazySimpleSerDe::LazySimpleSerDe()
    : HiveCSVSerDe(HiveSerDeType::LAZY_SIMPLE, HiveFileFormat::CSV, "LazySimpleSerDe",
                   "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "lazysimpleserde") {
}

string LazySimpleSerDe::FieldDelimiter(const GlueTableInfo &table) {
	string value;
	if (!table.TryGetProperty("field.delim", value)) {
		table.TryGetProperty("serialization.format", value);
	}
	if (value.empty()) {
		return "\x01";
	}
	int64_t number;
	auto separator = TryParseInteger(value, -128, 127, number) ? static_cast<uint8_t>(number & 0xFF)
	                                                           : static_cast<uint8_t>(value[0]);
	if (separator == 0 || separator == '\n' || separator == '\r' || separator >= 0x80) {
		throw NotImplementedException("Hive table '%s.%s' has the field delimiter '%s', a byte DuckDB can not split "
		                              "fields on; only ASCII delimiters other than NUL and line breaks are supported",
		                              table.schema_name, table.name, value);
	}
	return string(1, static_cast<char>(separator));
}

string LazySimpleSerDe::NullString(const GlueTableInfo &table) {
	string null_string;
	return table.TryGetProperty("serialization.null.format", null_string) ? null_string : "\\N";
}

void LazySimpleSerDe::CheckProperties(const GlueTableInfo &table) const {
	string value;
	if (table.TryGetProperty("escape.delim", value)) {
		Refuse(table, "has 'escape.delim' set");
	}
	if (table.TryGetProperty("serialization.encoding", value) && !StringUtil::CIEquals(value, "UTF-8") &&
	    !StringUtil::CIEquals(value, "UTF8")) {
		Refuse(table, StringUtil::Format("has the 'serialization.encoding' '%s' (DuckDB reads UTF-8)", value));
	}
	if (IsTrueProperty(table, "serialization.last.column.takes.rest")) {
		Refuse(table, "has 'serialization.last.column.takes.rest' set");
	}
	auto null_string = NullString(table);
	auto delimiter = FieldDelimiter(table);
	if (StringUtil::Contains(null_string, delimiter)) {
		Refuse(table,
		       StringUtil::Format("has the field delimiter '%s' in its NULL string '%s'", delimiter, null_string));
	}
}

HiveCSVDialect LazySimpleSerDe::DefaultDialect() const {
	HiveCSVDialect dialect;
	dialect.delimiter = "\x01";
	dialect.quote = "";
	dialect.escape = "";
	dialect.null_string = "\\N";
	dialect.serde_fields = true;
	return dialect;
}

HiveCSVDialect LazySimpleSerDe::NewTableDialect() const {
	auto dialect = DefaultDialect();
	dialect.delimiter = ",";
	return dialect;
}

void LazySimpleSerDe::ReadProperties(const GlueTableInfo &table, HiveCSVDialect &dialect) const {
	dialect.delimiter = FieldDelimiter(table);
	dialect.null_string = NullString(table);
}

HiveSerDeParameters LazySimpleSerDe::Format(const HiveCSVDialect &dialect) const {
	return {{"field.delim", dialect.delimiter}, {"serialization.format", dialect.delimiter}};
}

} // namespace duckdb
