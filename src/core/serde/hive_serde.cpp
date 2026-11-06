#include "core/serde/hive_serde.hpp"

#include "core/helpers.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>

namespace duckdb {

namespace {

const HiveSerDe ALL_SERDES[] = {HiveSerDe::PARQUET,    HiveSerDe::LAZY_SIMPLE, HiveSerDe::OPEN_CSV,
                                HiveSerDe::OPENX_JSON, HiveSerDe::HIVE_JSON,   HiveSerDe::AVRO};

//! A SerializationLibrary (lower case) that contains it is the SerDe, matched in the order of ALL_SERDES
const char *LibraryPattern(HiveSerDe serde) {
	switch (serde) {
	case HiveSerDe::PARQUET:
		return "parquet";
	case HiveSerDe::LAZY_SIMPLE:
		return "lazysimpleserde";
	case HiveSerDe::OPEN_CSV:
		return "opencsvserde";
	case HiveSerDe::OPENX_JSON:
		return "openx";
	case HiveSerDe::HIVE_JSON:
		return "json";
	case HiveSerDe::AVRO:
		return "avro";
	}
	throw InternalException("Unknown HiveSerDe");
}

[[noreturn]] void Refuse(const GlueTableInfo &table, const string &what) {
	throw NotImplementedException("Hive table '%s.%s' %s, which DuckDB can not read or write", table.schema_name,
	                              table.name, what);
}

//! A non-negative integer property, 0 when not set
idx_t CountProperty(const GlueTableInfo &table, const string &key) {
	string value;
	if (!table.TryGetProperty(key, value) || value.empty()) {
		return 0;
	}
	int64_t count;
	if (!TryParseInteger(value, 0, NumericLimits<int32_t>::Maximum(), count)) {
		throw InvalidInputException("Hive table '%s.%s' has an invalid '%s' of '%s', expected a non-negative number",
		                            table.schema_name, table.name, key, value);
	}
	return NumericCast<idx_t>(count);
}

bool IsTrueProperty(const GlueTableInfo &table, const string &key) {
	string value;
	return table.TryGetProperty(key, value) && StringUtil::CIEquals(value, "true");
}

//! Hive's LazyUtils.getByte: a byte code from -128 to 127 ('1' is '\001'), else the first character
string LazySimpleFieldDelimiter(const GlueTableInfo &table) {
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

string LazySimpleNullString(const GlueTableInfo &table) {
	string null_string;
	return table.TryGetProperty("serialization.null.format", null_string) ? null_string : "\\N";
}

//! An OpenCSVSerde character property (its first character), 'fallback' when not set
string OpenCSVCharacter(const GlueTableInfo &table, const string &key, const string &fallback) {
	string value;
	if (!table.TryGetProperty(key, value) || value.empty()) {
		return fallback;
	}
	string character;
	if (!TryFirstCharacter(value, character)) {
		throw InvalidInputException("Hive table '%s.%s' has an invalid '%s' of '%s', expected text", table.schema_name,
		                            table.name, key, value);
	}
	return character;
}

string OpenCSVQuote(const GlueTableInfo &table) {
	return OpenCSVCharacter(table, "quoteChar", "\"");
}

string OpenCSVEscape(const GlueTableInfo &table) {
	return OpenCSVCharacter(table, "escapeChar", OpenCSVQuote(table));
}

//! Records 'key' as not applied when it is set (to 'true' with only_if_true)
void NoteUnsupported(const GlueTableInfo &table, const string &key, bool only_if_true, HiveJSONDialect &dialect) {
	string value;
	if (!table.TryGetProperty(key, value)) {
		return;
	}
	if (only_if_true ? StringUtil::CIEquals(value, "true") : !value.empty()) {
		dialect.unsupported_properties.push_back(key);
	}
}

} // namespace

bool TryGetHiveSerDe(const string &library, HiveSerDe &result) {
	auto lower = StringUtil::Lower(library);
	for (auto serde : ALL_SERDES) {
		if (StringUtil::Contains(lower, LibraryPattern(serde))) {
			result = serde;
			return true;
		}
	}
	return false;
}

HiveFileFormat HiveSerDeFileFormat(HiveSerDe serde) {
	switch (serde) {
	case HiveSerDe::PARQUET:
		return HiveFileFormat::PARQUET;
	case HiveSerDe::LAZY_SIMPLE:
	case HiveSerDe::OPEN_CSV:
		return HiveFileFormat::CSV;
	case HiveSerDe::HIVE_JSON:
	case HiveSerDe::OPENX_JSON:
		return HiveFileFormat::JSON;
	case HiveSerDe::AVRO:
		return HiveFileFormat::AVRO;
	}
	throw InternalException("Unknown HiveSerDe");
}

string HiveSerDeName(HiveSerDe serde) {
	switch (serde) {
	case HiveSerDe::PARQUET:
		return "ParquetHiveSerDe";
	case HiveSerDe::LAZY_SIMPLE:
		return "LazySimpleSerDe";
	case HiveSerDe::OPEN_CSV:
		return "OpenCSVSerde";
	case HiveSerDe::HIVE_JSON:
	case HiveSerDe::OPENX_JSON:
		return "JsonSerDe";
	case HiveSerDe::AVRO:
		return "AvroSerDe";
	}
	throw InternalException("Unknown HiveSerDe");
}

string HiveSerDeLibrary(HiveSerDe serde) {
	switch (serde) {
	case HiveSerDe::PARQUET:
		return "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
	case HiveSerDe::LAZY_SIMPLE:
		return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
	case HiveSerDe::OPEN_CSV:
		return "org.apache.hadoop.hive.serde2.OpenCSVSerde";
	case HiveSerDe::HIVE_JSON:
		return "org.apache.hive.hcatalog.data.JsonSerDe";
	case HiveSerDe::OPENX_JSON:
		return "org.openx.data.jsonserde.JsonSerDe";
	case HiveSerDe::AVRO:
		return "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
	}
	throw InternalException("Unknown HiveSerDe");
}

string HiveSerDeInputFormat(HiveSerDe serde) {
	switch (HiveSerDeFileFormat(serde)) {
	case HiveFileFormat::PARQUET:
		return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
	case HiveFileFormat::CSV:
	case HiveFileFormat::JSON:
		return "org.apache.hadoop.mapred.TextInputFormat";
	case HiveFileFormat::AVRO:
		return "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
	}
	throw InternalException("Unknown HiveFileFormat");
}

string HiveSerDeOutputFormat(HiveSerDe serde) {
	switch (HiveSerDeFileFormat(serde)) {
	case HiveFileFormat::PARQUET:
		return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
	case HiveFileFormat::CSV:
	case HiveFileFormat::JSON:
		return "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
	case HiveFileFormat::AVRO:
		return "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
	}
	throw InternalException("Unknown HiveFileFormat");
}

HiveSerDe HiveSerDeForFileFormat(HiveFileFormat file_format) {
	switch (file_format) {
	case HiveFileFormat::PARQUET:
		return HiveSerDe::PARQUET;
	case HiveFileFormat::JSON:
		return HiveSerDe::HIVE_JSON;
	case HiveFileFormat::AVRO:
		return HiveSerDe::AVRO;
	case HiveFileFormat::CSV:
		break;
	}
	throw InternalException("A csv table's SerDe depends on its dialect");
}

HiveSerDeParameters HiveSerDeNewTableParameters(HiveSerDe serde) {
	switch (serde) {
	case HiveSerDe::PARQUET:
		return {{"serialization.format", "1"}};
	case HiveSerDe::LAZY_SIMPLE:
	case HiveSerDe::OPEN_CSV:
		return FormatCSVDialect(serde, NewTableCSVDialect(serde));
	case HiveSerDe::HIVE_JSON:
	case HiveSerDe::OPENX_JSON:
	case HiveSerDe::AVRO:
		return HiveSerDeParameters();
	}
	throw InternalException("Unknown HiveSerDe");
}

bool IsCreatedHiveSerDeParameter(const string &key) {
	for (auto serde : ALL_SERDES) {
		for (auto &parameter : HiveSerDeNewTableParameters(serde)) {
			if (StringUtil::CIEquals(parameter.first, key)) {
				return true;
			}
		}
	}
	return false;
}

void CheckHiveSerDe(HiveSerDe serde, const GlueTableInfo &table) {
	auto file_format = HiveSerDeFileFormat(serde);
	if (!IsTextFileFormat(file_format)) {
		return;
	}
	if (CountProperty(table, "skip.footer.line.count") > 0) {
		Refuse(table, "has 'skip.footer.line.count' set");
	}
	if (file_format == HiveFileFormat::JSON) {
		if (CountProperty(table, "skip.header.line.count") > 0) {
			Refuse(table, "has 'skip.header.line.count' set on JSON files");
		}
		return;
	}
	for (auto &column : table.columns) {
		if (column.type.find('<') != string::npos) {
			Refuse(table, StringUtil::Format("has the nested column '%s' (%s), stored with collection delimiters",
			                                 column.name, column.type));
		}
	}
	if (serde == HiveSerDe::OPEN_CSV) {
		if (OpenCSVQuote(table).size() > 1 || OpenCSVEscape(table).size() > 1) {
			Refuse(table, "has a quoteChar or escapeChar of more than one byte");
		}
		return;
	}
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
	auto null_string = LazySimpleNullString(table);
	auto delimiter = LazySimpleFieldDelimiter(table);
	if (StringUtil::Contains(null_string, delimiter)) {
		Refuse(table,
		       StringUtil::Format("has the field delimiter '%s' in its NULL string '%s'", delimiter, null_string));
	}
}

HiveCSVDialect DefaultCSVDialect(HiveSerDe serde) {
	HiveCSVDialect dialect;
	dialect.serde_fields = true;
	switch (serde) {
	case HiveSerDe::LAZY_SIMPLE:
		dialect.delimiter = "\x01";
		dialect.quote = "";
		dialect.escape = "";
		dialect.null_string = "\\N";
		return dialect;
	case HiveSerDe::OPEN_CSV:
		dialect.has_null_string = false;
		dialect.null_for_invalid_data = false;
		dialect.false_unless_true = true;
		dialect.epoch_fields = true;
		// without escapeChar OpenCSVSerde escapes with '\', which read_csv would keep
		dialect.field_escape = "\\";
		return dialect;
	default:
		throw InternalException("%s is not a csv SerDe", HiveSerDeName(serde));
	}
}

HiveCSVDialect NewTableCSVDialect(HiveSerDe serde) {
	auto dialect = DefaultCSVDialect(serde);
	if (serde == HiveSerDe::LAZY_SIMPLE) {
		dialect.delimiter = ",";
	} else {
		// CREATE TABLE writes escapeChar
		dialect.field_escape = "";
	}
	return dialect;
}

HiveCSVDialect ReadCSVDialect(HiveSerDe serde, const GlueTableInfo &table) {
	auto dialect = DefaultCSVDialect(serde);
	if (serde == HiveSerDe::LAZY_SIMPLE) {
		dialect.delimiter = LazySimpleFieldDelimiter(table);
		dialect.null_string = LazySimpleNullString(table);
		return dialect;
	}
	dialect.delimiter = OpenCSVCharacter(table, "separatorChar", dialect.delimiter);
	dialect.quote = OpenCSVQuote(table);
	dialect.escape = OpenCSVEscape(table);
	dialect.null_for_invalid_data = IsTrueProperty(table, "use.null.for.invalid.data");
	string escape_char;
	if ((table.TryGetProperty("escapeChar", escape_char) && !escape_char.empty()) || dialect.delimiter == "\\" ||
	    dialect.quote == "\\") {
		dialect.field_escape = "";
	}
	return dialect;
}

HiveCSVDialect ParseCSVDialect(HiveSerDe serde, const GlueTableInfo &table) {
	CheckHiveSerDe(serde, table);
	auto dialect = ReadCSVDialect(serde, table);
	dialect.skip_lines = CountProperty(table, "skip.header.line.count");
	return dialect;
}

HiveSerDeParameters FormatCSVDialect(HiveSerDe serde, const HiveCSVDialect &dialect) {
	switch (serde) {
	case HiveSerDe::LAZY_SIMPLE:
		return {{"field.delim", dialect.delimiter}, {"serialization.format", dialect.delimiter}};
	case HiveSerDe::OPEN_CSV:
		return {{"separatorChar", dialect.delimiter}, {"quoteChar", dialect.quote}, {"escapeChar", dialect.escape}};
	default:
		throw InternalException("%s is not a csv SerDe", HiveSerDeName(serde));
	}
}

HiveJSONDialect ParseJSONDialect(HiveSerDe serde, const GlueTableInfo &table) {
	CheckHiveSerDe(serde, table);
	HiveJSONDialect dialect;
	switch (serde) {
	case HiveSerDe::HIVE_JSON:
		NoteUnsupported(table, "timestamp.formats", false, dialect);
		break;
	case HiveSerDe::OPENX_JSON: {
		dialect.ignore_errors =
		    IsTrueProperty(table, "ignore.malformed.json") || IsTrueProperty(table, "use.null.for.invalid.data");
		string case_insensitive;
		if (table.TryGetProperty("case.insensitive", case_insensitive) &&
		    StringUtil::CIEquals(case_insensitive, "false")) {
			dialect.case_insensitive_keys = false;
		}
		set<string> mapping_keys;
		for (auto properties : {&table.parameters, &table.serde_parameters}) {
			for (auto &entry : *properties) {
				if (StringUtil::StartsWith(entry.first, "mapping.")) {
					mapping_keys.insert(entry.first);
				}
			}
		}
		for (auto &key : mapping_keys) {
			NoteUnsupported(table, key, false, dialect);
		}
		NoteUnsupported(table, "dots.in.keys", true, dialect);
		break;
	}
	default:
		throw InternalException("%s is not a json SerDe", HiveSerDeName(serde));
	}
	std::sort(dialect.unsupported_properties.begin(), dialect.unsupported_properties.end());
	return dialect;
}

} // namespace duckdb
