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

const HiveSerDeType ALL_SERDES[] = {HiveSerDeType::PARQUET,    HiveSerDeType::LAZY_SIMPLE, HiveSerDeType::OPEN_CSV,
                                    HiveSerDeType::OPENX_JSON, HiveSerDeType::HIVE_JSON,   HiveSerDeType::AVRO};

//! A SerializationLibrary (lower case) that contains it is the SerDe, matched in the order of ALL_SERDES
const char *LibraryPattern(HiveSerDeType type) {
	switch (type) {
	case HiveSerDeType::PARQUET:
		return "parquet";
	case HiveSerDeType::LAZY_SIMPLE:
		return "lazysimpleserde";
	case HiveSerDeType::OPEN_CSV:
		return "opencsvserde";
	case HiveSerDeType::OPENX_JSON:
		return "openx";
	case HiveSerDeType::HIVE_JSON:
		return "json";
	case HiveSerDeType::AVRO:
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

bool HiveSerDe::TryFind(const string &library, HiveSerDeType &result) {
	auto lower = StringUtil::Lower(library);
	for (auto type : ALL_SERDES) {
		if (StringUtil::Contains(lower, LibraryPattern(type))) {
			result = type;
			return true;
		}
	}
	return false;
}

HiveSerDe HiveSerDe::ForFileFormat(HiveFileFormat file_format) {
	switch (file_format) {
	case HiveFileFormat::PARQUET:
		return HiveSerDe(HiveSerDeType::PARQUET);
	case HiveFileFormat::JSON:
		return HiveSerDe(HiveSerDeType::HIVE_JSON);
	case HiveFileFormat::AVRO:
		return HiveSerDe(HiveSerDeType::AVRO);
	case HiveFileFormat::CSV:
		break;
	}
	throw InternalException("A csv table's SerDe depends on its dialect");
}

bool HiveSerDe::IsCreatedParameter(const string &key) {
	for (auto type : ALL_SERDES) {
		for (auto &parameter : HiveSerDe(type).NewTableParameters()) {
			if (StringUtil::CIEquals(parameter.first, key)) {
				return true;
			}
		}
	}
	return false;
}

HiveFileFormat HiveSerDe::FileFormat() const {
	switch (type) {
	case HiveSerDeType::PARQUET:
		return HiveFileFormat::PARQUET;
	case HiveSerDeType::LAZY_SIMPLE:
	case HiveSerDeType::OPEN_CSV:
		return HiveFileFormat::CSV;
	case HiveSerDeType::HIVE_JSON:
	case HiveSerDeType::OPENX_JSON:
		return HiveFileFormat::JSON;
	case HiveSerDeType::AVRO:
		return HiveFileFormat::AVRO;
	}
	throw InternalException("Unknown HiveSerDeType");
}

string HiveSerDe::Name() const {
	switch (type) {
	case HiveSerDeType::PARQUET:
		return "ParquetHiveSerDe";
	case HiveSerDeType::LAZY_SIMPLE:
		return "LazySimpleSerDe";
	case HiveSerDeType::OPEN_CSV:
		return "OpenCSVSerde";
	case HiveSerDeType::HIVE_JSON:
	case HiveSerDeType::OPENX_JSON:
		return "JsonSerDe";
	case HiveSerDeType::AVRO:
		return "AvroSerDe";
	}
	throw InternalException("Unknown HiveSerDeType");
}

string HiveSerDe::Library() const {
	switch (type) {
	case HiveSerDeType::PARQUET:
		return "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
	case HiveSerDeType::LAZY_SIMPLE:
		return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
	case HiveSerDeType::OPEN_CSV:
		return "org.apache.hadoop.hive.serde2.OpenCSVSerde";
	case HiveSerDeType::HIVE_JSON:
		return "org.apache.hive.hcatalog.data.JsonSerDe";
	case HiveSerDeType::OPENX_JSON:
		return "org.openx.data.jsonserde.JsonSerDe";
	case HiveSerDeType::AVRO:
		return "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
	}
	throw InternalException("Unknown HiveSerDeType");
}

string HiveSerDe::InputFormat() const {
	switch (FileFormat()) {
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

string HiveSerDe::OutputFormat() const {
	switch (FileFormat()) {
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

HiveSerDeParameters HiveSerDe::NewTableParameters() const {
	switch (type) {
	case HiveSerDeType::PARQUET:
		return {{"serialization.format", "1"}};
	case HiveSerDeType::LAZY_SIMPLE:
	case HiveSerDeType::OPEN_CSV:
		return FormatCSVDialect(NewTableCSVDialect());
	case HiveSerDeType::HIVE_JSON:
	case HiveSerDeType::OPENX_JSON:
	case HiveSerDeType::AVRO:
		return HiveSerDeParameters();
	}
	throw InternalException("Unknown HiveSerDeType");
}

void HiveSerDe::Check(const GlueTableInfo &table) const {
	if (!IsTextFileFormat(FileFormat())) {
		return;
	}
	if (CountProperty(table, "skip.footer.line.count") > 0) {
		Refuse(table, "has 'skip.footer.line.count' set");
	}
	if (FileFormat() == HiveFileFormat::CSV) {
		for (auto &column : table.columns) {
			if (column.type.find('<') != string::npos) {
				Refuse(table, StringUtil::Format("has the nested column '%s' (%s), stored with collection delimiters",
				                                 column.name, column.type));
			}
		}
	}
	string value;
	switch (type) {
	case HiveSerDeType::LAZY_SIMPLE: {
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
		return;
	}
	case HiveSerDeType::OPEN_CSV:
		if (OpenCSVQuote(table).size() > 1 || OpenCSVEscape(table).size() > 1) {
			Refuse(table, "has a quoteChar or escapeChar of more than one byte");
		}
		return;
	case HiveSerDeType::HIVE_JSON:
	case HiveSerDeType::OPENX_JSON:
		if (CountProperty(table, "skip.header.line.count") > 0) {
			Refuse(table, "has 'skip.header.line.count' set on JSON files");
		}
		return;
	case HiveSerDeType::PARQUET:
	case HiveSerDeType::AVRO:
		return;
	}
}

HiveCSVDialect HiveSerDe::DefaultCSVDialect() const {
	HiveCSVDialect dialect;
	dialect.serde_fields = true;
	switch (type) {
	case HiveSerDeType::LAZY_SIMPLE:
		dialect.delimiter = "\x01";
		dialect.quote = "";
		dialect.escape = "";
		dialect.null_string = "\\N";
		return dialect;
	case HiveSerDeType::OPEN_CSV:
		dialect.has_null_string = false;
		dialect.null_for_invalid_data = false;
		dialect.false_unless_true = true;
		dialect.epoch_fields = true;
		// without escapeChar OpenCSVSerde escapes with '\', which read_csv would keep
		dialect.field_escape = "\\";
		return dialect;
	case HiveSerDeType::PARQUET:
	case HiveSerDeType::HIVE_JSON:
	case HiveSerDeType::OPENX_JSON:
	case HiveSerDeType::AVRO:
		break;
	}
	throw InternalException("%s is not a csv SerDe", Name());
}

HiveCSVDialect HiveSerDe::NewTableCSVDialect() const {
	auto dialect = DefaultCSVDialect();
	switch (type) {
	case HiveSerDeType::LAZY_SIMPLE:
		dialect.delimiter = ",";
		return dialect;
	case HiveSerDeType::OPEN_CSV:
		// CREATE TABLE writes escapeChar
		dialect.field_escape = "";
		return dialect;
	case HiveSerDeType::PARQUET:
	case HiveSerDeType::HIVE_JSON:
	case HiveSerDeType::OPENX_JSON:
	case HiveSerDeType::AVRO:
		break;
	}
	throw InternalException("%s is not a csv SerDe", Name());
}

HiveCSVDialect HiveSerDe::ReadCSVDialect(const GlueTableInfo &table) const {
	auto dialect = DefaultCSVDialect();
	switch (type) {
	case HiveSerDeType::LAZY_SIMPLE:
		dialect.delimiter = LazySimpleFieldDelimiter(table);
		dialect.null_string = LazySimpleNullString(table);
		return dialect;
	case HiveSerDeType::OPEN_CSV: {
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
	case HiveSerDeType::PARQUET:
	case HiveSerDeType::HIVE_JSON:
	case HiveSerDeType::OPENX_JSON:
	case HiveSerDeType::AVRO:
		break;
	}
	throw InternalException("%s is not a csv SerDe", Name());
}

HiveCSVDialect HiveSerDe::ParseCSVDialect(const GlueTableInfo &table) const {
	Check(table);
	auto dialect = ReadCSVDialect(table);
	dialect.skip_lines = CountProperty(table, "skip.header.line.count");
	return dialect;
}

HiveSerDeParameters HiveSerDe::FormatCSVDialect(const HiveCSVDialect &dialect) const {
	switch (type) {
	case HiveSerDeType::LAZY_SIMPLE:
		return {{"field.delim", dialect.delimiter}, {"serialization.format", dialect.delimiter}};
	case HiveSerDeType::OPEN_CSV:
		return {{"separatorChar", dialect.delimiter}, {"quoteChar", dialect.quote}, {"escapeChar", dialect.escape}};
	case HiveSerDeType::PARQUET:
	case HiveSerDeType::HIVE_JSON:
	case HiveSerDeType::OPENX_JSON:
	case HiveSerDeType::AVRO:
		break;
	}
	throw InternalException("%s is not a csv SerDe", Name());
}

HiveJSONDialect HiveSerDe::ParseJSONDialect(const GlueTableInfo &table) const {
	Check(table);
	HiveJSONDialect dialect;
	switch (type) {
	case HiveSerDeType::HIVE_JSON:
		NoteUnsupported(table, "timestamp.formats", false, dialect);
		break;
	case HiveSerDeType::OPENX_JSON: {
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
	case HiveSerDeType::PARQUET:
	case HiveSerDeType::LAZY_SIMPLE:
	case HiveSerDeType::OPEN_CSV:
	case HiveSerDeType::AVRO:
		throw InternalException("%s is not a json SerDe", Name());
	}
	std::sort(dialect.unsupported_properties.begin(), dialect.unsupported_properties.end());
	return dialect;
}

} // namespace duckdb
