#include "core/glue_info.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string GlueTableFormatToString(GlueTableFormat format) {
	switch (format) {
	case GlueTableFormat::ICEBERG:
		return "ICEBERG";
	case GlueTableFormat::DELTA:
		return "DELTA";
	case GlueTableFormat::HUDI:
		return "HUDI";
	case GlueTableFormat::HIVE:
		return "HIVE";
	default:
		return "UNKNOWN";
	}
}

string GlueTableInfo::GetParameter(const string &key) const {
	for (auto &entry : parameters) {
		if (StringUtil::CIEquals(entry.first, key)) {
			return entry.second;
		}
	}
	return string();
}

string HiveFileFormatToString(HiveFileFormat format) {
	switch (format) {
	case HiveFileFormat::PARQUET:
		return "parquet";
	case HiveFileFormat::CSV:
		return "csv";
	case HiveFileFormat::JSON:
		return "json";
	case HiveFileFormat::AVRO:
		return "avro";
	}
	throw InternalException("Unknown HiveFileFormat");
}

HiveFileFormat HiveFileFormatFromString(const string &format) {
	auto lower = StringUtil::Lower(format);
	if (lower == "parquet") {
		return HiveFileFormat::PARQUET;
	}
	if (lower == "csv") {
		return HiveFileFormat::CSV;
	}
	if (lower == "json") {
		return HiveFileFormat::JSON;
	}
	if (lower == "avro") {
		return HiveFileFormat::AVRO;
	}
	throw BinderException("Unknown Hive file format '%s', expected 'parquet', 'csv', 'json' or 'avro'", format);
}

string GlueTableInfo::GetSerdeParameter(const string &key) const {
	for (auto &entry : serde_parameters) {
		if (StringUtil::CIEquals(entry.first, key)) {
			return entry.second;
		}
	}
	return string();
}

bool GlueTableInfo::IsBucketed() const {
	// NumberOfBuckets is -1 or 0 for unbucketed tables but can also be unset on bucketed ones
	return !bucket_columns.empty();
}

string GlueColumn::DescribeSortOrder() const {
	switch (sort_order) {
	case GlueSortOrder::ASCENDING:
		return "ASC";
	case GlueSortOrder::DESCENDING:
		return "DESC";
	default:
		return string();
	}
}

string GlueTableInfo::DescribeBucketing() const {
	auto result = StringUtil::Format("clustered by (%s)", StringUtil::Join(bucket_columns, ", "));
	if (number_of_buckets > 0) {
		result += StringUtil::Format(" into %d buckets", number_of_buckets);
	}
	if (!sort_columns.empty()) {
		vector<string> sorted;
		for (auto &sort_column : sort_columns) {
			sorted.push_back(sort_column.name + " " + sort_column.DescribeSortOrder());
		}
		result += StringUtil::Format(", sorted by (%s)", StringUtil::Join(sorted, ", "));
	}
	return result;
}

HiveFileFormat GlueTableInfo::GetFileFormat() const {
	auto serde = StringUtil::Lower(serde_library);
	if (StringUtil::Contains(serde, "parquet")) {
		return HiveFileFormat::PARQUET;
	}
	if (StringUtil::Contains(serde, "lazysimpleserde") || StringUtil::Contains(serde, "opencsvserde")) {
		return HiveFileFormat::CSV;
	}
	if (StringUtil::Contains(serde, "json")) {
		return HiveFileFormat::JSON;
	}
	if (StringUtil::Contains(serde, "avro")) {
		return HiveFileFormat::AVRO;
	}
	throw NotImplementedException("Hive table '%s.%s' uses SerDe '%s', only parquet (ParquetHiveSerDe), csv "
	                              "(LazySimpleSerDe, OpenCSVSerde), json (JsonSerDe) and avro (AvroSerDe) tables are "
	                              "supported",
	                              database_name, name, serde_library);
}

bool GlueTableInfo::IsOpenCSVSerde() const {
	return StringUtil::Contains(StringUtil::Lower(serde_library), "opencsvserde");
}

bool GlueTableInfo::TryGetSerdeProperty(const string &key, string &result) const {
	// Hive hands the SerDe its parameters with the table's on top
	for (auto properties : {&parameters, &serde_parameters}) {
		for (auto &entry : *properties) {
			if (StringUtil::CIEquals(entry.first, key)) {
				result = entry.second;
				return true;
			}
		}
	}
	return false;
}

//! OpenCSVSerde reads only the first character of its separator, quote and escape characters
static string FirstCharacter(const string &value) {
	idx_t length = 1;
	auto lead = static_cast<uint8_t>(value[0]);
	if (lead >= 0xF0) {
		length = 4;
	} else if (lead >= 0xE0) {
		length = 3;
	} else if (lead >= 0xC0) {
		length = 2;
	}
	return value.substr(0, length);
}

//! LazySimpleSerDe's separator byte, as Hive's LazyUtils.getByte reads it: a number from -128 to 127 is a byte code
//! ('1' is '\001'), anything else its first character
static string LazySimpleSeparator(const GlueTableInfo &table, const string &value) {
	if (value.empty()) {
		return "\x01";
	}
	int64_t number;
	uint8_t separator;
	auto digits = value[0] == '-' || value[0] == '+' ? value.substr(1) : value;
	bool is_digits = !digits.empty();
	for (auto c : digits) {
		is_digits = is_digits && StringUtil::CharacterIsDigit(c);
	}
	if (is_digits && TryCast::Operation<string_t, int64_t>(string_t(value), number) && number >= -128 &&
	    number <= 127) {
		separator = static_cast<uint8_t>(number & 0xFF);
	} else {
		separator = static_cast<uint8_t>(value[0]);
	}
	if (separator == 0 || separator >= 0x80) {
		throw NotImplementedException("Hive table '%s.%s' has the field delimiter '%s', a byte DuckDB can not split "
		                              "fields on; only ASCII delimiters other than NUL are supported",
		                              table.database_name, table.name, value);
	}
	return string(1, static_cast<char>(separator));
}

string GlueTableInfo::GetFieldDelimiter() const {
	string delimiter;
	if (IsOpenCSVSerde()) {
		return TryGetSerdeProperty("separatorChar", delimiter) && !delimiter.empty() ? FirstCharacter(delimiter) : ",";
	}
	if (!TryGetSerdeProperty("field.delim", delimiter)) {
		TryGetSerdeProperty("serialization.format", delimiter);
	}
	return LazySimpleSeparator(*this, delimiter);
}

string GlueTableInfo::GetNullFormat() const {
	string null_format;
	if (IsOpenCSVSerde()) {
		return null_format;
	}
	return TryGetSerdeProperty("serialization.null.format", null_format) ? null_format : "\\N";
}

idx_t GlueTableInfo::GetLineCount(const string &key) const {
	string value;
	TryGetSerdeProperty(key, value);
	StringUtil::Trim(value);
	if (value.empty()) {
		return 0;
	}
	uint64_t lines;
	if (!TryCast::Operation<string_t, uint64_t>(string_t(value), lines)) {
		throw InvalidInputException("Hive table '%s.%s' has an invalid '%s' of '%s', expected a number of lines",
		                            database_name, name, key, value);
	}
	return lines;
}

idx_t GlueTableInfo::GetHeaderLineCount() const {
	return GetLineCount("skip.header.line.count");
}

void GlueTableInfo::CheckTextSerdeSupported() const {
	if (GetLineCount("skip.footer.line.count") > 0) {
		throw NotImplementedException(
		    "Hive table '%s.%s' has 'skip.footer.line.count' set, files with footer lines are "
		    "not supported",
		    database_name, name);
	}
	string escape;
	if (!IsOpenCSVSerde() && TryGetSerdeProperty("escape.delim", escape)) {
		throw NotImplementedException("Hive table '%s.%s' has 'escape.delim' set, escaped LazySimpleSerDe files are "
		                              "not supported",
		                              database_name, name);
	}
}

string GlueTableInfo::GetQuoteCharacter() const {
	// OpenCSVSerde: quoteChar. LazySimpleSerDe does not quote at all, but DuckDB writes (and reads) quoted fields
	// with the '"' it defaults to, which is also OpenCSVSerde's default
	string quote;
	return TryGetSerdeProperty("quoteChar", quote) && !quote.empty() ? FirstCharacter(quote) : "\"";
}

string GlueTableInfo::GetEscapeCharacter() const {
	string escape;
	return TryGetSerdeProperty("escapeChar", escape) && !escape.empty() ? FirstCharacter(escape) : GetQuoteCharacter();
}

GlueTableFormat GlueTableInfo::GetFormat() const {
	// Open table formats register themselves through the 'table_type' parameter
	auto table_type = StringUtil::Upper(GetParameter("table_type"));
	if (table_type == "ICEBERG") {
		return GlueTableFormat::ICEBERG;
	}
	if (table_type == "DELTA") {
		return GlueTableFormat::DELTA;
	}
	if (table_type == "HUDI") {
		return GlueTableFormat::HUDI;
	}
	// Spark registers Delta tables through the data source provider
	auto provider = StringUtil::Lower(GetParameter("spark.sql.sources.provider"));
	if (provider == "delta") {
		return GlueTableFormat::DELTA;
	}
	if (provider == "iceberg") {
		return GlueTableFormat::ICEBERG;
	}
	if (provider == "hudi") {
		return GlueTableFormat::HUDI;
	}
	if (!GetMetadataLocation().empty()) {
		return GlueTableFormat::ICEBERG;
	}
	if (!input_format.empty() || !location.empty()) {
		// Regular (Hive style) table with a storage descriptor
		return GlueTableFormat::HIVE;
	}
	return GlueTableFormat::UNKNOWN;
}

GlueTableType GlueTableTypeFromString(const string &type) {
	if (StringUtil::CIEquals(type, "EXTERNAL_TABLE")) {
		return GlueTableType::EXTERNAL_TABLE;
	}
	if (StringUtil::CIEquals(type, "VIRTUAL_VIEW")) {
		return GlueTableType::VIRTUAL_VIEW;
	}
	return GlueTableType::OTHER;
}

string GlueTableInfo::GetFormatName() const {
	auto format = GetFormat();
	if (format == GlueTableFormat::HIVE) {
		return StringUtil::Format("HIVE (input format '%s')", input_format);
	}
	if (format == GlueTableFormat::UNKNOWN && !glue_table_type.empty()) {
		return StringUtil::Format("UNKNOWN (glue table type '%s')", glue_table_type);
	}
	return GlueTableFormatToString(format);
}

string GlueTableInfo::GetMetadataLocation() const {
	return GetParameter("metadata_location");
}

} // namespace duckdb
