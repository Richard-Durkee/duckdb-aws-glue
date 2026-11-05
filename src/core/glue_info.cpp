#include "core/glue_info.hpp"
#include "core/helpers.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

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

bool IsTextFileFormat(HiveFileFormat format) {
	return format == HiveFileFormat::CSV || format == HiveFileFormat::JSON;
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

bool GlueTableInfo::IsBucketed() const {
	// NumberOfBuckets is -1 or 0 for unbucketed tables but can also be unset on bucketed ones
	return !bucket_columns.empty();
}

bool GlueTableInfo::IsPartitionProjected() const {
	if (partition_keys.empty()) {
		return false;
	}
	auto enabled = GetParameter("projection.enabled");
	StringUtil::Trim(enabled);
	return StringUtil::CIEquals(enabled, "true");
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

HiveSerDe GlueTableInfo::GetSerDe() const {
	const UnsupportedSerDe *unsupported;
	auto definition = MatchSerDe(serde_library, unsupported);
	if (definition) {
		return definition->serde;
	}
	auto reason = unsupported ? StringUtil::Format(" (%s)", unsupported->reason) : string();
	throw NotImplementedException(
	    "Hive table '%s.%s' uses SerDe '%s'%s, only parquet (ParquetHiveSerDe), csv "
	    "(LazySimpleSerDe, OpenCSVSerde), json (JsonSerDe) and avro (AvroSerDe) tables can be "
	    "read and written; use glue_get_table_response() to inspect the table definition",
	    schema_name, name, serde_library, reason);
}

HiveFileFormat GlueTableInfo::GetFileFormat() const {
	return GetSerDeDefinition(GetSerDe()).format;
}

bool GlueTableInfo::TryGetProperty(const string &key, string &result) const {
	// as Hive: the table's parameters override the SerDe's, and keys are case-sensitive
	for (auto properties : {&parameters, &serde_parameters}) {
		auto entry = properties->find(key);
		if (entry != properties->end()) {
			result = entry->second;
			return true;
		}
	}
	return false;
}

string GlueTableInfo::GetCodecParameter(const string &key) const {
	auto codec = GetParameter(key);
	return codec.empty() ? GetParameter("write_compression") : codec;
}

FileCompressionType GlueTableInfo::GetTextCompression() const {
	auto codec = GetCodecParameter("write.compression");
	if (codec.empty()) {
		codec = GetParameter("compressionType");
	}
	if (codec.empty()) {
		return FileCompressionType::AUTO_DETECT;
	}
	FileCompressionType compression(codec);
	if (compression.IsCompressed() && compression != FileCompressionType::GZIP &&
	    compression != FileCompressionType::ZSTD) {
		throw NotImplementedException("Can not write to Hive table '%s.%s': it records %s compression, DuckDB writes "
		                              "only gzip and zstd compressed csv and json files",
		                              schema_name, name, compression.ToString());
	}
	return compression;
}

string GlueTableInfo::GetCodec(HiveFileFormat format) const {
	switch (format) {
	case HiveFileFormat::PARQUET: {
		auto codec = StringUtil::Lower(GetCodecParameter("parquet.compression"));
		return codec == "none" ? "uncompressed" : codec;
	}
	case HiveFileFormat::AVRO: {
		auto codec = StringUtil::Lower(GetCodecParameter("avro.output.codec"));
		if (codec == "none" || codec == "uncompressed") {
			return "null";
		}
		if (!codec.empty() && codec != "snappy" && codec != "deflate" && codec != "null") {
			throw NotImplementedException("Can not write to Hive table '%s.%s': it records %s compression, DuckDB "
			                              "writes only snappy and deflate compressed avro files",
			                              schema_name, name, codec);
		}
		return codec;
	}
	case HiveFileFormat::CSV:
	case HiveFileFormat::JSON: {
		auto compression = GetTextCompression();
		return compression.IsCompressed() ? compression.ToString() : string();
	}
	}
	throw InternalException("Unknown Hive file format");
}

string GlueTableInfo::GetCompressionLevel() const {
	return GetParameter("compression_level");
}

SerDeProperties GlueTableInfo::ResolveSerDeProperties(SerDeDirection direction, bool check) const {
	return duckdb::ResolveSerDeProperties(*this, GetSerDe(), direction, check);
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

GlueTableFormat GlueTableInfo::GetFormat() const {
	auto &info = *this;
	return GlueTableFormat::Detect(info);
}

bool GlueTableInfo::IsSameVersion(const GlueTableInfo &other) const {
	// Glue's CreateTime has whole seconds: a table replaced by a view within one second keeps version and CreateTime
	return !version_id.empty() && version_id == other.version_id && create_time_ms == other.create_time_ms &&
	       glue_table_type == other.glue_table_type;
}

string GlueTableInfo::GetMetadataLocation() const {
	return GetParameter("metadata_location");
}

} // namespace duckdb
