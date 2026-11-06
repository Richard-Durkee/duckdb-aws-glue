#include "core/serde/hive_serde.hpp"

#include "core/helpers.hpp"

#include <algorithm>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

class ParquetHiveSerDe : public HiveSerDe {
public:
	ParquetHiveSerDe()
	    : HiveSerDe(HiveSerDeType::PARQUET, HiveFileFormat::PARQUET, "ParquetHiveSerDe",
	                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "parquet") {
	}

public:
	HiveSerDeParameters NewTableParameters() const override {
		return {{"serialization.format", "1"}};
	}
};

class AvroSerDe : public HiveSerDe {
public:
	AvroSerDe()
	    : HiveSerDe(HiveSerDeType::AVRO, HiveFileFormat::AVRO, "AvroSerDe",
	                "org.apache.hadoop.hive.serde2.avro.AvroSerDe", "avro") {
	}
};

//! In the order SerializationLibrary names are matched
const vector<reference<const HiveSerDe>> &SerDes() {
	static const ParquetHiveSerDe parquet;
	static const LazySimpleSerDe lazy_simple;
	static const OpenCSVSerDe open_csv;
	static const OpenXJSONSerDe openx_json;
	static const HCatalogJSONSerDe hcatalog_json;
	static const AvroSerDe avro;
	static const vector<reference<const HiveSerDe>> serdes {parquet,    lazy_simple,   open_csv,
	                                                        openx_json, hcatalog_json, avro};
	return serdes;
}

} // namespace

HiveSerDe::HiveSerDe(HiveSerDeType type, HiveFileFormat file_format, string name, string library,
                     string library_pattern)
    : type(type), file_format(file_format), name(std::move(name)), library(std::move(library)),
      library_pattern(std::move(library_pattern)) {
}

optional_ptr<const HiveSerDe> HiveSerDe::Find(const string &library) {
	auto lower = StringUtil::Lower(library);
	for (auto &serde : SerDes()) {
		if (StringUtil::Contains(lower, serde.get().library_pattern)) {
			return &serde.get();
		}
	}
	return nullptr;
}

const HiveSerDe &HiveSerDe::Get(HiveSerDeType type) {
	for (auto &serde : SerDes()) {
		if (serde.get().type == type) {
			return serde.get();
		}
	}
	throw InternalException("Unknown HiveSerDeType");
}

const HiveSerDe &HiveSerDe::ForFileFormat(HiveFileFormat file_format) {
	switch (file_format) {
	case HiveFileFormat::PARQUET:
		return Get(HiveSerDeType::PARQUET);
	case HiveFileFormat::JSON:
		return Get(HiveSerDeType::HIVE_JSON);
	case HiveFileFormat::AVRO:
		return Get(HiveSerDeType::AVRO);
	case HiveFileFormat::CSV:
		break;
	}
	throw InternalException("A csv table's SerDe depends on its dialect");
}

bool HiveSerDe::IsCreatedParameter(const string &key) {
	for (auto &serde : SerDes()) {
		for (auto &parameter : serde.get().NewTableParameters()) {
			if (StringUtil::CIEquals(parameter.first, key)) {
				return true;
			}
		}
	}
	return false;
}

string HiveSerDe::InputFormat() const {
	switch (file_format) {
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
	switch (file_format) {
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
	return HiveSerDeParameters();
}

void HiveSerDe::Check(const GlueTableInfo &table) const {
}

void HiveSerDe::Refuse(const GlueTableInfo &table, const string &what) {
	throw NotImplementedException("Hive table '%s.%s' %s, which DuckDB can not read or write", table.schema_name,
	                              table.name, what);
}

idx_t HiveSerDe::CountProperty(const GlueTableInfo &table, const string &key) {
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

bool HiveSerDe::IsTrueProperty(const GlueTableInfo &table, const string &key) {
	string value;
	return table.TryGetProperty(key, value) && StringUtil::CIEquals(value, "true");
}

void HiveCSVSerDe::Check(const GlueTableInfo &table) const {
	if (CountProperty(table, "skip.footer.line.count") > 0) {
		Refuse(table, "has 'skip.footer.line.count' set");
	}
	for (auto &column : table.columns) {
		if (column.type.find('<') != string::npos) {
			Refuse(table, StringUtil::Format("has the nested column '%s' (%s), stored with collection delimiters",
			                                 column.name, column.type));
		}
	}
	CheckProperties(table);
}

HiveCSVDialect HiveCSVSerDe::ReadDialect(const GlueTableInfo &table) const {
	auto dialect = DefaultDialect();
	ReadProperties(table, dialect);
	return dialect;
}

HiveCSVDialect HiveCSVSerDe::Parse(const GlueTableInfo &table) const {
	Check(table);
	auto dialect = ReadDialect(table);
	dialect.skip_lines = CountProperty(table, "skip.header.line.count");
	return dialect;
}

HiveSerDeParameters HiveCSVSerDe::NewTableParameters() const {
	return Format(NewTableDialect());
}

void HiveJSONSerDe::Check(const GlueTableInfo &table) const {
	if (CountProperty(table, "skip.footer.line.count") > 0) {
		Refuse(table, "has 'skip.footer.line.count' set");
	}
	if (CountProperty(table, "skip.header.line.count") > 0) {
		Refuse(table, "has 'skip.header.line.count' set on JSON files");
	}
}

HiveJSONDialect HiveJSONSerDe::Parse(const GlueTableInfo &table) const {
	Check(table);
	auto dialect = ReadDialect(table);
	std::sort(dialect.unsupported_properties.begin(), dialect.unsupported_properties.end());
	return dialect;
}

void HiveJSONSerDe::NoteUnsupported(const GlueTableInfo &table, const string &key, bool only_if_true,
                                    HiveJSONDialect &dialect) {
	string value;
	if (!table.TryGetProperty(key, value)) {
		return;
	}
	if (only_if_true ? StringUtil::CIEquals(value, "true") : !value.empty()) {
		dialect.unsupported_properties.push_back(key);
	}
}

} // namespace duckdb
