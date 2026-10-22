#include "functions/glue_describe.hpp"

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <algorithm>
#include <functional>

namespace duckdb {

namespace {

string GetString(const JSONValue &object, const string &key) {
	auto value = object.GetMember(key);
	return value.IsString() ? value.GetString() : string();
}

string GetNumber(const JSONValue &object, const string &key) {
	auto value = object.GetMember(key);
	return value.IsNumber() ? std::to_string(static_cast<int64_t>(value.GetNumber())) : string();
}

string GetBoolean(const JSONValue &object, const string &key) {
	auto value = object.GetMember(key);
	if (!value.IsValid() || value.GetType() != JSONValueType::BOOLEAN) {
		return string();
	}
	return value.GetBoolean() ? "true" : "false";
}

//! A Glue time (seconds since the epoch) as a UTC timestamp; empty when unset
string GetTime(const JSONValue &object, const string &key) {
	auto value = object.GetMember(key);
	if (!value.IsNumber() || value.GetNumber() <= 0) {
		return string();
	}
	auto micros = static_cast<int64_t>(value.GetNumber() * Interval::MICROS_PER_SEC);
	return Timestamp::ToString(Timestamp::FromEpochMicroSeconds(micros));
}

//! Glue leaves out empty lists and maps, so a missing member iterates as empty
void ForEachEntry(const JSONValue &array, const std::function<void(JSONValue)> &callback) {
	if (array.IsArray()) {
		array.IterateArray(callback);
	}
}

string GetList(const JSONValue &object, const string &key) {
	vector<string> entries;
	ForEachEntry(object.GetMember(key),
	             [&](JSONValue entry) { entries.push_back(entry.IsString() ? entry.GetString() : entry.ToString()); });
	return "[" + StringUtil::Join(entries, ", ") + "]";
}

void AddRow(vector<GlueDescribeRow> &rows, string category, string name, string value, string comment = string()) {
	rows.push_back(GlueDescribeRow {std::move(category), std::move(name), std::move(value), std::move(comment)});
}

void AddColumns(vector<GlueDescribeRow> &rows, const string &category, const JSONValue &columns) {
	ForEachEntry(columns, [&](JSONValue column) {
		AddRow(rows, category, GetString(column, "Name"), GetString(column, "Type"), GetString(column, "Comment"));
	});
}

//! The entries of a string map, sorted by key
void AddMap(vector<GlueDescribeRow> &rows, const string &category, const JSONValue &map) {
	if (!map.IsObject()) {
		return;
	}
	vector<pair<string, string>> entries;
	map.IterateObject([&](const string &key, JSONValue value) {
		entries.emplace_back(key, value.IsString() ? value.GetString() : value.ToString());
	});
	std::sort(entries.begin(), entries.end());
	for (auto &entry : entries) {
		AddRow(rows, category, entry.first, entry.second);
	}
}

void AddStorage(vector<GlueDescribeRow> &rows, const JSONValue &sd) {
	auto serde = sd.GetMember("SerdeInfo");
	AddRow(rows, "storage", "serde", GetString(serde, "SerializationLibrary"));
	AddRow(rows, "storage", "input format", GetString(sd, "InputFormat"));
	AddRow(rows, "storage", "output format", GetString(sd, "OutputFormat"));
	AddRow(rows, "storage", "compressed", GetBoolean(sd, "Compressed"));
	AddRow(rows, "storage", "buckets", GetNumber(sd, "NumberOfBuckets"));
	AddRow(rows, "storage", "bucket columns", GetList(sd, "BucketColumns"));
	vector<string> sort_columns;
	ForEachEntry(sd.GetMember("SortColumns"), [&](JSONValue entry) {
		sort_columns.push_back(GetString(entry, "Column") + (GetNumber(entry, "SortOrder") == "0" ? " DESC" : " ASC"));
	});
	AddRow(rows, "storage", "sort columns", "[" + StringUtil::Join(sort_columns, ", ") + "]");
	AddMap(rows, "serde parameter", serde.GetMember("Parameters"));
}

} // namespace

vector<GlueDescribeRow> GlueDescribe(GlueDescribeFormat format, const string &table_json,
                                     const string *partition_json) {
	auto table_document = JSONDocument::Parse(table_json.c_str(), table_json.size());
	auto table = table_document->GetRoot();
	auto sd = table.GetMember("StorageDescriptor");

	vector<GlueDescribeRow> rows;
	AddColumns(rows, "column", sd.GetMember("Columns"));
	AddColumns(rows, "partition key", table.GetMember("PartitionKeys"));
	if (format == GlueDescribeFormat::DESCRIBE) {
		return rows;
	}

	if (partition_json) {
		auto partition_document = JSONDocument::Parse(partition_json->c_str(), partition_json->size());
		auto partition = partition_document->GetRoot();
		auto partition_sd = partition.GetMember("StorageDescriptor");
		AddRow(rows, "partition", "values", GetList(partition, "Values"));
		AddRow(rows, "partition", "location", GetString(partition_sd, "Location"));
		AddRow(rows, "partition", "created", GetTime(partition, "CreationTime"));
		AddRow(rows, "partition", "last accessed", GetTime(partition, "LastAccessTime"));
		AddMap(rows, "partition parameter", partition.GetMember("Parameters"));
		AddStorage(rows, partition_sd);
		if (format == GlueDescribeFormat::EXTENDED) {
			AddRow(rows, "partition", "glue definition", partition_document->ToString());
		}
		return rows;
	}

	AddRow(rows, "table", "database", GetString(table, "DatabaseName"));
	AddRow(rows, "table", "owner", GetString(table, "Owner"));
	AddRow(rows, "table", "created", GetTime(table, "CreateTime"));
	AddRow(rows, "table", "last accessed", GetTime(table, "LastAccessTime"));
	AddRow(rows, "table", "location", GetString(sd, "Location"));
	AddRow(rows, "table", "type", GetString(table, "TableType"));
	AddMap(rows, "table parameter", table.GetMember("Parameters"));
	AddStorage(rows, sd);
	if (format == GlueDescribeFormat::EXTENDED) {
		AddRow(rows, "table", "glue definition", table_document->ToString());
	}
	return rows;
}

} // namespace duckdb
