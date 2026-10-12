#include "functions/glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//! One row of the output: col_name, data_type, comment
using DescribeRow = array<string, 3>;

struct GlueDescribeTableBindData : public TableFunctionData {
	vector<DescribeRow> rows;
};

struct GlueDescribeTableState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

//! How much DESCRIBE shows, as in Hive: just the columns, the columns plus a
//! readable summary of the table (FORMATTED), or the columns plus the whole
//! table definition (EXTENDED)
enum class DescribeMode : uint8_t { PLAIN, FORMATTED, EXTENDED };

//! A time the way Hive prints it, e.g. "Thu Apr 23 02:55:21 UTC 2020"
string HiveTime(int64_t seconds) {
	if (seconds <= 0) {
		return "UNKNOWN";
	}
	static const char *DAYS[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
	static const char *MONTHS[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
	date_t date;
	dtime_t time;
	Timestamp::Convert(Timestamp::FromEpochSeconds(seconds), date, time);
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	int32_t hour, minute, second, micros;
	Time::Convert(time, hour, minute, second, micros);
	return StringUtil::Format("%s %s %02d %02d:%02d:%02d UTC %d", DAYS[Date::ExtractISODayOfTheWeek(date) - 1],
	                          MONTHS[month - 1], day, hour, minute, second, year);
}

//! A "<title>:" row, then one row per key of 'parameters', sorted by key
void AddParameterRows(vector<DescribeRow> &rows, const string &title, const unordered_map<string, string> &parameters) {
	rows.push_back({title, "", ""});
	vector<string> keys;
	for (auto &entry : parameters) {
		keys.push_back(entry.first);
	}
	std::sort(keys.begin(), keys.end());
	for (auto &key : keys) {
		rows.push_back({"", key, parameters.at(key)});
	}
}

void AddColumnRows(vector<DescribeRow> &rows, const vector<GlueColumn> &columns) {
	for (auto &column : columns) {
		rows.push_back({column.name, column.type, column.comment});
	}
}

//! The rows plain DESCRIBE shows: the data columns and partition keys, then the
//! partition keys again under their own heading
void AddColumnSection(vector<DescribeRow> &rows, const GlueTableInfo &table) {
	AddColumnRows(rows, table.columns);
	AddColumnRows(rows, table.partition_keys);
	if (!table.partition_keys.empty()) {
		rows.push_back({"", "", ""});
		rows.push_back({"# Partition Information", "", ""});
		rows.push_back({"# col_name", "data_type", "comment"});
		rows.push_back({"", "", ""});
		AddColumnRows(rows, table.partition_keys);
	}
}

//! FORMATTED's "# Storage Information" for the storage of a table or of one
//! partition
void AddStorageSection(vector<DescribeRow> &rows, const GlueTableInfo &storage) {
	vector<string> sort_columns;
	for (auto &column : storage.sort_columns) {
		// Hive writes 1 for ascending and 0 for descending
		sort_columns.push_back(StringUtil::Format("Order(col:%s, order:%d)", column.name,
		                                          column.sort_order == GlueSortOrder::DESCENDING ? 0 : 1));
	}
	rows.push_back({"", "", ""});
	rows.push_back({"# Storage Information", "", ""});
	rows.push_back({"SerDe Library:", storage.serde_library, ""});
	rows.push_back({"InputFormat:", storage.input_format, ""});
	rows.push_back({"OutputFormat:", storage.output_format, ""});
	rows.push_back({"Compressed:", storage.compressed ? "Yes" : "No", ""});
	rows.push_back({"Num Buckets:", to_string(storage.number_of_buckets), ""});
	rows.push_back({"Bucket Columns:", "[" + StringUtil::Join(storage.bucket_columns, ", ") + "]", ""});
	rows.push_back({"Sort Columns:", "[" + StringUtil::Join(sort_columns, ", ") + "]", ""});
	AddParameterRows(rows, "Storage Desc Params:", storage.serde_parameters);
}

//! FORMATTED for a whole table
void AddTableDetails(vector<DescribeRow> &rows, const GlueTableInfo &table) {
	rows.push_back({"", "", ""});
	rows.push_back({"# Detailed Table Information", "", ""});
	rows.push_back({"Database:", table.database_name, ""});
	rows.push_back({"Owner:", table.owner, ""});
	rows.push_back({"CreateTime:", HiveTime(table.create_time), ""});
	rows.push_back({"LastAccessTime:", HiveTime(table.last_access_time), ""});
	rows.push_back({"Protect Mode:", "None", ""});
	rows.push_back({"Retention:", to_string(table.retention), ""});
	if (!table.IsView()) {
		rows.push_back({"Location:", table.location, ""});
	}
	rows.push_back({"Table Type:", table.glue_table_type, ""});
	AddParameterRows(rows, "Table Parameters:", table.parameters);
	if (table.IsView()) {
		rows.push_back({"", "", ""});
		rows.push_back({"# View Information", "", ""});
		rows.push_back({"View Original Text:", table.view_original_text, ""});
		rows.push_back({"View Expanded Text:", table.view_expanded_text, ""});
		return;
	}
	AddStorageSection(rows, table);
}

//! FORMATTED for one partition: its values, times, location and parameters,
//! then its own storage
void AddPartitionDetails(vector<DescribeRow> &rows, const GlueTableInfo &table, const GluePartitionInfo &partition,
                         const GlueTableInfo &details) {
	rows.push_back({"", "", ""});
	rows.push_back({"# Detailed Partition Information", "", ""});
	rows.push_back({"Partition Value:", "[" + StringUtil::Join(partition.values, ", ") + "]", ""});
	rows.push_back({"Database:", table.database_name, ""});
	rows.push_back({"Table:", table.name, ""});
	rows.push_back({"CreateTime:", HiveTime(details.create_time), ""});
	rows.push_back({"LastAccessTime:", HiveTime(details.last_access_time), ""});
	rows.push_back({"Location:", partition.location, ""});
	AddParameterRows(rows, "Partition Parameters:", details.parameters);
	AddStorageSection(rows, details);
}

//! EXTENDED: Hive prints the definition as one line of Thrift; we give Glue's
//! own JSON for it instead
void AddDefinitionRow(vector<DescribeRow> &rows, const string &title, const string &json) {
	rows.push_back({"", "", ""});
	rows.push_back({title, json, ""});
}

unique_ptr<FunctionData> GlueDescribeTableBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto mode = DescribeMode::PLAIN;
	optional_ptr<const Value> partition_spec;
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "mode" && !option.second.IsNull()) {
			auto value = StringUtil::Lower(option.second.GetValue<string>());
			if (value == "formatted") {
				mode = DescribeMode::FORMATTED;
			} else if (value == "extended") {
				mode = DescribeMode::EXTENDED;
			} else {
				throw BinderException("glue_describe_table: 'mode' is 'formatted' or 'extended', not '%s'",
				                      option.second.GetValue<string>());
			}
		} else if (name == "partition" && !option.second.IsNull()) {
			partition_spec = &option.second;
		}
	}

	auto result = make_uniq<GlueDescribeTableBindData>();
	auto &rows = result->rows;
	if (partition_spec) {
		// partitions only exist for Hive tables, which ResolveGlueTable checks
		auto target = ResolveGlueTable(context, "glue_describe_table", input.inputs[0]);
		auto values = ParsePartitionSpec("glue_describe_table", target, *partition_spec);
		GluePartitionInfo partition;
		GlueTableInfo details;
		string json;
		if (!GlueAPI::GetPartition(context, *target.catalog, target.table.database_name, target.table.name, values,
		                           partition, &details, &json)) {
			throw CatalogException("Partition [%s] of table '%s' does not exist", StringUtil::Join(values, ", "),
			                       target.TableName());
		}
		AddColumnSection(rows, target.table);
		if (mode == DescribeMode::FORMATTED) {
			AddPartitionDetails(rows, target.table, partition, details);
		} else if (mode == DescribeMode::EXTENDED) {
			AddDefinitionRow(rows, "Detailed Partition Information", json);
		}
	} else {
		auto name = ResolveGlueTableName(context, "glue_describe_table", input.inputs[0].GetValue<string>());
		auto &catalog = Catalog::GetCatalog(context, name.Catalog()).Cast<GlueCatalog>();
		GlueTableInfo table;
		string json;
		if (!GlueAPI::GetTable(context, catalog, name.Schema().GetIdentifierName(), name.Name().GetIdentifierName(),
		                       table, &json)) {
			throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'",
			                       name.Schema().GetIdentifierName(), name.Name().GetIdentifierName(),
			                       name.Catalog().GetIdentifierName());
		}
		AddColumnSection(rows, table);
		if (mode == DescribeMode::FORMATTED) {
			AddTableDetails(rows, table);
		} else if (mode == DescribeMode::EXTENDED) {
			AddDefinitionRow(rows, "Detailed Table Information", json);
		}
	}
	names = {"col_name", "data_type", "comment"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueDescribeTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueDescribeTableState>();
}

void GlueDescribeTableScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueDescribeTableState>();
	auto &rows = data.bind_data->Cast<GlueDescribeTableBindData>().rows;
	idx_t count = 0;
	for (; state.offset < rows.size() && count < STANDARD_VECTOR_SIZE; state.offset++, count++) {
		for (idx_t column = 0; column < 3; column++) {
			output.SetValue(column, count, Value(rows[state.offset][column]));
		}
	}
	output.SetCardinality(count);
}

} // namespace

TableFunction GetGlueDescribeTableFunction() {
	TableFunction function("glue_describe_table", {LogicalType::VARCHAR}, GlueDescribeTableScan, GlueDescribeTableBind,
	                       GlueDescribeTableInit);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("mode", LogicalType::VARCHAR);
		options.Add("partition", LogicalType::ANY);
	});
	return function;
}

} // namespace duckdb
