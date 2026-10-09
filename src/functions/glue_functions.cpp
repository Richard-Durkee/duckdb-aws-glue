#include "functions/glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"

#include <algorithm>

namespace duckdb {

namespace {

struct GlueGetTableResponseBindData : public TableFunctionData {
	GlueTableInfo table;
	string raw_json;
};

struct GlueGetTableResponseState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<FunctionData> GlueGetTableResponseBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto name = ResolveGlueTableName(context, "glue_get_table_response", input.inputs[0].GetValue<string>());
	auto &catalog = Catalog::GetCatalog(context, name.Catalog()).Cast<GlueCatalog>();
	auto result = make_uniq<GlueGetTableResponseBindData>();
	if (!GlueAPI::GetTable(context, catalog, name.Schema().GetIdentifierName(), name.Name().GetIdentifierName(),
	                       result->table, &result->raw_json)) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'", name.Schema().GetIdentifierName(),
		                       name.Name().GetIdentifierName(), name.Catalog().GetIdentifierName());
	}

	auto column_type = LogicalType::LIST(LogicalType::STRUCT(
	    {{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}, {"comment", LogicalType::VARCHAR}}));
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	names = {"database_name", "table_name",     "table_type", "glue_table_type",  "location", "serde_library",
	         "columns",       "partition_keys", "parameters", "serde_parameters", "response"};
	return_types = {LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                column_type,
	                column_type,
	                map_type,
	                map_type,
	                LogicalType::VARIANT()};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueGetTableResponseInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueGetTableResponseState>();
}

Value ColumnsToValue(const vector<GlueColumn> &columns, const LogicalType &list_type) {
	vector<Value> entries;
	for (auto &column : columns) {
		entries.push_back(
		    Value::STRUCT({{"name", Value(column.name)},
		                   {"type", Value(column.type)},
		                   {"comment", column.comment.empty() ? Value(LogicalType::VARCHAR) : Value(column.comment)}}));
	}
	return Value::LIST(ListType::GetChildType(list_type), std::move(entries));
}

Value MapToValue(const unordered_map<string, string> &map) {
	vector<Value> keys;
	vector<Value> values;
	for (auto &entry : map) {
		keys.emplace_back(entry.first);
		values.emplace_back(entry.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

void GlueGetTableResponseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueGetTableResponseState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueGetTableResponseBindData>();
	auto &table = bind_data.table;

	output.data[0].Append(Value(table.database_name));
	output.data[1].Append(Value(table.name));
	output.data[2].Append(Value(GlueTableFormatToString(table.GetFormat())));
	output.data[3].Append(Value(table.glue_table_type));
	output.data[4].Append(Value(table.location));
	output.data[5].Append(Value(table.serde_library));
	output.data[6].Append(ColumnsToValue(table.columns, output.data[6].GetType()));
	output.data[7].Append(ColumnsToValue(table.partition_keys, output.data[7].GetType()));
	output.data[8].Append(MapToValue(table.parameters));
	output.data[9].Append(MapToValue(table.serde_parameters));

	// The complete Glue Table object: JSON as serialized by the AWS SDK, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[10], 1);

	output.CheckCardinality(1);
}

struct GlueGetDatabaseResponseBindData : public TableFunctionData {
	GlueDatabaseInfo database;
	string raw_json;
};

unique_ptr<FunctionData> GlueGetDatabaseResponseBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto components = QualifiedName::ParseComponents(input.inputs[0].GetValue<string>());
	if (components.size() == 1) {
		// an unqualified database name: resolve it the way a query would (search path, default catalog)
		auto &schema = Catalog::GetSchema(context, Identifier(), components[0]);
		components = {schema.ParentCatalog().GetName(), Identifier(schema.name)};
	}
	if (components.size() != 2) {
		throw BinderException("glue_get_database_response expects a database name: '<catalog>.<database>' or "
		                      "'<database>', got '%s'",
		                      input.inputs[0].GetValue<string>());
	}
	auto &catalog_name = components[0];
	auto database_name = components[1].GetIdentifierName();
	auto catalog = Catalog::GetCatalogEntry(context, catalog_name);
	if (!catalog) {
		throw BinderException("Catalog '%s' does not exist", catalog_name.GetIdentifierName());
	}
	if (catalog->GetCatalogType() != "glue") {
		throw BinderException("glue_get_database_response only works on a Glue catalog, '%s' is a %s catalog",
		                      catalog_name.GetIdentifierName(), catalog->GetCatalogType());
	}
	auto result = make_uniq<GlueGetDatabaseResponseBindData>();
	if (!GlueAPI::GetDatabase(context, catalog->Cast<GlueCatalog>(), database_name, result->database,
	                          &result->raw_json)) {
		throw CatalogException("Database '%s' does not exist in Glue catalog '%s'", database_name,
		                       catalog_name.GetIdentifierName());
	}
	names = {"database_name", "description", "location_uri", "parameters", "response"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), LogicalType::VARIANT()};
	return std::move(result);
}

void GlueGetDatabaseResponseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueGetTableResponseState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GlueGetDatabaseResponseBindData>();
	auto &database = bind_data.database;
	auto optional_string = [](const string &value) {
		return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
	};

	output.data[0].Append(Value(database.name));
	output.data[1].Append(optional_string(database.description));
	output.data[2].Append(optional_string(database.location_uri));
	output.data[3].Append(MapToValue(database.parameters));

	// The complete Glue Database object: JSON as serialized by the AWS SDK, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[4], 1);

	output.CheckCardinality(1);
}

//===--------------------------------------------------------------------===//
// glue_describe_table: a table described the way Hive's DESCRIBE [FORMATTED] lays it out
//===--------------------------------------------------------------------===//
struct GlueDescribeTableBindData : public TableFunctionData {
	vector<array<string, 3>> rows;
};

struct GlueDescribeTableState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

//! A time as Hive prints it, e.g. "Thu Apr 23 02:55:21 UTC 2020"
string HiveTime(int64_t seconds) {
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

//! Key / value rows of a map, by key, under a "<title>:" row
void AddParameterRows(vector<array<string, 3>> &rows, const string &title,
                      const unordered_map<string, string> &parameters) {
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

vector<array<string, 3>> DescribeRows(const GlueTableInfo &table, bool formatted) {
	vector<array<string, 3>> rows;
	auto add_columns = [&](const vector<GlueColumn> &columns) {
		for (auto &column : columns) {
			rows.push_back({column.name, column.type, column.comment});
		}
	};
	// the partition keys are listed with the data columns, and again under their own heading
	add_columns(table.columns);
	add_columns(table.partition_keys);
	if (!table.partition_keys.empty()) {
		rows.push_back({"", "", ""});
		rows.push_back({"# Partition Information", "", ""});
		rows.push_back({"# col_name", "data_type", "comment"});
		rows.push_back({"", "", ""});
		add_columns(table.partition_keys);
	}
	if (!formatted) {
		return rows;
	}
	rows.push_back({"", "", ""});
	rows.push_back({"# Detailed Table Information", "", ""});
	rows.push_back({"Database:", table.database_name, ""});
	rows.push_back({"Owner:", table.owner, ""});
	rows.push_back({"CreateTime:", table.create_time >= 0 ? HiveTime(table.create_time) : "UNKNOWN", ""});
	rows.push_back({"LastAccessTime:", table.last_access_time > 0 ? HiveTime(table.last_access_time) : "UNKNOWN", ""});
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
		return rows;
	}
	vector<string> sort_columns;
	for (auto &column : table.sort_columns) {
		// Hive's Order: 1 for ascending, 0 for descending
		sort_columns.push_back(StringUtil::Format("Order(col:%s, order:%d)", column.name,
		                                          column.sort_order == GlueSortOrder::DESCENDING ? 0 : 1));
	}
	rows.push_back({"", "", ""});
	rows.push_back({"# Storage Information", "", ""});
	rows.push_back({"SerDe Library:", table.serde_library, ""});
	rows.push_back({"InputFormat:", table.input_format, ""});
	rows.push_back({"OutputFormat:", table.output_format, ""});
	rows.push_back({"Compressed:", table.compressed ? "Yes" : "No", ""});
	rows.push_back({"Num Buckets:", to_string(table.number_of_buckets), ""});
	rows.push_back({"Bucket Columns:", "[" + StringUtil::Join(table.bucket_columns, ", ") + "]", ""});
	rows.push_back({"Sort Columns:", "[" + StringUtil::Join(sort_columns, ", ") + "]", ""});
	AddParameterRows(rows, "Storage Desc Params:", table.serde_parameters);
	return rows;
}

unique_ptr<FunctionData> GlueDescribeTableBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto name = ResolveGlueTableName(context, "glue_describe_table", input.inputs[0].GetValue<string>());
	auto &catalog = Catalog::GetCatalog(context, name.Catalog()).Cast<GlueCatalog>();
	GlueTableInfo table;
	if (!GlueAPI::GetTable(context, catalog, name.Schema().GetIdentifierName(), name.Name().GetIdentifierName(),
	                       table)) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'", name.Schema().GetIdentifierName(),
		                       name.Name().GetIdentifierName(), name.Catalog().GetIdentifierName());
	}
	bool formatted = false;
	for (auto &option : input.named_parameters) {
		if (StringUtil::Lower(option.first.GetIdentifierName()) == "formatted") {
			formatted = !option.second.IsNull() && option.second.GetValue<bool>();
		}
	}
	auto result = make_uniq<GlueDescribeTableBindData>();
	result->rows = DescribeRows(table, formatted);
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

QualifiedName ResolveGlueTableName(ClientContext &context, const string &function_name, const string &table_name) {
	auto qualified = QualifiedName::Parse(table_name);
	if (qualified.Catalog().empty() || qualified.Schema().empty()) {
		// a partially qualified name: resolve it the way a query would (search path, default catalog)
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, qualified);
		auto &entry = Catalog::GetEntry(context, lookup);
		qualified = QualifiedName(entry.ParentCatalog().GetName(), Identifier(entry.ParentSchema().name),
		                          Identifier(entry.name));
	}
	auto catalog = Catalog::GetCatalogEntry(context, qualified.Catalog());
	if (!catalog) {
		throw BinderException("Catalog '%s' does not exist", qualified.Catalog().GetIdentifierName());
	}
	if (catalog->GetCatalogType() != "glue") {
		throw BinderException("%s only works on tables of a Glue catalog, '%s' is a %s catalog", function_name,
		                      qualified.Catalog().GetIdentifierName(), catalog->GetCatalogType());
	}
	return qualified;
}

TableFunction GetGlueGetDatabaseResponseFunction() {
	TableFunction function("glue_get_database_response", {LogicalType::VARCHAR}, GlueGetDatabaseResponseScan,
	                       GlueGetDatabaseResponseBind, GlueGetTableResponseInit);
	return function;
}

TableFunction GetGlueGetTableResponseFunction() {
	TableFunction function("glue_get_table_response", {LogicalType::VARCHAR}, GlueGetTableResponseScan,
	                       GlueGetTableResponseBind, GlueGetTableResponseInit);
	return function;
}

TableFunction GetGlueDescribeTableFunction() {
	TableFunction function("glue_describe_table", {LogicalType::VARCHAR}, GlueDescribeTableScan, GlueDescribeTableBind,
	                       GlueDescribeTableInit);
	function.GetSignature().WithTypedKwargs(
	    "options", [](TypedKwargs &options) { options.Add("formatted", LogicalType::BOOLEAN); });
	return function;
}

} // namespace duckdb
