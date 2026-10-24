#include "functions/glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "api/glue_api.hpp"
#include "api/glue_api_util.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"

#include <aws/core/utils/json/JsonSerializer.h>

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
	auto database_name = name.Schema().GetIdentifierName();
	auto table_name = name.Name().GetIdentifierName();
	// Always fetches: the 'response' column carries the raw reply, which only a fresh call has. The fetched
	// table warms the metadata cache for the binds that follow.
	GlueTableInfo info;
	if (!GlueAPI::GetTable(context, catalog, database_name, table_name, info, &result->raw_json)) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'", database_name, table_name,
		                       name.Catalog().GetIdentifierName());
	}
	result->table = info;
	// Of() is null in a bare statement (nothing started the catalog's transaction); Get() starts it
	auto &cache = *GlueTransaction::Get(context, catalog).cache;
	auto fetched = make_shared_ptr<const GlueTableInfo>(std::move(info));
	cache.GetTable(database_name, table_name, [&fetched]() { return fetched; });

	auto column_type = GlueColumnsType();
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
	output.data[2].Append(Value(table.GetFormat().ToString()));
	output.data[3].Append(Value(table.glue_table_type));
	output.data[4].Append(Value(table.location));
	output.data[5].Append(Value(table.serde_library));
	output.data[6].Append(GlueColumnsToValue(table.columns));
	output.data[7].Append(GlueColumnsToValue(table.partition_keys));
	output.data[8].Append(GlueParametersToValue(table.parameters));
	output.data[9].Append(GlueParametersToValue(table.serde_parameters));

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
	output.data[3].Append(GlueParametersToValue(database.parameters));

	// The complete Glue Database object: JSON as serialized by the AWS SDK, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[4], 1);

	output.CheckCardinality(1);
}

} // namespace

LogicalType GlueColumnsType() {
	return LogicalType::LIST(LogicalType::STRUCT(
	    {{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}, {"comment", LogicalType::VARCHAR}}));
}

Value GlueColumnsToValue(const vector<GlueColumn> &columns) {
	vector<Value> entries;
	for (auto &column : columns) {
		entries.push_back(
		    Value::STRUCT({{"name", Value(column.name)},
		                   {"type", Value(column.type)},
		                   {"comment", column.comment.empty() ? Value(LogicalType::VARCHAR) : Value(column.comment)}}));
	}
	return Value::LIST(ListType::GetChildType(GlueColumnsType()), std::move(entries));
}

Value GlueParametersToValue(const unordered_map<string, string> &parameters) {
	vector<Value> keys;
	vector<Value> values;
	for (auto &entry : parameters) {
		keys.emplace_back(entry.first);
		values.emplace_back(entry.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

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

namespace {

using Aws::Utils::Json::JsonValue;
using Aws::Utils::Json::JsonView;

//! Parse a JSON column list: [{"name": "id", "type": "int", "comment": "..."}, ...]
vector<GlueColumn> ParseColumnsFromJSON(JsonView arr) {
	vector<GlueColumn> result;
	if (!arr.IsListType()) {
		return result;
	}
	for (auto &elem : arr.AsArray()) {
		GlueColumn col;
		if (elem.ValueExists("name")) {
			col.name = ToStdString(elem.GetString("name"));
		}
		if (elem.ValueExists("type")) {
			col.type = ToStdString(elem.GetString("type"));
		}
		if (elem.ValueExists("comment")) {
			col.comment = ToStdString(elem.GetString("comment"));
		}
		result.push_back(std::move(col));
	}
	return result;
}

//! Parse a JSON string map: {"key": "value", ...}
unordered_map<string, string> ParseStringMapFromJSON(JsonView obj) {
	unordered_map<string, string> result;
	if (!obj.IsObject()) {
		return result;
	}
	for (auto &entry : obj.GetAllObjects()) {
		if (entry.second.IsString()) {
			result[ToStdString(entry.first)] = ToStdString(entry.second.AsString());
		}
	}
	return result;
}

//! Parse a JSON object into a GlueTableInfo, the same struct GlueAPI::GetTable populates from an AWS SDK response
GlueTableInfo ParseTableInfoFromJSON(JsonView root, const string &database_name, const string &table_name) {
	GlueTableInfo info;
	info.name = table_name;
	info.database_name = database_name;
	if (root.ValueExists("location")) {
		info.location = ToStdString(root.GetString("location"));
	}
	if (root.ValueExists("serde_library")) {
		info.serde_library = ToStdString(root.GetString("serde_library"));
	}
	if (root.ValueExists("input_format")) {
		info.input_format = ToStdString(root.GetString("input_format"));
	}
	if (root.ValueExists("output_format")) {
		info.output_format = ToStdString(root.GetString("output_format"));
	}
	if (root.ValueExists("description")) {
		info.description = ToStdString(root.GetString("description"));
	}
	if (root.ValueExists("table_type")) {
		info.glue_table_type = ToStdString(root.GetString("table_type"));
	}
	if (info.glue_table_type.empty()) {
		info.glue_table_type = "EXTERNAL_TABLE";
	}
	info.table_type = GlueTableTypeFromString(info.glue_table_type);
	if (root.ValueExists("number_of_buckets")) {
		info.number_of_buckets = root.GetInteger("number_of_buckets");
	}
	if (root.ValueExists("columns")) {
		info.columns = ParseColumnsFromJSON(root.GetArray("columns"));
	}
	if (root.ValueExists("partition_keys")) {
		info.partition_keys = ParseColumnsFromJSON(root.GetArray("partition_keys"));
	}
	if (root.ValueExists("parameters")) {
		info.parameters = ParseStringMapFromJSON(root.GetObject("parameters"));
	}
	if (root.ValueExists("serde_parameters")) {
		info.serde_parameters = ParseStringMapFromJSON(root.GetObject("serde_parameters"));
	}
	return info;
}

struct GluePrefetchTableBindData : public TableFunctionData {
	string catalog_name;
	string database_name;
	string table_name;
	GlueTableInfo table_info;
};

unique_ptr<FunctionData> GluePrefetchTableBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto table_ref = input.inputs[0].GetValue<string>();
	auto json_str = input.inputs[1].GetValue<string>();

	auto qualified = ResolveGlueTableName(context, "glue_prefetch_table", table_ref);
	auto &catalog = Catalog::GetCatalog(context, qualified.Catalog()).Cast<GlueCatalog>();
	auto database_name = qualified.Schema().GetIdentifierName();
	auto table_name = qualified.Name().GetIdentifierName();

	// Parse the JSON into a GlueTableInfo
	JsonValue json(json_str);
	if (!json.WasParseSuccessful()) {
		throw InvalidInputException("glue_prefetch_table: invalid JSON");
	}
	auto root = json.View();
	if (!root.IsObject()) {
		throw InvalidInputException("glue_prefetch_table: JSON must be an object");
	}

	auto result = make_uniq<GluePrefetchTableBindData>();
	result->catalog_name = qualified.Catalog().GetIdentifierName();
	result->database_name = database_name;
	result->table_name = table_name;
	result->table_info = ParseTableInfoFromJSON(root, database_name, table_name);

	// Pre-populate the transaction cache: the same pattern BulkLoadTableInfo and glue_get_table_response use
	auto &cache = *GlueTransaction::Get(context, catalog).cache;
	auto prefetched = make_shared_ptr<const GlueTableInfo>(result->table_info);
	cache.GetTable(database_name, table_name, [&prefetched]() { return prefetched; });

	names = {"database_name", "table_name", "status"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}

void GluePrefetchTableScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueGetTableResponseState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GluePrefetchTableBindData>();
	output.data[0].Append(Value(bind_data.database_name));
	output.data[1].Append(Value(bind_data.table_name));
	output.data[2].Append(Value("prefetched"));
	output.CheckCardinality(1);
}

} // namespace

TableFunction GetGluePrefetchTableFunction() {
	TableFunction function("glue_prefetch_table", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       GluePrefetchTableScan, GluePrefetchTableBind, GlueGetTableResponseInit);
	return function;
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

} // namespace duckdb
