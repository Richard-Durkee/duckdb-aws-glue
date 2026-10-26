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

[[noreturn]] void ThrowInvalidPrefetch(const string &message) {
	throw InvalidInputException("glue_prefetch_table: %s", message);
}

string StringField(JsonView value, const string &field, bool required) {
	if (value.IsNull()) {
		if (required) {
			ThrowInvalidPrefetch(StringUtil::Format("'%s' is required", field));
		}
		return string();
	}
	if (!value.IsString()) {
		ThrowInvalidPrefetch(StringUtil::Format("'%s' must be a string", field));
	}
	auto result = ToStdString(value.AsString());
	if (required && result.empty()) {
		ThrowInvalidPrefetch(StringUtil::Format("'%s' must not be empty", field));
	}
	return result;
}

//! The value of 'key', a JSON null when the object does not have it (a missing value's view is not null)
JsonView Field(JsonView object, const string &key) {
	static const JsonValue null_value("null");
	return object.ValueExists(key) ? object.GetObject(key) : null_value.View();
}

//! [{"name": ..., "type": ..., "comment": ...}, ...], or [{"name": ..., "sort_order": "ASC" | "DESC"}, ...]
vector<GlueColumn> ColumnsField(JsonView value, const string &field, bool sort_columns) {
	vector<GlueColumn> result;
	if (value.IsNull()) {
		return result;
	}
	if (!value.IsListType()) {
		ThrowInvalidPrefetch(StringUtil::Format("'%s' must be a list of objects", field));
	}
	auto entries = value.AsArray();
	for (size_t i = 0; i < entries.GetLength(); i++) {
		auto entry = entries[i];
		auto name = StringUtil::Format("%s[%d]", field, i);
		if (!entry.IsObject()) {
			ThrowInvalidPrefetch(StringUtil::Format("'%s' must be an object", name));
		}
		GlueColumn column;
		column.name = StringField(Field(entry, "name"), name + ".name", true);
		if (sort_columns) {
			auto order = StringUtil::Upper(StringField(Field(entry, "sort_order"), name + ".sort_order", true));
			if (order != "ASC" && order != "DESC") {
				ThrowInvalidPrefetch(StringUtil::Format("'%s.sort_order' must be 'ASC' or 'DESC'", name));
			}
			column.sort_order = order == "ASC" ? GlueSortOrder::ASCENDING : GlueSortOrder::DESCENDING;
		} else {
			column.type = StringField(Field(entry, "type"), name + ".type", true);
			column.comment = StringField(Field(entry, "comment"), name + ".comment", false);
		}
		result.push_back(std::move(column));
	}
	return result;
}

unordered_map<string, string> StringMapField(JsonView value, const string &field) {
	unordered_map<string, string> result;
	if (value.IsNull()) {
		return result;
	}
	if (!value.IsObject()) {
		ThrowInvalidPrefetch(StringUtil::Format("'%s' must be an object of strings", field));
	}
	for (auto &entry : value.GetAllObjects()) {
		auto key = ToStdString(entry.first);
		result[key] = StringField(entry.second, field + "." + key, false);
	}
	return result;
}

vector<string> StringListField(JsonView value, const string &field) {
	vector<string> result;
	if (value.IsNull()) {
		return result;
	}
	if (!value.IsListType()) {
		ThrowInvalidPrefetch(StringUtil::Format("'%s' must be a list of strings", field));
	}
	auto entries = value.AsArray();
	for (size_t i = 0; i < entries.GetLength(); i++) {
		result.push_back(StringField(entries[i], StringUtil::Format("%s[%d]", field, i), true));
	}
	return result;
}

//! The table definition, with the fields of a glue_get_table_response row plus the rest of its storage descriptor
GlueTableInfo ParseTableInfo(JsonView root, const string &database_name, const string &table_name) {
	static const vector<string> fields = {"glue_table_type", "location",       "serde_library",  "serde_parameters",
	                                      "columns",         "partition_keys", "parameters",     "input_format",
	                                      "output_format",   "description",    "bucket_columns", "number_of_buckets",
	                                      "sort_columns"};
	// the other columns of a glue_get_table_response row
	static const vector<string> ignored = {"database_name", "table_name", "table_type", "response"};
	for (auto &entry : root.GetAllObjects()) {
		auto key = ToStdString(entry.first);
		if (std::find(fields.begin(), fields.end(), key) == fields.end() &&
		    std::find(ignored.begin(), ignored.end(), key) == ignored.end()) {
			ThrowInvalidPrefetch(
			    StringUtil::Format("unknown field '%s', the fields are: %s", key, StringUtil::Join(fields, ", ")));
		}
	}
	GlueTableInfo info;
	info.name = table_name;
	info.database_name = database_name;
	info.glue_table_type = StringField(Field(root, "glue_table_type"), "glue_table_type", false);
	if (info.glue_table_type.empty()) {
		info.glue_table_type = "EXTERNAL_TABLE";
	}
	info.table_type = GlueTableTypeFromString(info.glue_table_type);
	if (info.IsView()) {
		ThrowInvalidPrefetch("views can not be prefetched");
	}
	info.location = StringField(Field(root, "location"), "location", true);
	info.serde_library = StringField(Field(root, "serde_library"), "serde_library", true);
	info.serde_parameters = StringMapField(Field(root, "serde_parameters"), "serde_parameters");
	info.columns = ColumnsField(Field(root, "columns"), "columns", false);
	if (info.columns.empty()) {
		ThrowInvalidPrefetch("'columns' must list the table's columns");
	}
	info.partition_keys = ColumnsField(Field(root, "partition_keys"), "partition_keys", false);
	info.parameters = StringMapField(Field(root, "parameters"), "parameters");
	info.input_format = StringField(Field(root, "input_format"), "input_format", false);
	info.output_format = StringField(Field(root, "output_format"), "output_format", false);
	info.description = StringField(Field(root, "description"), "description", false);
	info.bucket_columns = StringListField(Field(root, "bucket_columns"), "bucket_columns");
	auto buckets = Field(root, "number_of_buckets");
	if (!buckets.IsNull()) {
		if (!buckets.IsIntegerType()) {
			ThrowInvalidPrefetch("'number_of_buckets' must be an integer");
		}
		info.number_of_buckets = buckets.AsInteger();
	}
	info.sort_columns = ColumnsField(Field(root, "sort_columns"), "sort_columns", true);
	return info;
}

struct GluePrefetchTableBindData : public TableFunctionData {
	string database_name;
	string table_name;
	bool stored = false;
};

unique_ptr<FunctionData> GluePrefetchTableBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		ThrowInvalidPrefetch("the table and its definition must not be NULL");
	}
	auto table_ref = input.inputs[0].GetValue<string>();
	// a partial name is resolved through the catalog, which asks Glue for the table
	auto parsed = QualifiedName::Parse(table_ref);
	if (parsed.Catalog().empty() || parsed.Schema().empty()) {
		throw BinderException("glue_prefetch_table needs the fully qualified table, '<catalog>.<database>.<table>', "
		                      "got '%s'",
		                      table_ref);
	}
	auto qualified = ResolveGlueTableName(context, "glue_prefetch_table", table_ref);
	if (!GlueTransactionCache::ConnectionScoped(context) && context.transaction.IsAutoCommit()) {
		ThrowInvalidPrefetch("the metadata cache is kept for the transaction, which ends with this statement. Call it "
		                     "inside BEGIN ... COMMIT, or SET glue_metadata_cache_scope = 'connection'");
	}
	JsonValue json(input.inputs[1].GetValue<string>());
	if (!json.WasParseSuccessful()) {
		ThrowInvalidPrefetch("the definition is not valid JSON");
	}
	auto root = json.View();
	if (!root.IsObject()) {
		ThrowInvalidPrefetch("the definition must be a JSON object");
	}
	auto result = make_uniq<GluePrefetchTableBindData>();
	result->database_name = qualified.Schema().GetIdentifierName();
	result->table_name = qualified.Name().GetIdentifierName();
	auto info = make_shared_ptr<const GlueTableInfo>(ParseTableInfo(root, result->database_name, result->table_name));

	auto &catalog = Catalog::GetCatalog(context, qualified.Catalog()).Cast<GlueCatalog>();
	auto &cache = *GlueTransaction::Get(context, catalog).cache;
	cache.GetTable(result->database_name, result->table_name, [&]() {
		result->stored = true;
		return info;
	});

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
	output.data[2].Append(Value(bind_data.stored ? "prefetched" : "already cached"));
	output.CheckCardinality(1);
}

} // namespace

TableFunction GetGluePrefetchTableFunction() {
	TableFunction function("glue_prefetch_table", {LogicalType::VARCHAR, LogicalType::VARCHAR}, GluePrefetchTableScan,
	                       GluePrefetchTableBind, GlueGetTableResponseInit);
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
