#include "functions/glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "functions/glue_describe.hpp"

namespace duckdb {

namespace {

struct GlueGetTableResponseBindData : public TableFunctionData {
	GlueTableInfo table;
	string raw_json;
	//! With 'format': a description of the table instead of the response
	bool describe = false;
	vector<GlueDescribeRow> describe_rows;
};

struct GlueGetTableResponseState : public GlobalTableFunctionState {
	bool done = false;
	idx_t offset = 0;
};

GlueDescribeFormat ParseDescribeFormat(const Value &value) {
	auto format = StringUtil::Lower(value.ToString());
	if (format == "describe") {
		return GlueDescribeFormat::DESCRIBE;
	}
	if (format == "formatted") {
		return GlueDescribeFormat::FORMATTED;
	}
	if (format == "extended") {
		return GlueDescribeFormat::EXTENDED;
	}
	throw BinderException(
	    "glue_get_table_response: unknown format '%s', expected 'describe', 'extended' or 'formatted'",
	    value.ToString());
}

//! Describe a table, or one of its partitions, for glue_get_table_response(..., format := ...)
vector<GlueDescribeRow> DescribeGlueTable(ClientContext &context, GlueCatalog &catalog, const GlueTableInfo &table,
                                          const string &raw_json, GlueDescribeFormat format,
                                          optional_ptr<const Value> partition) {
	auto table_name = table.database_name + "." + table.name;
	if (!partition) {
		return GlueDescribe(format, raw_json);
	}
	auto table_format = table.GetFormat();
	if (table.IsView() || !table_format.IsHive()) {
		throw NotImplementedException("glue_get_table_response: 'partition' describes partitions of Hive tables, '%s' "
		                              "is a %s",
		                              table_name,
		                              table.IsView() ? "Glue view" : table_format.Describe(table) + " table");
	}
	if (table.partition_keys.empty()) {
		throw InvalidInputException("Table '%s' is not partitioned", table_name);
	}
	auto values = ParseGluePartitionSpec("glue_get_table_response", table, *partition);
	GluePartitionInfo partition_info;
	string partition_json;
	if (!GlueAPI::GetPartition(context, catalog, table.database_name, table.name, values, partition_info,
	                           &partition_json)) {
		vector<string> spec;
		for (idx_t i = 0; i < values.size(); i++) {
			spec.push_back(table.partition_keys[i].name + "=" + values[i]);
		}
		throw CatalogException("Table '%s' has no partition %s", table_name, StringUtil::Join(spec, ", "));
	}
	return GlueDescribe(format, raw_json, &partition_json);
}

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
	optional_ptr<const Value> format;
	optional_ptr<const Value> partition;
	for (auto &option : input.named_parameters) {
		auto option_name = StringUtil::Lower(option.first.GetIdentifierName());
		if (option_name == "format") {
			format = &option.second;
		} else if (option_name == "partition") {
			partition = &option.second;
		}
	}
	if (partition && !format) {
		throw BinderException("glue_get_table_response: 'partition' describes a partition and needs a 'format'");
	}
	if (format) {
		result->describe = true;
		result->describe_rows =
		    DescribeGlueTable(context, catalog, info, result->raw_json, ParseDescribeFormat(*format), partition);
	}
	result->table = info;
	// Of() is null in a bare statement (nothing started the catalog's transaction); Get() starts it
	auto &cache = *GlueTransaction::Get(context, catalog).cache;
	auto fetched = make_shared_ptr<const GlueTableInfo>(std::move(info));
	cache.GetTable(database_name, table_name, [&fetched]() { return fetched; });

	if (result->describe) {
		names = {"category", "name", "value", "comment"};
		return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
		return std::move(result);
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
	auto &bind_data = data.bind_data->Cast<GlueGetTableResponseBindData>();
	if (bind_data.describe) {
		auto &rows = bind_data.describe_rows;
		idx_t count = 0;
		for (; state.offset < rows.size() && count < STANDARD_VECTOR_SIZE; state.offset++, count++) {
			auto &row = rows[state.offset];
			output.data[0].Append(Value(row.category));
			output.data[1].Append(Value(row.name));
			output.data[2].Append(Value(row.value));
			output.data[3].Append(Value(row.comment));
		}
		output.CheckCardinality(count);
		return;
	}
	if (state.done) {
		return;
	}
	state.done = true;
	auto &table = bind_data.table;

	output.data[0].Append(Value(table.database_name));
	output.data[1].Append(Value(table.name));
	output.data[2].Append(Value(table.GetFormat().ToString()));
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
	// a description of the table (or one of its partitions) instead of the response
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("format", LogicalType::VARCHAR);
		options.Add("partition", LogicalType::ANY);
	});
	return function;
}

} // namespace duckdb
