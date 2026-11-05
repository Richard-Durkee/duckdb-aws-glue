#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
class GlueCatalog;
class GlueSchemaEntry;

//! The COPY of INSERT and CTAS into a Hive table. Each execution adds a write to the transaction's writes before
//! anything is written, which registers its partitions (and creates the CTAS table) when the transaction commits and
//! is undone when it rolls back.
class GlueHiveCopy final : public PhysicalCopyToFile {
public:
	//! INSERT: no schema and info. CTAS: the schema to create in, the bound statement and the definition resolved at
	//! planning, which the commit creates as is. The files are named <write_id>_..., below the table's location, or
	//! below 'staging_root' when staged.
	GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
	             unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality, GlueCatalog &catalog,
	             optional_ptr<GlueSchemaEntry> schema, unique_ptr<BoundCreateTableInfo> info, GlueTableInfo table_info,
	             const string &write_id, string staging_root);

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	string GetName() const override;

private:
	GlueCatalog &catalog;
	optional_ptr<GlueSchemaEntry> schema;
	unique_ptr<BoundCreateTableInfo> info;
	GlueTableInfo table_info;
	string write_id;
	string staging_root;
	mutable annotated_mutex start_lock;
};

} // namespace duckdb
