#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
class GlueSchemaEntry;

//! The COPY of INSERT and CTAS into a Hive table; CTAS creates the Glue table when the sink starts, like PhysicalInsert
class GlueHiveCopy final : public PhysicalCopyToFile {
public:
	//! INSERT: no schema and info. CTAS: the schema to create in, the bound statement and the definition resolved at
	//! planning, which the create uses as is
	GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
	             unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality, optional_ptr<GlueSchemaEntry> schema,
	             unique_ptr<BoundCreateTableInfo> info, GlueTableInfo table_info);

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	string GetName() const override;

private:
	optional_ptr<GlueSchemaEntry> schema;
	unique_ptr<BoundCreateTableInfo> info;
	GlueTableInfo table_info;
	mutable annotated_mutex create_lock;
	mutable bool table_created DUCKDB_GUARDED_BY(create_lock) = false;
};

} // namespace duckdb
