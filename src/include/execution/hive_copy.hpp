#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
class GlueSchemaEntry;
class GlueTransaction;
struct GlueTableWrite;

//! The COPY of INSERT and CTAS into a Hive table; CTAS creates the Glue table when the sink starts, like PhysicalInsert
class GlueHiveCopy final : public PhysicalCopyToFile {
public:
	//! INSERT: no schema and info. CTAS: the schema to create in, the bound statement and the definition resolved at
	//! planning, which the create uses as is
	GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
	             unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality, optional_ptr<GlueSchemaEntry> schema,
	             unique_ptr<BoundCreateTableInfo> info, GlueTableInfo table_info, bool overwrite_location,
	             string staging_root);

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	string GetName() const override;
	//! The write of the current execution in the transaction's write set
	GlueTableWrite &CurrentWrite() const;

	//! The catalog of the table
	optional_ptr<Catalog> table_catalog;
	//! The start of the name of every file this plan writes
	string file_prefix;

private:
	optional_ptr<GlueSchemaEntry> schema;
	unique_ptr<BoundCreateTableInfo> info;
	GlueTableInfo table_info;
	bool overwrite_location;
	string staging_root;
	mutable annotated_mutex create_lock;
	mutable optional_ptr<GlueTableWrite> current_write DUCKDB_GUARDED_BY(create_lock);
	mutable optional_ptr<GlueTransaction> current_transaction DUCKDB_GUARDED_BY(create_lock);
};

} // namespace duckdb
