#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! glue_cleanup_staging(table, older_than := INTERVAL '1 day'): removes the staging roots (.duckdb-staging-<write id>)
//! below the table location whose write is older than 'older_than'. A root that still holds its journal belongs to a
//! commit that did not finish: the files the journal lists are deleted from their final locations first. Returns one
//! row per removed root.
TableFunction GetGlueCleanupStagingFunction();

} // namespace duckdb
