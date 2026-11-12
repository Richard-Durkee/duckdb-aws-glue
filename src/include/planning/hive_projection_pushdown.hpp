#pragma once

#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

//! A projected column of the scan that is a partition key
struct PartitionKeyProjection {
	idx_t projected_column_index;
	idx_t partition_key_index;
};

//! Replace references to partition columns of the scanned table by the partition's values
void ReplacePartitionColumnRefs(ClientContext &context, unique_ptr<Expression> &expr, TableIndex table_index,
                                const vector<PartitionKeyProjection> &projections, const HiveScanInfo &info,
                                const GluePartitionInfo &partition);

//! The partitions of a projected table this scan can read: the projected values of each partition key, narrowed by the
//! filters on that key alone before they are combined, so a filtered scan never builds the partitions it rules out.
//! An injected key takes its values from the filters. 'projected' is the number of partitions without the filters,
//! and 'pruning_filters' receives the filters that narrowed a key.
vector<GluePartitionInfo> ProjectPartitions(ClientContext &context, const HiveScanInfo &info, TableIndex table_index,
                                            const vector<PartitionKeyProjection> &projections,
                                            const vector<unique_ptr<Expression>> &filters, idx_t &projected,
                                            unordered_set<idx_t> &pruning_filters);

} // namespace duckdb
