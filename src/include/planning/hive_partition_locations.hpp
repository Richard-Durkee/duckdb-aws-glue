#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "core/glue_info.hpp"

namespace duckdb {

//! Which partition a data file belongs to: the one registered at the deepest location containing the file. Glue does
//! not constrain partition locations, so one partition may sit inside another's directory, and two may share one
//! (the first wins).
class HivePartitionLocations {
public:
	explicit HivePartitionLocations(const vector<GluePartitionInfo> &partitions);

	//! The partition (index into the partitions given) owning the file at 'path', searching locations no shorter than
	//! 'min_directory_size'
	optional_idx Owner(const string &path, idx_t min_directory_size) const;

private:
	//! Registered partition location (without trailing '/') to its partition index
	unordered_map<string, idx_t> partition_by_location;
};

} // namespace duckdb
