#include "planning/hive_partition_locations.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

HivePartitionLocations::HivePartitionLocations(const vector<GluePartitionInfo> &partitions) {
	for (idx_t i = 0; i < partitions.size(); i++) {
		auto location = partitions[i].location;
		StringUtil::RTrim(location, "/");
		if (location.empty()) {
			continue;
		}
		partition_by_location.emplace(std::move(location), i);
	}
}

optional_idx HivePartitionLocations::Owner(const string &path, idx_t min_directory_size) const {
	auto separator = path.find_last_of('/');
	if (separator == string::npos) {
		return optional_idx();
	}
	auto directory = path.substr(0, separator);
	while (directory.size() >= min_directory_size) {
		auto entry = partition_by_location.find(directory);
		if (entry != partition_by_location.end()) {
			return optional_idx(entry->second);
		}
		auto parent = directory.find_last_of('/');
		if (parent == string::npos) {
			break;
		}
		directory = directory.substr(0, parent);
	}
	return optional_idx();
}

} // namespace duckdb
