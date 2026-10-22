#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! How much of a table glue_get_table_response(..., format := ...) describes
enum class GlueDescribeFormat : uint8_t {
	//! the columns and partition keys
	DESCRIBE,
	//! and the table's (or partition's) details, parameters and storage
	FORMATTED,
	//! and its complete Glue definition
	EXTENDED
};

//! One row of the description: what kind of entry it is, its name, value and comment
struct GlueDescribeRow {
	string category;
	string name;
	string value;
	string comment;
};

//! Describe a Hive table, or one of its partitions when 'partition_json' is given, from the Glue Table (and
//! Partition) objects as JSON
vector<GlueDescribeRow> GlueDescribe(GlueDescribeFormat format, const string &table_json,
                                     const string *partition_json = nullptr);

} // namespace duckdb
