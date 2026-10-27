#pragma once

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class LogicalGet;

//! A Hive scan (of a Glue table or a hive_scan() call) as the call to a standard reader (read_parquet, read_csv,
//! read_json or read_avro) that reads any subset of the scan's data files the way the scan reads them, for an engine
//! that hands some of the files to machines without this extension. Core DuckDB types only, so a caller needs no other
//! header of this extension.
struct HiveReaderCall {
	//! read_parquet, read_csv, read_json or read_avro
	string function;
	//! The reader's named parameters, the caller adds the files: the ones the scan bound the reader with, and what a
	//! subset of the files needs besides (the data columns as `schema` for read_parquet and read_avro,
	//! hive_partitioning off)
	named_parameter_map_t parameters;
	//! The scan's data columns in order, each as an expression over the reader's column of the same name, named after
	//! the column
	vector<unique_ptr<ParsedExpression>> columns;
};

//! Nothing when 'get' is not a Hive scan bound by this build of the extension, or reads its files in a way no reader
//! call can say: a Parquet table with a binary column, whose string columns are decoded file by file, or a column the
//! scan reads as another type than the table's, which the optimizer gives a csv scan when it pushes the type of a cast
//! over the column into it (type_pushdown) and takes the cast out of the plan
optional<HiveReaderCall> HiveScanGetReaderCall(const LogicalGet &get);
//! The values of the partition a data file of the scan belongs to as the scan reads them, one constant per partition
//! key in key order, named after the key. Nothing when 'get' is not such a scan, has no partition keys, reads a key as
//! another type than the table's (see HiveScanGetReaderCall), or has not listed 'path': a scan lists its files when
//! they are first asked for (MultiFileList::GetAllFiles lists them all)
optional<vector<unique_ptr<ParsedExpression>>> HiveScanGetFilePartitionValues(const LogicalGet &get,
                                                                              const string &path);

} // namespace duckdb
