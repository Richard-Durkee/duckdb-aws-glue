#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "core/glue_info.hpp"
#include "core/serde/hive_csv_dialect.hpp"
#include "core/serde/hive_json_dialect.hpp"

namespace duckdb {

using HiveSerDeParameters = unordered_map<string, string>;

//! The SerDe of a SerializationLibrary; false for one DuckDB can not read
bool TryGetHiveSerDe(const string &library, HiveSerDe &result);
HiveFileFormat HiveSerDeFileFormat(HiveSerDe serde);
//! The class name, e.g. 'OpenCSVSerde'
string HiveSerDeName(HiveSerDe serde);
//! The SerializationLibrary CREATE TABLE writes
string HiveSerDeLibrary(HiveSerDe serde);
//! The InputFormat and OutputFormat CREATE TABLE writes
string HiveSerDeInputFormat(HiveSerDe serde);
string HiveSerDeOutputFormat(HiveSerDe serde);
//! The SerDe CREATE TABLE writes a table of a format other than csv with
HiveSerDe HiveSerDeForFileFormat(HiveFileFormat file_format);
//! The SerDe parameters CREATE TABLE writes, without options
HiveSerDeParameters HiveSerDeNewTableParameters(HiveSerDe serde);
//! Whether CREATE TABLE writes the SerDe parameter 'key' (case-insensitive) for a table of some SerDe
bool IsCreatedHiveSerDeParameter(const string &key);

//! Throws NotImplementedException for a table DuckDB can not read or write
void CheckHiveSerDe(HiveSerDe serde, const GlueTableInfo &table);
//! Throws NotImplementedException for columns the SerDe stores in a way DuckDB can not read or write
void CheckHiveSerDeColumns(HiveSerDe serde, const GlueTableInfo &table);

//! The dialect of a csv table that sets none of its SerDe's properties
HiveCSVDialect DefaultCSVDialect(HiveSerDe serde);
//! The dialect of a csv table CREATE TABLE creates, before its delimiter, quote and escape options
HiveCSVDialect NewTableCSVDialect(HiveSerDe serde);
//! The dialect a csv table's SerDe properties describe, without refusing what DuckDB can not read or write and
//! without the header lines
HiveCSVDialect ReadCSVDialect(HiveSerDe serde, const GlueTableInfo &table);
//! The dialect of a csv table's files; throws NotImplementedException for a table DuckDB can not read or write
HiveCSVDialect ParseCSVDialect(HiveSerDe serde, const GlueTableInfo &table);
//! The SerDe parameters of a csv table of 'dialect', a dialect CREATE TABLE creates tables of
HiveSerDeParameters FormatCSVDialect(HiveSerDe serde, const HiveCSVDialect &dialect);

//! The dialect of a json table's files; throws NotImplementedException for a table DuckDB can not read or write
HiveJSONDialect ParseJSONDialect(HiveSerDe serde, const GlueTableInfo &table);

} // namespace duckdb
