#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "core/glue_info.hpp"
#include "core/serde/hive_csv_dialect.hpp"
#include "core/serde/hive_json_dialect.hpp"

namespace duckdb {

enum class HiveSerDeType : uint8_t { PARQUET, LAZY_SIMPLE, OPEN_CSV, HIVE_JSON, OPENX_JSON, AVRO };

using HiveSerDeParameters = unordered_map<string, string>;

//! A SerDe (SerializationLibrary) DuckDB reads and writes the data files of a Hive table with
class HiveSerDe {
public:
	explicit HiveSerDe(HiveSerDeType type) : type(type) {
	}

public:
	//! The SerDe of a SerializationLibrary; false for one DuckDB can not read
	static bool TryFind(const string &library, HiveSerDeType &result);
	//! The SerDe CREATE TABLE writes a table of a format other than csv with
	static HiveSerDe ForFileFormat(HiveFileFormat file_format);
	//! Whether CREATE TABLE writes the SerDe parameter 'key' (case-insensitive) for a table of some SerDe
	static bool IsCreatedParameter(const string &key);

	HiveSerDeType Type() const {
		return type;
	}
	HiveFileFormat FileFormat() const;
	//! The class name, e.g. 'OpenCSVSerde'
	string Name() const;
	//! The SerializationLibrary CREATE TABLE writes
	string Library() const;
	//! The InputFormat and OutputFormat CREATE TABLE writes
	string InputFormat() const;
	string OutputFormat() const;
	//! The SerDe parameters CREATE TABLE writes, without options
	HiveSerDeParameters NewTableParameters() const;
	//! Throws NotImplementedException for a table of this SerDe DuckDB can not read or write
	void Check(const GlueTableInfo &table) const;

	//! The dialect of a csv table that sets none of the SerDe's properties
	HiveCSVDialect DefaultCSVDialect() const;
	//! The dialect of a csv table CREATE TABLE creates, before its delimiter, quote and escape options
	HiveCSVDialect NewTableCSVDialect() const;
	//! The dialect the table's SerDe properties describe, without refusing what DuckDB can not read or write and
	//! without the header lines
	HiveCSVDialect ReadCSVDialect(const GlueTableInfo &table) const;
	//! The dialect of a csv table's files; throws NotImplementedException for a table DuckDB can not read or write
	HiveCSVDialect ParseCSVDialect(const GlueTableInfo &table) const;
	//! The SerDe parameters of a csv table of 'dialect', a dialect CREATE TABLE creates tables of
	HiveSerDeParameters FormatCSVDialect(const HiveCSVDialect &dialect) const;
	//! The dialect of a json table's files; throws NotImplementedException for a table DuckDB can not read or write
	HiveJSONDialect ParseJSONDialect(const GlueTableInfo &table) const;

private:
	HiveSerDeType type;
};

} // namespace duckdb
