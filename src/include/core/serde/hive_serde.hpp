#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
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
	HiveSerDe(HiveSerDeType type, HiveFileFormat file_format, string name, string library, string library_pattern);
	virtual ~HiveSerDe() = default;

public:
	//! The SerDe of a SerializationLibrary, null for one DuckDB can not read
	static optional_ptr<const HiveSerDe> Find(const string &library);
	static const HiveSerDe &Get(HiveSerDeType type);
	//! The SerDe CREATE TABLE writes a table of a format other than csv with
	static const HiveSerDe &ForFileFormat(HiveFileFormat file_format);
	//! Whether CREATE TABLE writes the SerDe parameter 'key' (case-insensitive) for a table of some SerDe
	static bool IsCreatedParameter(const string &key);

	HiveSerDeType Type() const {
		return type;
	}
	HiveFileFormat FileFormat() const {
		return file_format;
	}
	//! The class name, e.g. 'OpenCSVSerde'
	const string &Name() const {
		return name;
	}
	//! The SerializationLibrary CREATE TABLE writes
	const string &Library() const {
		return library;
	}
	//! The InputFormat and OutputFormat CREATE TABLE writes
	string InputFormat() const;
	string OutputFormat() const;
	//! The SerDe parameters CREATE TABLE writes, without options
	virtual HiveSerDeParameters NewTableParameters() const;
	//! Throws NotImplementedException for a table of this SerDe DuckDB can not read or write
	virtual void Check(const GlueTableInfo &table) const;
	//! Throws NotImplementedException for columns this SerDe stores in a way DuckDB can not read or write
	virtual void CheckColumns(const GlueTableInfo &table) const;

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	//! Throws NotImplementedException: the table 'what', which DuckDB can not read or write
	[[noreturn]] static void Refuse(const GlueTableInfo &table, const string &what);
	//! A non-negative integer property, 0 when not set
	static idx_t CountProperty(const GlueTableInfo &table, const string &key);
	static bool IsTrueProperty(const GlueTableInfo &table, const string &key);

private:
	HiveSerDeType type;
	HiveFileFormat file_format;
	string name;
	string library;
	//! A SerializationLibrary (lower case) that contains it is this SerDe
	string library_pattern;
};

//! A SerDe of csv files: what it reads from the table's properties is a HiveCSVDialect
class HiveCSVSerDe : public HiveSerDe {
public:
	using HiveSerDe::HiveSerDe;

public:
	//! The dialect of the table's files; throws NotImplementedException for a table DuckDB can not read or write
	HiveCSVDialect Parse(const GlueTableInfo &table) const;
	//! The dialect the table's SerDe properties describe, without refusing what DuckDB can not read or write and
	//! without the header lines
	HiveCSVDialect ReadDialect(const GlueTableInfo &table) const;
	//! The dialect of a table that sets none of the SerDe's properties
	virtual HiveCSVDialect DefaultDialect() const = 0;
	//! The dialect of a table CREATE TABLE creates, before its delimiter, quote and escape options
	virtual HiveCSVDialect NewTableDialect() const = 0;
	//! The SerDe parameters of a table of 'dialect', a dialect CREATE TABLE creates tables of
	virtual HiveSerDeParameters Format(const HiveCSVDialect &dialect) const = 0;
	HiveSerDeParameters NewTableParameters() const override;
	void Check(const GlueTableInfo &table) const override;
	void CheckColumns(const GlueTableInfo &table) const override;

protected:
	//! Applies the SerDe properties the table sets to 'dialect'
	virtual void ReadProperties(const GlueTableInfo &table, HiveCSVDialect &dialect) const = 0;
	//! Throws NotImplementedException for SerDe properties DuckDB can not read or write
	virtual void CheckProperties(const GlueTableInfo &table) const = 0;
};

//! A SerDe of newline-delimited JSON files: what it reads from the table's properties is a HiveJSONDialect
class HiveJSONSerDe : public HiveSerDe {
public:
	using HiveSerDe::HiveSerDe;

public:
	//! The dialect of the table's files; throws NotImplementedException for a table DuckDB can not read or write
	HiveJSONDialect Parse(const GlueTableInfo &table) const;
	void Check(const GlueTableInfo &table) const override;

protected:
	virtual HiveJSONDialect ReadDialect(const GlueTableInfo &table) const = 0;
	//! Records 'key' as not applied when it is set (to 'true' with only_if_true)
	static void NoteUnsupported(const GlueTableInfo &table, const string &key, bool only_if_true,
	                            HiveJSONDialect &dialect);
};

//! org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe: unquoted fields, a NULL string, invalid values are NULL
class LazySimpleSerDe : public HiveCSVSerDe {
public:
	LazySimpleSerDe();

public:
	HiveCSVDialect DefaultDialect() const override;
	HiveCSVDialect NewTableDialect() const override;
	HiveSerDeParameters Format(const HiveCSVDialect &dialect) const override;

protected:
	void ReadProperties(const GlueTableInfo &table, HiveCSVDialect &dialect) const override;
	void CheckProperties(const GlueTableInfo &table) const override;

private:
	//! Hive's LazyUtils.getByte: a byte code from -128 to 127 ('1' is '\001'), else the first character
	static string FieldDelimiter(const GlueTableInfo &table);
	static string NullString(const GlueTableInfo &table);
};

//! org.apache.hadoop.hive.serde2.OpenCSVSerde: quoted fields, no NULL, temporal values as numbers since the epoch
class OpenCSVSerDe : public HiveCSVSerDe {
public:
	OpenCSVSerDe();

public:
	HiveCSVDialect DefaultDialect() const override;
	HiveCSVDialect NewTableDialect() const override;
	HiveSerDeParameters Format(const HiveCSVDialect &dialect) const override;

protected:
	void ReadProperties(const GlueTableInfo &table, HiveCSVDialect &dialect) const override;
	void CheckProperties(const GlueTableInfo &table) const override;

private:
	//! A character property (its first character), 'fallback' when not set
	static string Character(const GlueTableInfo &table, const string &key, const string &fallback);
	static string Quote(const GlueTableInfo &table);
	static string Escape(const GlueTableInfo &table);
};

//! org.apache.hive.hcatalog.data.JsonSerDe (and Hive 3's org.apache.hadoop.hive.serde2.JsonSerDe)
class HCatalogJSONSerDe : public HiveJSONSerDe {
public:
	HCatalogJSONSerDe();

protected:
	HiveJSONDialect ReadDialect(const GlueTableInfo &table) const override;
};

//! org.openx.data.jsonserde.JsonSerDe
class OpenXJSONSerDe : public HiveJSONSerDe {
public:
	OpenXJSONSerDe();

protected:
	HiveJSONDialect ReadDialect(const GlueTableInfo &table) const override;
};

} // namespace duckdb
