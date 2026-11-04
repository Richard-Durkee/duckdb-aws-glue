#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ClientContext;

//! A Glue varchar(n) or char(n) type: a string type with a declared length
struct GlueCharacterType {
	//! char(n) rather than varchar(n)
	bool fixed_length = false;
	idx_t length = 0;

	//! The Glue type string, e.g. 'varchar(10)'
	string ToString() const;
};

//! Conversion between Glue (Hive style) type strings and DuckDB logical types
struct GlueTypes {
	//! Parse a Glue column type string, e.g. 'int', 'decimal(10,2)', 'array<string>', 'struct<a:int,b:string>'
	static LogicalType ToLogicalType(const string &glue_type);
	//! Produce a Glue column type string from a DuckDB logical type
	static string FromLogicalType(const LogicalType &type);
	//! Whether 'glue_type' is a varchar(n) or char(n) type, and if so its declared length
	static bool TryGetCharacterType(const string &glue_type, GlueCharacterType &result);
	//! The DuckDB type of a varchar(n) / char(n) column of a table being created: VARCHAR, named after the Glue type
	//! so the length reaches the table definition. Throws when 'length' is outside what the Glue type allows.
	static LogicalType CharacterLogicalType(bool fixed_length, int64_t length);

	//! HivePartitioning::GetValue without the unescaping: Glue stores the raw value, so 'a%20b' stays 'a%20b', not 'a
	//! b'. Throws if the value can't be cast to the key's type.
	static Value PartitionValue(ClientContext &context, const string &key, const string &str_value,
	                            const LogicalType &type);
	//! Whether PartitionValue reads the value Glue stores as NULL: Hive's default partition name, and for a non-string
	//! key the spellings a NULL arrives as
	static bool PartitionValueIsNull(const string &str_value, const LogicalType &type);
};

} // namespace duckdb
