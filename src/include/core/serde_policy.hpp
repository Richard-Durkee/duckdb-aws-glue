#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct GlueTableInfo;

//! The file format of a Hive table's data files, decided by its SerDe
enum class HiveFileFormat : uint8_t { PARQUET, CSV, JSON, AVRO };

//! The SerDe of a Hive table, decided by its SerializationLibrary; SerDes of one file format can differ in how they
//! read the data files and which engines write them
enum class HiveSerDe : uint8_t { PARQUET, LAZY_SIMPLE, OPEN_CSV, HIVE_JSON, OPENX_JSON, AVRO };

//! Whether a table's files are read or written
enum class SerDeDirection : uint8_t { READ, WRITE };

//! A reader / writer option a SerDe property sets
enum class SerDeOption : uint8_t {
	DELIMITER,
	QUOTE,
	ESCAPE,
	NULL_STRING,
	HEADER_LINES,
	NULL_FOR_INVALID_DATA,
	FALSE_UNLESS_TRUE,
	IGNORE_ERRORS,
	NONE
};
static constexpr idx_t SERDE_OPTION_COUNT = static_cast<idx_t>(SerDeOption::NONE);

//! What a property does: sets an option, is harmless, is not applied (logged), or makes the table unreadable
enum class SerDeAction : uint8_t { APPLY, IGNORE, WARN, REFUSE };

//! How a property's value becomes the option's value
enum class SerDeTransform : uint8_t { TEXT, BYTE_CODE, CHARACTER, COUNT, BOOLEAN };

//! When a WARN or REFUSE property takes effect (an APPLY property refuses when it holds, if not ALWAYS)
enum class SerDeCondition : uint8_t { ALWAYS, SET, NON_EMPTY, IS_TRUE, POSITIVE, ABOVE_ONE, NOT_UTF8 };

//! How the values of several properties of one option combine: the first one set, or true if any is true
enum class SerDeCombine : uint8_t { FIRST, ANY };

//! Checks of a SerDe that span several properties or the columns
enum SerDeCheck : uint8_t {
	SERDE_CHECK_NONE = 0,
	SERDE_CHECK_NO_NESTED_COLUMNS = 1,
	SERDE_CHECK_DELIMITER_NOT_IN_NULL_STRING = 2,
	SERDE_CHECK_SINGLE_BYTE_QUOTE_ESCAPE = 4,
	SERDE_CHECK_SINGLE_BYTE_DELIMITER = 8
};

struct SerDeDefinition {
	HiveSerDe serde;
	//! as written in messages
	const char *name;
	//! a SerializationLibrary holding this (lower case) is the SerDe; matched in table order
	const char *match;
	HiveFileFormat format;
	//! the SerializationLibrary, InputFormat and OutputFormat a created table gets
	const char *library;
	const char *input_format;
	const char *output_format;
	//! CREATE TABLE ... WITH (format = ...) creates tables of the format with this SerDe
	bool creates_format;
	bool writable;
	const char *write_refusal;
	//! fields are quoted
	bool quoted;
	uint8_t checks;
	uint8_t write_checks;
};

//! A SerDe this extension knows it can not read
struct UnsupportedSerDe {
	const char *name;
	const char *match;
	const char *reason;
};

//! The value of an option without any property, or that of another option
struct SerDeOptionDefault {
	HiveSerDe serde;
	SerDeDirection direction;
	SerDeOption option;
	const char *value;
	SerDeOption from_option;
};

//! How one SerDe property is handled when reading or writing a table of one SerDe
struct SerDePropertyRule {
	HiveSerDe serde;
	SerDeDirection direction;
	const char *key;
	//! the key is a prefix (mapping.<column>)
	bool is_prefix;
	SerDeAction action;
	SerDeOption option;
	SerDeTransform transform;
	SerDeCondition condition;
	SerDeCombine combine;
	//! the error of a refusal; {table}, {key} and {value} are replaced. nullptr for the standard one
	const char *message;
};

//! A SerDe parameter CREATE TABLE writes, from a CREATE TABLE option (delimiter, quote, escape) or a constant
struct SerDeCreateParameter {
	HiveSerDe serde;
	const char *key;
	const char *create_option;
	const char *value;
};

const SerDeDefinition &GetSerDeDefinition(HiveSerDe serde);
//! The SerDe a CREATE TABLE of the format gets (csv: LazySimpleSerDe, see BuildTableInfo)
const SerDeDefinition &GetSerDeDefinitionForFormat(HiveFileFormat format);
//! The SerDe of a SerializationLibrary; nullptr for others, with 'unsupported' set when the table knows why
const SerDeDefinition *MatchSerDe(const string &serde_library, const UnsupportedSerDe *&unsupported);
//! The SerDe parameters CREATE TABLE writes for the SerDe
vector<SerDeCreateParameter> GetSerDeCreateParameters(HiveSerDe serde);
//! Whether CREATE TABLE writes the SerDe parameter (for any SerDe); keys compared ignoring case
bool IsCreatedSerDeParameter(const string &key);

//! The options a table's SerDe and SerDe properties give its reader or writer
struct SerDeProperties {
	HiveSerDe serde;
	SerDeDirection direction;
	string values[SERDE_OPTION_COUNT];
	//! properties that change how the files are read but are not applied (logged)
	vector<string> not_applied;

public:
	const string &Get(SerDeOption option) const {
		return values[static_cast<idx_t>(option)];
	}
	bool GetBool(SerDeOption option) const;
	idx_t GetCount(SerDeOption option) const;
};

//! Resolve the SerDe properties of a text table through the policy table; with 'check' throws for the properties
//! and SerDe checks that refuse the table
SerDeProperties ResolveSerDeProperties(const GlueTableInfo &table, HiveSerDe serde, SerDeDirection direction, bool check);

} // namespace duckdb
