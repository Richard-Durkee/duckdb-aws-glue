#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class Serializer;
class Deserializer;
struct LogicalType;

//! The longest line a scan of a csv table reads by default (hive_csv_max_line_size): read_csv's buffers hold 32 MB
//! and each thread reads a quarter of one, so a line up to this size changes neither
static constexpr idx_t HIVE_CSV_DEFAULT_MAX_LINE_SIZE = 8000000;

//! How a field read as text becomes a value of its column's type
enum class HiveFieldConversion : uint8_t {
	//! the text itself, for a string column
	NONE,
	//! a field that does not parse fails the query
	CAST,
	//! a field that does not parse is NULL
	TRY_CAST,
	//! lower(text) = 'true': true for 'true' in any case and false otherwise, never an error
	FALSE_UNLESS_TRUE,
	//! the text as milliseconds (timestamp) or days (date) since the epoch; a field that is not an integer fails the
	//! query
	EPOCH,
	//! the text as milliseconds (timestamp) or days (date) since the epoch; NULL when not an integer or out of range
	TRY_EPOCH
};

//! How the files of a csv table are written and read, as its SerDe (HiveCSVSerDe) describes them
struct HiveCSVDialect {
	string delimiter = ",";
	//! Empty when fields are not quoted
	string quote = "\"";
	string escape = "\"";
	//! The header lines every file starts with
	idx_t skip_lines = 0;
	//! Without, no field is NULL and NULL is written as an empty field
	bool has_null_string = true;
	string null_string;
	//! Read fields as text and TRY_CAST them; short rows padded with NULL, extra fields ignored
	bool serde_fields = false;
	//! With serde_fields: a field that does not parse as its column type is NULL (TRY_CAST), else the query fails
	bool null_for_invalid_data = true;
	//! With serde_fields: a boolean field is true when it is 'true' in any case and false otherwise, never an error
	bool false_unless_true = false;
	//! With serde_fields: an escape character read_csv does not apply, dropped from a field's text; the escape or quote
	//! character after it is kept
	string field_escape;
	//! With serde_fields: a timestamp field is a number of milliseconds, a date field a number of days since the epoch
	bool epoch_fields = false;

public:
	//! With serde_fields: how a field becomes a value of a column of 'type', in the scan and in its reader call
	HiveFieldConversion FieldConversion(const LogicalType &type) const;
	//! The regexp_replace pattern that drops field_escape from a field's text, with '\1' the character it escapes
	string FieldEscapePattern() const;
	//! The nullstr of read_csv: without a NULL string "\n", which matches no unquoted field
	string ReadNullString() const;
	//! The nullstr of COPY
	string WriteNullString() const;
	bool operator==(const HiveCSVDialect &other) const;
	bool operator!=(const HiveCSVDialect &other) const;
	void Serialize(Serializer &serializer) const;
	static HiveCSVDialect Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
