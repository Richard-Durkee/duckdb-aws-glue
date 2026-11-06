#include "core/serde/hive_serde.hpp"

#include "core/helpers.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

OpenCSVSerDe::OpenCSVSerDe()
    : HiveCSVSerDe(HiveSerDeType::OPEN_CSV, HiveFileFormat::CSV, "OpenCSVSerde",
                   "org.apache.hadoop.hive.serde2.OpenCSVSerde", "opencsvserde") {
}

string OpenCSVSerDe::Character(const GlueTableInfo &table, const string &key, const string &fallback) {
	string value;
	if (!table.TryGetProperty(key, value) || value.empty()) {
		return fallback;
	}
	string character;
	if (!TryFirstCharacter(value, character)) {
		throw InvalidInputException("Hive table '%s.%s' has an invalid '%s' of '%s', expected text", table.schema_name,
		                            table.name, key, value);
	}
	return character;
}

string OpenCSVSerDe::Quote(const GlueTableInfo &table) {
	return Character(table, "quoteChar", "\"");
}

string OpenCSVSerDe::Escape(const GlueTableInfo &table) {
	return Character(table, "escapeChar", Quote(table));
}

void OpenCSVSerDe::CheckProperties(const GlueTableInfo &table) const {
	if (Quote(table).size() > 1 || Escape(table).size() > 1) {
		Refuse(table, "has a quoteChar or escapeChar of more than one byte");
	}
}

HiveCSVDialect OpenCSVSerDe::DefaultDialect() const {
	HiveCSVDialect dialect;
	dialect.has_null_string = false;
	dialect.serde_fields = true;
	dialect.null_for_invalid_data = false;
	dialect.false_unless_true = true;
	dialect.epoch_fields = true;
	// without escapeChar OpenCSVSerde reads with a '\' escape, which read_csv, given the quote character, would keep
	dialect.field_escape = "\\";
	return dialect;
}

HiveCSVDialect OpenCSVSerDe::NewTableDialect() const {
	auto dialect = DefaultDialect();
	// CREATE TABLE writes escapeChar
	dialect.field_escape = "";
	return dialect;
}

void OpenCSVSerDe::ReadProperties(const GlueTableInfo &table, HiveCSVDialect &dialect) const {
	dialect.delimiter = Character(table, "separatorChar", dialect.delimiter);
	dialect.quote = Quote(table);
	dialect.escape = Escape(table);
	dialect.null_for_invalid_data = IsTrueProperty(table, "use.null.for.invalid.data");
	string escape_char;
	if ((table.TryGetProperty("escapeChar", escape_char) && !escape_char.empty()) || dialect.delimiter == "\\" ||
	    dialect.quote == "\\") {
		dialect.field_escape = "";
	}
}

HiveSerDeParameters OpenCSVSerDe::Format(const HiveCSVDialect &dialect) const {
	return {{"separatorChar", dialect.delimiter}, {"quoteChar", dialect.quote}, {"escapeChar", dialect.escape}};
}

} // namespace duckdb
