#include "catch.hpp"
#include "core/serde/hive_serde.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;

namespace {

GlueTableInfo Table(HiveSerDe serde, HiveSerDeParameters serde_parameters,
                    unordered_map<string, string> parameters = {}) {
	GlueTableInfo table;
	table.schema_name = "db";
	table.name = "t";
	table.serde_library = HiveSerDeLibrary(serde);
	table.serde_parameters = std::move(serde_parameters);
	table.parameters = std::move(parameters);
	GlueColumn column;
	column.name = "id";
	column.type = "int";
	table.columns.push_back(column);
	return table;
}

HiveSerDe Find(const string &library) {
	HiveSerDe serde;
	REQUIRE(TryGetHiveSerDe(library, serde));
	return serde;
}

//! The message Parse throws, empty when it does not throw
string ParseError(const GlueTableInfo &table) {
	try {
		if (table.GetFileFormat() == HiveFileFormat::CSV) {
			table.GetCSVDialect();
		} else {
			table.GetJSONDialect();
		}
	} catch (const Exception &e) {
		return ErrorData(e).RawMessage();
	}
	return string();
}

HiveCSVDialect NewTable(HiveSerDe type, const string &delimiter, const string &quote = "", const string &escape = "") {
	auto dialect = NewTableCSVDialect(type);
	dialect.delimiter = delimiter;
	if (!quote.empty()) {
		dialect.quote = quote;
		dialect.escape = escape;
	}
	return dialect;
}

} // namespace

TEST_CASE("A SerDe is found by its SerializationLibrary", "[hive_serde]") {
	REQUIRE(Find("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe") == HiveSerDe::PARQUET);
	REQUIRE(Find("parquet.hive.serde.ParquetHiveSerDe") == HiveSerDe::PARQUET);
	REQUIRE(Find("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") == HiveSerDe::LAZY_SIMPLE);
	REQUIRE(Find("org.apache.hadoop.hive.serde2.OpenCSVSerde") == HiveSerDe::OPEN_CSV);
	REQUIRE(Find("org.openx.data.jsonserde.JsonSerDe") == HiveSerDe::OPENX_JSON);
	REQUIRE(Find("org.apache.hive.hcatalog.data.JsonSerDe") == HiveSerDe::HIVE_JSON);
	REQUIRE(Find("org.apache.hadoop.hive.serde2.JsonSerDe") == HiveSerDe::HIVE_JSON);
	REQUIRE(Find("org.apache.hadoop.hive.serde2.avro.AvroSerDe") == HiveSerDe::AVRO);
	HiveSerDe orc;
	REQUIRE(!TryGetHiveSerDe("org.apache.hadoop.hive.ql.io.orc.OrcSerde", orc));
	for (auto serde : {HiveSerDe::PARQUET, HiveSerDe::LAZY_SIMPLE, HiveSerDe::OPEN_CSV, HiveSerDe::HIVE_JSON,
	                   HiveSerDe::OPENX_JSON, HiveSerDe::AVRO}) {
		REQUIRE(Find(HiveSerDeLibrary(serde)) == serde);
	}
}

TEST_CASE("Parse reads back the dialect of every table CREATE TABLE creates", "[hive_serde]") {
	vector<HiveCSVDialect> dialects;
	for (auto &delimiter : {",", "|", "\t", ";", "\x01"}) {
		dialects.push_back(NewTable(HiveSerDe::LAZY_SIMPLE, delimiter));
		dialects.push_back(NewTable(HiveSerDe::OPEN_CSV, delimiter, "\"", "\""));
		dialects.push_back(NewTable(HiveSerDe::OPEN_CSV, delimiter, "'", "\\"));
	}
	for (auto &dialect : dialects) {
		auto serde = dialect.quote.empty() ? HiveSerDe::LAZY_SIMPLE : HiveSerDe::OPEN_CSV;
		INFO(HiveSerDeName(serde) << " delimiter " << int(dialect.delimiter[0]));
		// parsed from the defaults of a table that sets nothing, not from the new table's dialect
		REQUIRE(Table(serde, FormatCSVDialect(serde, dialect)).GetCSVDialect() == dialect);
	}
}

TEST_CASE("A new table's dialect differs from the SerDe's defaults only in what CREATE TABLE writes", "[hive_serde]") {
	auto lazy_simple = HiveSerDe::LAZY_SIMPLE;
	auto expected = DefaultCSVDialect(lazy_simple);
	REQUIRE(expected.delimiter == "\x01");
	expected.delimiter = ",";
	REQUIRE(NewTableCSVDialect(lazy_simple) == expected);

	auto open_csv = HiveSerDe::OPEN_CSV;
	expected = DefaultCSVDialect(open_csv);
	REQUIRE(expected.field_escape == "\\");
	expected.field_escape = "";
	REQUIRE(NewTableCSVDialect(open_csv) == expected);
}

TEST_CASE("The dialects of tables other engines create", "[hive_serde]") {
	auto lazy_simple = HiveSerDe::LAZY_SIMPLE;
	auto open_csv = HiveSerDe::OPEN_CSV;

	// ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' NULL DEFINED AS ''
	auto dialect =
	    Table(lazy_simple, {{"field.delim", "|"}, {"serialization.format", "|"}, {"serialization.null.format", ""}},
	          {{"skip.header.line.count", "1"}})
	        .GetCSVDialect();
	auto expected = DefaultCSVDialect(lazy_simple);
	expected.delimiter = "|";
	expected.null_string = "";
	expected.skip_lines = 1;
	REQUIRE(dialect == expected);

	// a crawled table: only serialization.format, Hive's default '\001' delimiter
	expected = DefaultCSVDialect(lazy_simple);
	REQUIRE(Table(lazy_simple, {{"serialization.format", "1"}}).GetCSVDialect() == expected);

	// ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ('separatorChar' = '\t')
	dialect = Table(open_csv, {{"separatorChar", "\t"}}).GetCSVDialect();
	expected = DefaultCSVDialect(open_csv);
	expected.delimiter = "\t";
	REQUIRE(dialect == expected);
	REQUIRE(dialect.field_escape == "\\");

	// ... ('separatorChar' = ',', 'quoteChar' = '\'', 'escapeChar' = '\\')
	dialect = Table(open_csv, {{"separatorChar", ","}, {"quoteChar", "'"}, {"escapeChar", "\\"}}).GetCSVDialect();
	expected = DefaultCSVDialect(open_csv);
	expected.quote = "'";
	expected.escape = "\\";
	expected.field_escape = "";
	REQUIRE(dialect == expected);
}

TEST_CASE("Check refuses only what DuckDB can not read or write", "[hive_serde]") {
	auto open_csv = HiveSerDe::OPEN_CSV;
	// an invalid property fails Parse, but is not a table DuckDB refuses
	auto invalid = Table(open_csv, {{"separatorChar", "\xFF"}});
	REQUIRE_NOTHROW(CheckHiveSerDe(open_csv, invalid));
	REQUIRE_THROWS_AS(invalid.GetCSVDialect(), InvalidInputException);
	REQUIRE_THROWS_AS(CheckHiveSerDe(open_csv, Table(open_csv, {}, {{"skip.footer.line.count", "1"}})),
	                  NotImplementedException);

	auto parquet = HiveSerDe::PARQUET;
	REQUIRE_NOTHROW(CheckHiveSerDe(parquet, Table(parquet, {}, {{"skip.footer.line.count", "1"}})));
	auto openx = HiveSerDe::OPENX_JSON;
	REQUIRE_THROWS_AS(CheckHiveSerDe(openx, Table(openx, {}, {{"skip.header.line.count", "1"}})),
	                  NotImplementedException);
	REQUIRE_NOTHROW(CheckHiveSerDe(openx, Table(openx, {{"ignore.malformed.json", "true"}})));
}

TEST_CASE("Format writes the SerDe parameters of a new table", "[hive_serde]") {
	REQUIRE(HiveSerDeNewTableParameters(HiveSerDe::LAZY_SIMPLE) ==
	        HiveSerDeParameters {{"field.delim", ","}, {"serialization.format", ","}});
	REQUIRE(HiveSerDeNewTableParameters(HiveSerDe::OPEN_CSV) ==
	        HiveSerDeParameters {{"separatorChar", ","}, {"quoteChar", "\""}, {"escapeChar", "\""}});
	REQUIRE(HiveSerDeNewTableParameters(HiveSerDeForFileFormat(HiveFileFormat::PARQUET)) ==
	        HiveSerDeParameters {{"serialization.format", "1"}});
	REQUIRE(HiveSerDeNewTableParameters(HiveSerDeForFileFormat(HiveFileFormat::JSON)).empty());
	REQUIRE(HiveSerDeNewTableParameters(HiveSerDeForFileFormat(HiveFileFormat::AVRO)).empty());
}

TEST_CASE("Only the SerDe parameters CREATE TABLE writes are created parameters", "[hive_serde]") {
	for (auto &key :
	     {"field.delim", "serialization.format", "separatorChar", "quoteChar", "escapeChar", "FIELD.DELIM"}) {
		REQUIRE(IsCreatedHiveSerDeParameter(key));
	}
	for (auto &key : {"serialization.null.format", "escape.delim", "ignore.malformed.json", "case.insensitive"}) {
		REQUIRE(!IsCreatedHiveSerDeParameter(key));
	}
}

TEST_CASE("LazySimpleSerDe properties", "[hive_serde]") {
	auto serde = HiveSerDe::LAZY_SIMPLE;
	auto dialect = Table(serde, {}).GetCSVDialect();
	REQUIRE(dialect.delimiter == "\x01");
	REQUIRE(dialect.quote.empty());
	REQUIRE(dialect.escape.empty());
	REQUIRE(dialect.has_null_string);
	REQUIRE(dialect.null_string == "\\N");
	REQUIRE(dialect.serde_fields);
	REQUIRE(dialect.null_for_invalid_data);
	REQUIRE(!dialect.false_unless_true);
	REQUIRE(!dialect.epoch_fields);
	// a byte code, as Hive's LazyUtils.getByte reads it
	REQUIRE(Table(serde, {{"serialization.format", "1"}}).GetCSVDialect().delimiter == "\x01");
	REQUIRE(Table(serde, {{"field.delim", "9"}}).GetCSVDialect().delimiter == "\t");
	REQUIRE(Table(serde, {{"field.delim", "|"}, {"serialization.format", ","}}).GetCSVDialect().delimiter == "|");
	// table parameters override SerDe parameters
	REQUIRE(Table(serde, {{"field.delim", "|"}}, {{"field.delim", ";"}}).GetCSVDialect().delimiter == ";");
	REQUIRE(Table(serde, {{"serialization.null.format", ""}}).GetCSVDialect().null_string.empty());
	REQUIRE(Table(serde, {}, {{"skip.header.line.count", "2"}}).GetCSVDialect().skip_lines == 2);

	REQUIRE(StringUtil::Contains(ParseError(Table(serde, {{"escape.delim", "\\"}})), "has 'escape.delim' set"));
	REQUIRE(StringUtil::Contains(ParseError(Table(serde, {{"serialization.encoding", "latin1"}})),
	                             "'serialization.encoding' 'latin1'"));
	REQUIRE(ParseError(Table(serde, {{"serialization.encoding", "utf8"}})).empty());
	REQUIRE(StringUtil::Contains(ParseError(Table(serde, {{"serialization.last.column.takes.rest", "TRUE"}})),
	                             "serialization.last.column.takes.rest"));
	REQUIRE(StringUtil::Contains(ParseError(Table(serde, {{"field.delim", ","}, {"serialization.null.format", "a,b"}})),
	                             "has the field delimiter ',' in its NULL string 'a,b'"));
	REQUIRE(StringUtil::Contains(ParseError(Table(serde, {{"field.delim", "10"}})), "a byte DuckDB can not split"));
	// ReadDialect does not refuse what DuckDB can not read or write
	REQUIRE(ReadCSVDialect(serde, Table(serde, {{"field.delim", "|"}, {"escape.delim", "\\"}})).delimiter == "|");
}

TEST_CASE("OpenCSVSerde properties", "[hive_serde]") {
	auto serde = HiveSerDe::OPEN_CSV;
	auto dialect = Table(serde, {}).GetCSVDialect();
	REQUIRE(dialect.delimiter == ",");
	REQUIRE(dialect.quote == "\"");
	REQUIRE(dialect.escape == "\"");
	REQUIRE(dialect.field_escape == "\\");
	REQUIRE(!dialect.has_null_string);
	REQUIRE(dialect.ReadNullString() == "\n");
	REQUIRE(dialect.WriteNullString().empty());
	REQUIRE(!dialect.null_for_invalid_data);
	REQUIRE(dialect.false_unless_true);
	REQUIRE(dialect.epoch_fields);
	REQUIRE(Table(serde, {{"escapeChar", "\""}}).GetCSVDialect().field_escape.empty());
	REQUIRE(Table(serde, {{"separatorChar", "\\"}}).GetCSVDialect().field_escape.empty());
	REQUIRE(Table(serde, {{"quoteChar", "'"}}).GetCSVDialect().escape == "'");
	REQUIRE(Table(serde, {{"use.null.for.invalid.data", "true"}}).GetCSVDialect().null_for_invalid_data);

	REQUIRE(StringUtil::Contains(ParseError(Table(serde, {{"quoteChar", "\xC3\xA9"}})), "more than one byte"));
	REQUIRE(StringUtil::Contains(ParseError(Table(serde, {}, {{"skip.header.line.count", "x"}})),
	                             "has an invalid 'skip.header.line.count' of 'x'"));
	REQUIRE(StringUtil::Contains(ParseError(Table(serde, {}, {{"skip.footer.line.count", "1"}})),
	                             "has 'skip.footer.line.count' set, which DuckDB can not read or write"));
	auto nested = Table(serde, {});
	nested.columns[0].type = "array<string>";
	REQUIRE(StringUtil::Contains(ParseError(nested), "has the nested column 'id' (array<string>)"));
}

TEST_CASE("JSON SerDe properties", "[hive_serde]") {
	auto openx = HiveSerDe::OPENX_JSON;
	auto hcatalog = HiveSerDe::HIVE_JSON;
	auto dialect = Table(openx, {}).GetJSONDialect();
	REQUIRE(!dialect.ignore_errors);
	REQUIRE(dialect.case_insensitive_keys);
	REQUIRE(Table(openx, {{"ignore.malformed.json", "TRUE"}}).GetJSONDialect().ignore_errors);
	REQUIRE(Table(openx, {{"use.null.for.invalid.data", "true"}}).GetJSONDialect().ignore_errors);
	REQUIRE(!Table(openx, {{"case.insensitive", "False"}}).GetJSONDialect().case_insensitive_keys);
	REQUIRE(Table(openx, {{"mapping.b", "x"}, {"dots.in.keys", "true"}}, {{"mapping.a", "y"}})
	            .GetJSONDialect()
	            .unsupported_properties == vector<string> {"dots.in.keys", "mapping.a", "mapping.b"});
	REQUIRE(Table(openx, {{"dots.in.keys", "false"}}).GetJSONDialect().unsupported_properties.empty());
	// the Hive JsonSerDe has none of OpenX's properties
	REQUIRE(!Table(hcatalog, {{"ignore.malformed.json", "true"}}).GetJSONDialect().ignore_errors);
	REQUIRE(Table(hcatalog, {{"timestamp.formats", "yyyy"}}).GetJSONDialect().unsupported_properties ==
	        vector<string> {"timestamp.formats"});
	REQUIRE(StringUtil::Contains(ParseError(Table(hcatalog, {}, {{"skip.header.line.count", "1"}})),
	                             "has 'skip.header.line.count' set on JSON files"));
}
