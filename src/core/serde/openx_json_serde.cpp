#include "core/serde/hive_serde.hpp"

#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

OpenXJSONSerDe::OpenXJSONSerDe()
    : HiveJSONSerDe(HiveSerDeType::OPENX_JSON, HiveFileFormat::JSON, "JsonSerDe", "org.openx.data.jsonserde.JsonSerDe",
                    "openx") {
}

HiveJSONDialect OpenXJSONSerDe::ReadDialect(const GlueTableInfo &table) const {
	HiveJSONDialect dialect;
	dialect.ignore_errors =
	    IsTrueProperty(table, "ignore.malformed.json") || IsTrueProperty(table, "use.null.for.invalid.data");
	string case_insensitive;
	if (table.TryGetProperty("case.insensitive", case_insensitive) && StringUtil::CIEquals(case_insensitive, "false")) {
		dialect.case_insensitive_keys = false;
	}
	set<string> mapping_keys;
	for (auto properties : {&table.parameters, &table.serde_parameters}) {
		for (auto &entry : *properties) {
			if (StringUtil::StartsWith(entry.first, "mapping.")) {
				mapping_keys.insert(entry.first);
			}
		}
	}
	for (auto &key : mapping_keys) {
		NoteUnsupported(table, key, false, dialect);
	}
	NoteUnsupported(table, "dots.in.keys", true, dialect);
	return dialect;
}

} // namespace duckdb
