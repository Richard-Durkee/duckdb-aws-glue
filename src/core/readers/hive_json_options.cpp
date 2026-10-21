#include "core/readers/hive_json_options.hpp"

#include "core/glue_info.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

enum class JsonSerDePropertyEffect : uint8_t { IGNORE_ERRORS_IF_TRUE, NOT_APPLIED_IF_TRUE, NOT_APPLIED_IF_SET };

struct JsonSerDeProperty {
	HiveSerDe serde;
	const char *name;
	//! the name is a prefix (mapping.<column>)
	bool is_prefix;
	JsonSerDePropertyEffect effect;
};

//! The SerDe properties that change how the files of a JSON table are read, by their names in the SerDe's docs
static const JsonSerDeProperty JSON_SERDE_PROPERTIES[] = {
    {HiveSerDe::OPENX_JSON, "ignore.malformed.json", false, JsonSerDePropertyEffect::IGNORE_ERRORS_IF_TRUE},
    {HiveSerDe::OPENX_JSON, "use.null.for.invalid.data", false, JsonSerDePropertyEffect::IGNORE_ERRORS_IF_TRUE},
    {HiveSerDe::OPENX_JSON, "case.insensitive", false, JsonSerDePropertyEffect::NOT_APPLIED_IF_TRUE},
    {HiveSerDe::OPENX_JSON, "mapping.", true, JsonSerDePropertyEffect::NOT_APPLIED_IF_SET},
    {HiveSerDe::OPENX_JSON, "dots.in.keys", false, JsonSerDePropertyEffect::NOT_APPLIED_IF_TRUE},
    {HiveSerDe::HIVE_JSON, "timestamp.formats", false, JsonSerDePropertyEffect::NOT_APPLIED_IF_SET},
};

//! The keys of the table that are the property, or start with it if it is a prefix
static set<string> PropertyKeys(const GlueTableInfo &table, const JsonSerDeProperty &property) {
	set<string> keys;
	if (!property.is_prefix) {
		keys.insert(property.name);
		return keys;
	}
	for (auto properties : {&table.parameters, &table.serde_parameters}) {
		for (auto &entry : *properties) {
			if (StringUtil::StartsWith(entry.first, property.name)) {
				keys.insert(entry.first);
			}
		}
	}
	return keys;
}

HiveJSONOptions HiveJSONOptions::FromTable(const GlueTableInfo &table) {
	HiveJSONOptions result;
	const auto serde = table.GetSerDe();
	for (auto &property : JSON_SERDE_PROPERTIES) {
		if (property.serde != serde) {
			continue;
		}
		for (auto &key : PropertyKeys(table, property)) {
			string value;
			if (!table.TryGetProperty(key, value)) {
				continue;
			}
			const auto is_true = StringUtil::CIEquals(value, "true");
			switch (property.effect) {
			case JsonSerDePropertyEffect::IGNORE_ERRORS_IF_TRUE:
				result.ignore_errors = result.ignore_errors || is_true;
				break;
			case JsonSerDePropertyEffect::NOT_APPLIED_IF_TRUE:
				if (is_true) {
					result.unsupported_properties.push_back(key);
				}
				break;
			case JsonSerDePropertyEffect::NOT_APPLIED_IF_SET:
				if (!value.empty()) {
					result.unsupported_properties.push_back(key);
				}
				break;
			}
		}
	}
	std::sort(result.unsupported_properties.begin(), result.unsupported_properties.end());
	return result;
}

void HiveJSONOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "ignore_errors", ignore_errors);
}

HiveJSONOptions HiveJSONOptions::Deserialize(Deserializer &deserializer) {
	HiveJSONOptions result;
	result.ignore_errors = deserializer.ReadProperty<bool>(100, "ignore_errors");
	return result;
}

} // namespace duckdb
