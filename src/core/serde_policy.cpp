#include "core/serde_policy.hpp"

#include "core/glue_info.hpp"
#include "core/helpers.hpp"
#include "core/serde_policy_table.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

const SerDeDefinition &GetSerDeDefinition(HiveSerDe serde) {
	for (auto &definition : SERDE_DEFINITIONS) {
		if (definition.serde == serde) {
			return definition;
		}
	}
	throw InternalException("Unknown HiveSerDe");
}

const SerDeDefinition &GetSerDeDefinitionForFormat(HiveFileFormat format) {
	for (auto &definition : SERDE_DEFINITIONS) {
		if (definition.format == format && definition.creates_format) {
			return definition;
		}
	}
	throw InternalException("No SerDe creates the file format");
}

const SerDeDefinition *MatchSerDe(const string &serde_library, const UnsupportedSerDe *&unsupported) {
	auto library = StringUtil::Lower(serde_library);
	unsupported = nullptr;
	for (auto &definition : SERDE_DEFINITIONS) {
		if (StringUtil::Contains(library, definition.match)) {
			return &definition;
		}
	}
	for (auto &serde : UNSUPPORTED_SERDES) {
		if (StringUtil::Contains(library, serde.match)) {
			unsupported = &serde;
			break;
		}
	}
	return nullptr;
}

vector<SerDeCreateParameter> GetSerDeCreateParameters(HiveSerDe serde) {
	vector<SerDeCreateParameter> result;
	for (auto &parameter : SERDE_CREATE_PARAMETERS) {
		if (parameter.serde == serde) {
			result.push_back(parameter);
		}
	}
	return result;
}

bool IsCreatedSerDeParameter(const string &key) {
	for (auto &parameter : SERDE_CREATE_PARAMETERS) {
		if (StringUtil::CIEquals(key, parameter.key)) {
			return true;
		}
	}
	return false;
}

bool SerDeProperties::GetBool(SerDeOption option) const {
	return Get(option) == "true";
}

idx_t SerDeProperties::GetCount(SerDeOption option) const {
	auto &value = Get(option);
	return value.empty() ? 0 : NumericCast<idx_t>(std::stoll(value));
}

static string TableName(const GlueTableInfo &table) {
	return table.schema_name + "." + table.name;
}

//! Hive's LazyUtils.getByte: a byte code from -128 to 127 ('1' is '\001'), else the first character
static string ByteCode(const GlueTableInfo &table, const string &value) {
	if (value.empty()) {
		return "\x01";
	}
	int64_t number;
	auto separator = TryParseInteger(value, -128, 127, number) ? static_cast<uint8_t>(number & 0xFF)
	                                                           : static_cast<uint8_t>(value[0]);
	if (separator == 0 || separator == '\n' || separator == '\r' || separator >= 0x80) {
		throw NotImplementedException("Hive table '%s.%s' has the field delimiter '%s', a byte DuckDB can not split "
		                              "fields on; only ASCII delimiters other than NUL and line breaks are supported",
		                              table.schema_name, table.name, value);
	}
	return string(1, static_cast<char>(separator));
}

static idx_t Count(const GlueTableInfo &table, const string &key, const string &value) {
	if (value.empty()) {
		return 0;
	}
	int64_t count;
	if (!TryParseInteger(value, 0, NumericLimits<int32_t>::Maximum(), count)) {
		throw InvalidInputException("Hive table '%s.%s' has an invalid '%s' of '%s', expected a non-negative number",
		                            table.schema_name, table.name, key, value);
	}
	return NumericCast<idx_t>(count);
}

//! The option value of a property's value; false when the value leaves the option at its default
static bool Transform(const GlueTableInfo &table, const SerDePropertyRule &rule, const string &key,
                      const string &value, string &result) {
	switch (rule.transform) {
	case SerDeTransform::TEXT:
		result = value;
		return true;
	case SerDeTransform::BYTE_CODE:
		result = ByteCode(table, value);
		return true;
	case SerDeTransform::CHARACTER:
		if (value.empty()) {
			return false;
		}
		if (!TryFirstCharacter(value, result)) {
			throw InvalidInputException("Hive table '%s.%s' has an invalid '%s' of '%s', expected text",
			                            table.schema_name, table.name, key, value);
		}
		return true;
	case SerDeTransform::COUNT:
		if (value.empty()) {
			return false;
		}
		result = to_string(Count(table, key, value));
		return true;
	case SerDeTransform::BOOLEAN:
		result = StringUtil::CIEquals(value, "true") ? "true" : "false";
		return true;
	}
	throw InternalException("Unknown SerDeTransform");
}

static bool Holds(const GlueTableInfo &table, const SerDePropertyRule &rule, const string &key, const string &value) {
	switch (rule.condition) {
	case SerDeCondition::ALWAYS:
	case SerDeCondition::SET:
		return true;
	case SerDeCondition::NON_EMPTY:
		return !value.empty();
	case SerDeCondition::IS_TRUE:
		return StringUtil::CIEquals(value, "true");
	case SerDeCondition::POSITIVE:
		return Count(table, key, value) > 0;
	case SerDeCondition::ABOVE_ONE:
		return Count(table, key, value) > 1;
	case SerDeCondition::NOT_UTF8:
		return !StringUtil::CIEquals(value, "UTF-8") && !StringUtil::CIEquals(value, "UTF8");
	}
	throw InternalException("Unknown SerDeCondition");
}

[[noreturn]] static void Refuse(const GlueTableInfo &table, const string &what) {
	throw NotImplementedException("Hive table '%s.%s' %s, which DuckDB can not read or write", table.schema_name,
	                              table.name, what);
}

[[noreturn]] static void Refuse(const GlueTableInfo &table, const SerDePropertyRule &rule, const string &key,
                                const string &value) {
	if (!rule.message) {
		Refuse(table, StringUtil::Format("has '%s' set", key));
	}
	auto shown = rule.transform == SerDeTransform::COUNT ? to_string(Count(table, key, value)) : value;
	auto message = StringUtil::Replace(rule.message, "{table}", TableName(table));
	message = StringUtil::Replace(message, "{key}", key);
	message = StringUtil::Replace(message, "{value}", shown);
	throw NotImplementedException(message);
}

//! The keys of the table that are the rule's property, or start with it if it is a prefix
static set<string> PropertyKeys(const GlueTableInfo &table, const SerDePropertyRule &rule) {
	set<string> keys;
	if (!rule.is_prefix) {
		keys.insert(rule.key);
		return keys;
	}
	for (auto properties : {&table.parameters, &table.serde_parameters}) {
		for (auto &entry : *properties) {
			if (StringUtil::StartsWith(entry.first, rule.key)) {
				keys.insert(entry.first);
			}
		}
	}
	return keys;
}

static void RunChecks(const GlueTableInfo &table, const SerDeProperties &result, uint8_t checks) {
	if (checks & SERDE_CHECK_NO_NESTED_COLUMNS) {
		for (auto &column : table.columns) {
			if (column.type.find('<') != string::npos) {
				Refuse(table, StringUtil::Format("has the nested column '%s' (%s), stored with collection delimiters",
				                                 column.name, column.type));
			}
		}
	}
	if (checks & SERDE_CHECK_SINGLE_BYTE_QUOTE_ESCAPE) {
		if (result.Get(SerDeOption::QUOTE).size() > 1 || result.Get(SerDeOption::ESCAPE).size() > 1) {
			Refuse(table, "has a quoteChar or escapeChar of more than one byte");
		}
	}
	if (checks & SERDE_CHECK_DELIMITER_NOT_IN_NULL_STRING) {
		auto &delimiter = result.Get(SerDeOption::DELIMITER);
		auto &null_string = result.Get(SerDeOption::NULL_STRING);
		if (StringUtil::Contains(null_string, delimiter)) {
			Refuse(table,
			       StringUtil::Format("has the field delimiter '%s' in its NULL string '%s'", delimiter, null_string));
		}
	}
	if (checks & SERDE_CHECK_SINGLE_BYTE_DELIMITER) {
		auto &delimiter = result.Get(SerDeOption::DELIMITER);
		if (delimiter.size() > 1) {
			throw NotImplementedException("Writing to Hive table '%s.%s' is not supported: DuckDB writes single-byte "
			                              "field delimiters only, the table's is '%s'",
			                              table.schema_name, table.name, delimiter);
		}
	}
}

SerDeProperties ResolveSerDeProperties(const GlueTableInfo &table, HiveSerDe serde, SerDeDirection direction,
                                       bool check) {
	SerDeProperties result;
	result.serde = serde;
	result.direction = direction;
	bool set_by_property[SERDE_OPTION_COUNT] = {};
	for (auto &option_default : SERDE_OPTION_DEFAULTS) {
		if (option_default.serde == result.serde && option_default.direction == direction &&
		    option_default.value) {
			result.values[static_cast<idx_t>(option_default.option)] = option_default.value;
		}
	}
	for (auto &rule : SERDE_PROPERTY_RULES) {
		if (rule.serde != result.serde || rule.direction != direction) {
			continue;
		}
		for (auto &key : PropertyKeys(table, rule)) {
			string value;
			if (!table.TryGetProperty(key, value)) {
				continue;
			}
			switch (rule.action) {
			case SerDeAction::IGNORE:
				break;
			case SerDeAction::WARN:
				if (Holds(table, rule, key, value)) {
					result.not_applied.push_back(key);
				}
				break;
			case SerDeAction::REFUSE:
				if (check && Holds(table, rule, key, value)) {
					Refuse(table, rule, key, value);
				}
				break;
			case SerDeAction::APPLY: {
				auto index = static_cast<idx_t>(rule.option);
				if (check && rule.condition != SerDeCondition::ALWAYS && Holds(table, rule, key, value)) {
					Refuse(table, rule, key, value);
				}
				string option_value;
				if (!Transform(table, rule, key, value, option_value)) {
					break;
				}
				if (rule.combine == SerDeCombine::ANY) {
					auto any = (set_by_property[index] && result.values[index] == "true") || option_value == "true";
					result.values[index] = any ? "true" : "false";
				} else if (!set_by_property[index]) {
					result.values[index] = option_value;
				}
				set_by_property[index] = true;
				break;
			}
			}
		}
	}
	for (auto &option_default : SERDE_OPTION_DEFAULTS) {
		auto index = static_cast<idx_t>(option_default.option);
		if (option_default.serde == result.serde && option_default.direction == direction &&
		    option_default.from_option != SerDeOption::NONE && !set_by_property[index]) {
			result.values[index] = result.Get(option_default.from_option);
		}
	}
	std::sort(result.not_applied.begin(), result.not_applied.end());
	if (check) {
		auto &definition = GetSerDeDefinition(result.serde);
		RunChecks(table, result, definition.checks);
		if (direction == SerDeDirection::WRITE) {
			RunChecks(table, result, definition.write_checks);
		}
	}
	return result;
}

} // namespace duckdb
