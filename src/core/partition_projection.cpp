#include "core/partition_projection.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

enum class DatePrecision : uint8_t { YEAR, MONTH, DAY, TIME };

static string Trim(const string &input) {
	auto result = input;
	StringUtil::Trim(result);
	return result;
}

//! A Java DateTimeFormatter pattern as a strftime format, and the finest unit it writes
static string JavaPatternToStrftime(const string &table_name, const string &key, const string &pattern,
                                    DatePrecision &precision) {
	auto unsupported = [&](const string &what) {
		return NotImplementedException("Hive table '%s' projects partition column '%s' with the date format '%s', "
		                               "which uses %s",
		                               table_name, key, pattern, what);
	};
	auto finer = [&](DatePrecision unit) {
		if (unit > precision) {
			precision = unit;
		}
	};
	precision = DatePrecision::YEAR;
	string result;
	idx_t i = 0;
	while (i < pattern.size()) {
		auto c = pattern[i];
		if (c == '\'') {
			// quoted literal text; '' is a single quote
			if (i + 1 < pattern.size() && pattern[i + 1] == '\'') {
				result += '\'';
				i += 2;
				continue;
			}
			i++;
			while (i < pattern.size()) {
				if (pattern[i] == '\'') {
					if (i + 1 < pattern.size() && pattern[i + 1] == '\'') {
						result += '\'';
						i += 2;
						continue;
					}
					break;
				}
				result += pattern[i] == '%' ? "%%" : string(1, pattern[i]);
				i++;
			}
			if (i == pattern.size()) {
				throw InvalidInputException("Hive table '%s' has an unterminated quote in the date format '%s' of "
				                            "partition column '%s'",
				                            table_name, pattern, key);
			}
			i++;
			continue;
		}
		if (!StringUtil::CharacterIsAlpha(c)) {
			result += c == '%' ? "%%" : string(1, c);
			i++;
			continue;
		}
		idx_t count = 0;
		while (i < pattern.size() && pattern[i] == c) {
			count++;
			i++;
		}
		switch (c) {
		case 'y':
		case 'u':
			result += count == 2 ? "%y" : "%Y";
			break;
		case 'M':
		case 'L':
			finer(DatePrecision::MONTH);
			if (count > 4) {
				throw unsupported(StringUtil::Format("'%s'", string(count, c)));
			}
			result += count == 1 ? "%-m" : count == 2 ? "%m" : count == 3 ? "%b" : "%B";
			break;
		case 'd':
			finer(DatePrecision::DAY);
			if (count > 2) {
				throw unsupported(StringUtil::Format("'%s'", string(count, c)));
			}
			result += count == 1 ? "%-d" : "%d";
			break;
		case 'D':
			finer(DatePrecision::DAY);
			result += count == 1 ? "%-j" : "%j";
			break;
		case 'H':
		case 'h':
		case 'm':
		case 's':
			finer(DatePrecision::TIME);
			if (count > 2) {
				throw unsupported(StringUtil::Format("'%s'", string(count, c)));
			}
			result += count == 1 ? "%-" : "%";
			result += c == 'H' ? 'H' : c == 'h' ? 'I' : c == 'm' ? 'M' : 'S';
			break;
		case 'a':
			finer(DatePrecision::TIME);
			result += "%p";
			break;
		case 'S':
			finer(DatePrecision::TIME);
			if (count != 3 && count != 6) {
				throw unsupported(StringUtil::Format("'%s' (only SSS and SSSSSS are supported)", string(count, c)));
			}
			result += count == 3 ? "%g" : "%f";
			break;
		default:
			throw unsupported(StringUtil::Format("the pattern letter '%c'", c));
		}
	}
	return result;
}

static bool TryParseDateUnit(const string &input, int64_t amount, interval_t &result) {
	auto unit = StringUtil::Upper(input);
	if (unit.size() > 1 && unit.back() == 'S') {
		unit.pop_back();
	}
	result = {0, 0, 0};
	if (unit == "YEAR") {
		result.months = NumericCast<int32_t>(amount * 12);
	} else if (unit == "MONTH") {
		result.months = NumericCast<int32_t>(amount);
	} else if (unit == "WEEK") {
		result.days = NumericCast<int32_t>(amount * 7);
	} else if (unit == "DAY") {
		result.days = NumericCast<int32_t>(amount);
	} else if (unit == "HOUR") {
		result.micros = amount * Interval::MICROS_PER_HOUR;
	} else if (unit == "MINUTE") {
		result.micros = amount * Interval::MICROS_PER_MINUTE;
	} else if (unit == "SECOND") {
		result.micros = amount * Interval::MICROS_PER_SEC;
	} else if (unit == "MILLI") {
		result.micros = amount * Interval::MICROS_PER_MSEC;
	} else {
		return false;
	}
	return true;
}

static bool TryParsePositiveInteger(const string &input, int64_t &result) {
	auto text = Trim(input);
	if (text.empty() || text.size() > 18) {
		return false;
	}
	for (auto c : text) {
		if (!StringUtil::CharacterIsDigit(c)) {
			return false;
		}
	}
	result = std::stoll(text);
	return result > 0;
}

static bool TryParseInteger(const string &input, int64_t &result) {
	auto text = Trim(input);
	bool negative = !text.empty() && (text[0] == '-' || text[0] == '+');
	auto digits = negative ? text.substr(1) : text;
	if (digits.empty() || digits.size() > 18) {
		return false;
	}
	for (auto c : digits) {
		if (!StringUtil::CharacterIsDigit(c)) {
			return false;
		}
	}
	result = std::stoll(text);
	return true;
}

//! NOW, NOW+3DAYS, NOW - 1 YEAR, ...: the offset from the current time, false when 'input' is not relative
static bool TryParseRelativeDate(const string &input, bool &is_relative, int64_t &sign, interval_t &offset) {
	auto text = StringUtil::Upper(Trim(input));
	if (!StringUtil::StartsWith(text, "NOW")) {
		is_relative = false;
		return true;
	}
	is_relative = true;
	offset = {0, 0, 0};
	sign = 1;
	auto rest = Trim(text.substr(3));
	if (rest.empty()) {
		return true;
	}
	if (rest[0] != '+' && rest[0] != '-') {
		return false;
	}
	sign = rest[0] == '-' ? -1 : 1;
	rest = Trim(rest.substr(1));
	idx_t digits = 0;
	while (digits < rest.size() && StringUtil::CharacterIsDigit(rest[digits])) {
		digits++;
	}
	if (digits == 0 || digits > 9) {
		return false;
	}
	auto amount = std::stoll(rest.substr(0, digits));
	auto unit = Trim(rest.substr(digits));
	if (StringUtil::StartsWith(unit, "MILLI")) {
		// not a unit of relative dates
		return false;
	}
	return TryParseDateUnit(unit, amount, offset);
}

static interval_t Negate(interval_t interval) {
	return {-interval.months, -interval.days, -interval.micros};
}

bool PartitionProjection::IsEnabled(const GlueTableInfo &table) {
	return StringUtil::CIEquals(Trim(table.GetParameter("projection.enabled")), "true");
}

shared_ptr<const PartitionProjection> PartitionProjection::FromTable(const GlueTableInfo &table) {
	if (table.partition_keys.empty() || !IsEnabled(table)) {
		return nullptr;
	}
	auto result = make_shared_ptr<PartitionProjection>();
	result->table_name = table.database_name + "." + table.name;
	result->root_location = table.location;
	while (!result->root_location.empty() && result->root_location.back() == '/') {
		result->root_location.pop_back();
	}
	auto &table_name = result->table_name;
	auto property = [&](const string &key, const string &name) {
		return table.GetParameter("projection." + key + "." + name);
	};
	auto invalid = [&](const string &key, const string &name, const string &value, const string &expected) {
		return InvalidInputException("Hive table '%s' has an invalid 'projection.%s.%s' of '%s', expected %s",
		                             table_name, key, name, value, expected);
	};
	for (auto &column : table.partition_keys) {
		ProjectedPartitionKey key;
		key.name = column.name;
		auto type = StringUtil::Lower(Trim(property(column.name, "type")));
		if (type.empty()) {
			throw InvalidInputException("Hive table '%s' projects its partitions (projection.enabled), but has no "
			                            "'projection.%s.type' for its partition column '%s'",
			                            table_name, column.name, column.name);
		}
		if (type == "enum") {
			key.type = ProjectionType::ENUM;
			auto values = property(column.name, "values");
			if (values.empty()) {
				throw invalid(column.name, "values", values, "a comma-separated list of values");
			}
			key.values = StringUtil::Split(values, ',');
		} else if (type == "integer") {
			key.type = ProjectionType::INTEGER;
			auto range = property(column.name, "range");
			auto bounds = StringUtil::Split(range, ',');
			if (bounds.size() != 2 || !TryParseInteger(bounds[0], key.min) || !TryParseInteger(bounds[1], key.max)) {
				throw invalid(column.name, "range", range, "two comma-separated integers");
			}
			auto interval = property(column.name, "interval");
			if (!interval.empty() && !TryParsePositiveInteger(interval, key.step)) {
				throw invalid(column.name, "interval", interval, "a positive integer");
			}
			auto digits = property(column.name, "digits");
			int64_t digit_count = 0;
			if (!digits.empty() && !TryParsePositiveInteger(digits, digit_count)) {
				throw invalid(column.name, "digits", digits, "a positive integer");
			}
			key.digits = NumericCast<idx_t>(digit_count);
		} else if (type == "date") {
			key.type = ProjectionType::DATE;
			auto pattern = property(column.name, "format");
			if (pattern.empty()) {
				throw invalid(column.name, "format", pattern, "a date format");
			}
			DatePrecision precision;
			key.format = JavaPatternToStrftime(table_name, column.name, pattern, precision);
			auto range = property(column.name, "range");
			auto bounds = StringUtil::Split(range, ',');
			if (bounds.size() != 2) {
				throw invalid(column.name, "range", range, "two comma-separated dates");
			}
			key.range_start = bounds[0];
			key.range_end = bounds[1];
			auto interval = property(column.name, "interval");
			auto unit = Trim(property(column.name, "interval.unit"));
			int64_t step = 1;
			if (!interval.empty() && !TryParsePositiveInteger(interval, step)) {
				throw invalid(column.name, "interval", interval, "a positive integer");
			}
			if (unit.empty()) {
				if (precision == DatePrecision::DAY) {
					unit = "DAYS";
				} else if (precision == DatePrecision::MONTH) {
					unit = "MONTHS";
				} else {
					throw InvalidInputException("Hive table '%s' needs 'projection.%s.interval.unit' for the date "
					                            "format '%s' of partition column '%s'",
					                            table_name, column.name, pattern, column.name);
				}
			}
			if (!TryParseDateUnit(unit, step, key.interval)) {
				throw invalid(column.name, "interval.unit", unit,
				              "YEARS, MONTHS, WEEKS, DAYS, HOURS, MINUTES, SECONDS or MILLIS");
			}
			auto column_type = StringUtil::Lower(Trim(column.type));
			if (column_type == "date") {
				key.value_type = LogicalTypeId::DATE;
			} else if (StringUtil::StartsWith(column_type, "timestamp")) {
				key.value_type = LogicalTypeId::TIMESTAMP;
			}
		} else if (type == "injected") {
			key.type = ProjectionType::INJECTED;
		} else {
			throw invalid(column.name, "type", type, "enum, integer, date or injected");
		}
		result->keys.push_back(std::move(key));
	}
	result->location_template = Trim(table.GetParameter("storage.location.template"));
	if (!result->location_template.empty()) {
		for (auto &key : result->keys) {
			if (!StringUtil::Contains(result->location_template, "${" + key.name + "}")) {
				throw InvalidInputException("Hive table '%s' has a 'storage.location.template' without the "
				                            "placeholder ${%s} for its partition column '%s'",
				                            table_name, key.name, key.name);
			}
		}
		while (!result->location_template.empty() && result->location_template.back() == '/') {
			result->location_template.pop_back();
		}
	}
	return std::move(result);
}

static string ZeroPad(int64_t value, idx_t digits) {
	auto text = std::to_string(value < 0 ? -value : value);
	// as Java's %0<digits>d: the width includes the sign
	auto width = value < 0 && digits > 0 ? digits - 1 : digits;
	if (text.size() < width) {
		text = string(width - text.size(), '0') + text;
	}
	return value < 0 ? "-" + text : text;
}

vector<ProjectedValue> PartitionProjection::Values(ClientContext &context, idx_t key_index) const {
	auto &key = keys[key_index];
	auto too_many = [&]() {
		return InvalidInputException("Hive table '%s' projects more than %llu values for partition column '%s'",
		                             table_name, idx_t(MAX_PARTITIONS), key.name);
	};
	vector<ProjectedValue> result;
	switch (key.type) {
	case ProjectionType::ENUM:
		for (auto &value : key.values) {
			result.push_back({value, value});
		}
		break;
	case ProjectionType::INTEGER: {
		if (key.min > key.max) {
			break;
		}
		auto count = static_cast<uint64_t>(key.max - key.min) / static_cast<uint64_t>(key.step) + 1;
		if (count > MAX_PARTITIONS) {
			throw too_many();
		}
		for (idx_t i = 0; i < count; i++) {
			auto value = ZeroPad(key.min + static_cast<int64_t>(i) * key.step, key.digits);
			result.push_back({value, value});
		}
		break;
	}
	case ProjectionType::DATE: {
		auto now = MetaTransaction::Get(context).GetCurrentTransactionStartTimestamp();
		auto bound = [&](const string &text) {
			bool is_relative;
			int64_t sign;
			interval_t offset;
			if (!TryParseRelativeDate(text, is_relative, sign, offset)) {
				throw InvalidInputException("Hive table '%s' has an invalid relative date '%s' in "
				                            "'projection.%s.range'",
				                            table_name, text, key.name);
			}
			timestamp_t result;
			if (is_relative) {
				result = AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(now, sign < 0 ? Negate(offset)
				                                                                                    : offset);
			} else {
				StrpTimeFormat::ParseResult parsed;
				if (!StrpTimeFormat::TryParse(key.format, Trim(text), parsed) || !parsed.TryToTimestamp(result)) {
					throw InvalidInputException("Hive table '%s' has the date '%s' in 'projection.%s.range', which "
					                            "does not match its format",
					                            table_name, text, key.name);
				}
			}
			// truncated to what the format writes, as a value read back from a location
			StrpTimeFormat::ParseResult truncated;
			timestamp_t truncated_result;
			if (StrpTimeFormat::TryParse(key.format, StrfTimeFormat::Format(result, key.format), truncated) &&
			    truncated.TryToTimestamp(truncated_result)) {
				return truncated_result;
			}
			return result;
		};
		auto start = bound(key.range_start);
		auto end = bound(key.range_end);
		for (auto current = start; current <= end;) {
			auto path = StrfTimeFormat::Format(current, key.format);
			if (result.empty() || result.back().path != path) {
				if (result.size() == MAX_PARTITIONS) {
					throw too_many();
				}
				string value = path;
				if (key.value_type == LogicalTypeId::DATE) {
					value = Date::ToString(Timestamp::GetDate(current));
				} else if (key.value_type == LogicalTypeId::TIMESTAMP) {
					value = Timestamp::ToString(current);
				}
				result.push_back({std::move(value), std::move(path)});
			}
			auto next = AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(current, key.interval);
			if (next <= current) {
				break;
			}
			current = next;
		}
		break;
	}
	case ProjectionType::INJECTED:
		ThrowMissingInjectedFilter(key_index);
	}
	return result;
}

void PartitionProjection::ThrowMissingInjectedFilter(idx_t key_index) const {
	throw InvalidInputException("Hive table '%s' projects partition column '%s' with the type injected: a query of "
	                            "the table must filter '%s' with = or IN",
	                            table_name, keys[key_index].name, keys[key_index].name);
}

string PartitionProjection::Location(const vector<const ProjectedValue *> &values) const {
	if (location_template.empty()) {
		auto result = root_location;
		for (idx_t i = 0; i < keys.size(); i++) {
			result +=
			    "/" + HivePartitioning::Escape(keys[i].name) + "=" + HivePartitioning::EscapeValue(values[i]->path);
		}
		return result;
	}
	auto result = location_template;
	for (idx_t i = 0; i < keys.size(); i++) {
		result = StringUtil::Replace(result, "${" + keys[i].name + "}", values[i]->path);
	}
	return result;
}

vector<GluePartitionInfo> PartitionProjection::Combine(const vector<vector<ProjectedValue>> &values) const {
	D_ASSERT(values.size() == keys.size());
	uint64_t count = 1;
	for (auto &key_values : values) {
		count *= key_values.size();
		if (count > MAX_PARTITIONS) {
			throw InvalidInputException("Hive table '%s' projects more than %llu partitions for the query; filter its "
			                            "partition columns to read fewer",
			                            table_name, idx_t(MAX_PARTITIONS));
		}
	}
	vector<GluePartitionInfo> result;
	if (count == 0) {
		return result;
	}
	result.reserve(count);
	vector<idx_t> positions(values.size(), 0);
	vector<const ProjectedValue *> current(values.size());
	while (true) {
		GluePartitionInfo partition;
		for (idx_t i = 0; i < values.size(); i++) {
			current[i] = &values[i][positions[i]];
			partition.values.push_back(current[i]->value);
		}
		partition.location = Location(current);
		result.push_back(std::move(partition));
		// the last key varies fastest
		idx_t key = values.size();
		while (key > 0) {
			key--;
			if (++positions[key] < values[key].size()) {
				break;
			}
			positions[key] = 0;
			if (key == 0) {
				return result;
			}
		}
	}
}

} // namespace duckdb
