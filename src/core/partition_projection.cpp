#include "core/partition_projection.hpp"
#include "core/hive_timestamp_formats.hpp"
#include "core/glue_types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

enum class DatePrecision : uint8_t { YEAR, MONTH, DAY, TIME };

static string Trim(const string &input) {
	auto result = input;
	StringUtil::Trim(result);
	return result;
}

//! The finest unit a strftime format writes
static DatePrecision FormatPrecision(const string &format) {
	auto writes = [&](const string &codes) {
		for (idx_t i = 0; i + 1 < format.size(); i++) {
			if (format[i] != '%') {
				continue;
			}
			i++;
			if (format[i] == '-' && i + 1 < format.size()) {
				i++;
			}
			if (codes.find(format[i]) != string::npos) {
				return true;
			}
		}
		return false;
	};
	if (writes("HIMSgp")) {
		return DatePrecision::TIME;
	}
	if (writes("daA")) {
		return DatePrecision::DAY;
	}
	return writes("mbB") ? DatePrecision::MONTH : DatePrecision::YEAR;
}

static bool TryParseDateUnit(const string &input, int64_t amount, interval_t &result) {
	auto unit = StringUtil::Upper(input);
	if (unit.size() > 1 && unit.back() == 'S') {
		unit.pop_back();
	}
	result = {0, 0, 0};
	if (unit == "CENTURIE") {
		unit = "CENTURY";
	}
	// an amount the interval can not hold is no step
	auto months = [&](int64_t per_unit) {
		if (amount > NumericLimits<int32_t>::Maximum() / per_unit) {
			return false;
		}
		result.months = NumericCast<int32_t>(amount * per_unit);
		return true;
	};
	auto days = [&](int64_t per_unit) {
		if (amount > NumericLimits<int32_t>::Maximum() / per_unit) {
			return false;
		}
		result.days = NumericCast<int32_t>(amount * per_unit);
		return true;
	};
	auto micros = [&](int64_t per_unit) {
		if (amount > NumericLimits<int64_t>::Maximum() / per_unit) {
			return false;
		}
		result.micros = amount * per_unit;
		return true;
	};
	if (unit == "MILLENNIA" || unit == "MILLENNIUM") {
		return months(12000);
	} else if (unit == "CENTURY") {
		return months(1200);
	} else if (unit == "DECADE") {
		return months(120);
	} else if (unit == "YEAR") {
		return months(12);
	} else if (unit == "MONTH") {
		return months(1);
	} else if (unit == "WEEK") {
		return days(7);
	} else if (unit == "DAY") {
		return days(1);
	} else if (unit == "HALF_DAY") {
		return micros(12 * Interval::MICROS_PER_HOUR);
	} else if (unit == "HOUR") {
		return micros(Interval::MICROS_PER_HOUR);
	} else if (unit == "MINUTE") {
		return micros(Interval::MICROS_PER_MINUTE);
	} else if (unit == "SECOND") {
		return micros(Interval::MICROS_PER_SEC);
	} else if (unit == "MILLI") {
		return micros(Interval::MICROS_PER_MSEC);
	} else if (unit == "MICRO") {
		return micros(1);
	} else if (unit == "NANO") {
		// a timestamp has no finer step than a microsecond
		if (amount % 1000 != 0) {
			return false;
		}
		result.micros = amount / 1000;
	} else {
		return false;
	}
	return true;
}

//! An integer of optional sign and digits that fits int64_t
static bool TryParseInteger(const string &input, int64_t &result) {
	auto text = Trim(input);
	auto digits = !text.empty() && (text[0] == '-' || text[0] == '+') ? text.substr(1) : text;
	if (digits.empty()) {
		return false;
	}
	for (auto c : digits) {
		if (!StringUtil::CharacterIsDigit(c)) {
			return false;
		}
	}
	return TryCast::Operation<string_t, int64_t>(string_t(text), result);
}

static bool TryParsePositiveInteger(const string &input, int64_t &result) {
	auto text = Trim(input);
	return !text.empty() && text[0] != '-' && text[0] != '+' && TryParseInteger(text, result) && result > 0;
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

shared_ptr<const PartitionProjection> PartitionProjection::FromTable(const GlueTableInfo &table) {
	if (!table.IsPartitionProjected()) {
		return nullptr;
	}
	auto result = make_shared_ptr<PartitionProjection>();
	result->table_name = table.schema_name + "." + table.name;
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
			unordered_set<string> seen;
			for (auto &value : StringUtil::Split(values, string(","))) {
				if (!value.empty() && seen.insert(value).second) {
					key.values.push_back(value);
				}
			}
		} else if (type == "integer") {
			key.type = ProjectionType::INTEGER;
			auto range = property(column.name, "range");
			auto bounds = StringUtil::Split(range, ',');
			if (bounds.size() != 2 || !TryParseInteger(bounds[0], key.min) || !TryParseInteger(bounds[1], key.max)) {
				throw invalid(column.name, "range", range, "two comma-separated integers");
			}
			if (key.min > key.max) {
				throw invalid(column.name, "range", range, "a lower bound that is not after the upper bound");
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
			if (!JodaPatternToStrptime(pattern, key.format, true)) {
				throw NotImplementedException(
				    "Hive table '%s' projects partition column '%s' with the date format '%s', "
				    "which DuckDB can not write: the supported letters are y, M, d, H, h, m, "
				    "s, SSS and a, with quoted text, and a year is required",
				    table_name, column.name, pattern);
			}
			auto precision = FormatPrecision(key.format);
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
			interval_t unit_check;
			if (!unit.empty() && !TryParseDateUnit(unit, 1000, unit_check)) {
				throw invalid(column.name, "interval.unit", unit,
				              "a java.time ChronoUnit from MILLENNIA to NANOS (a multiple of 1000)");
			}
			// as Athena: the interval and its unit count only together, otherwise one of the format's finest field
			if (interval.empty() || unit.empty()) {
				step = 1;
				unit.clear();
			}
			if (unit.empty()) {
				if (precision == DatePrecision::DAY) {
					unit = "DAYS";
				} else if (precision == DatePrecision::MONTH) {
					unit = "MONTHS";
				} else if (precision == DatePrecision::YEAR) {
					unit = "YEARS";
				} else {
					throw InvalidInputException("Hive table '%s' needs both 'projection.%s.interval' and "
					                            "'projection.%s.interval.unit' for the date format '%s' of partition "
					                            "column '%s'",
					                            table_name, column.name, column.name, pattern, column.name);
				}
			}
			if (!TryParseDateUnit(unit, step, key.interval)) {
				throw invalid(column.name, "interval.unit", unit,
				              "a java.time ChronoUnit from MILLENNIA to NANOS (a multiple of 1000)");
			}
			auto column_type = StringUtil::Lower(Trim(column.type));
			if (column_type == "date") {
				key.value_type = LogicalTypeId::DATE;
			} else if (StringUtil::StartsWith(column_type, "timestamp")) {
				key.value_type = LogicalTypeId::TIMESTAMP;
			}
		} else if (type == "injected") {
			key.type = ProjectionType::INJECTED;
			if (StringUtil::StartsWith(StringUtil::Lower(Trim(column.type)), "timestamp")) {
				// as in Athena
				throw NotImplementedException("Hive table '%s' can not project the timestamp partition column '%s' "
				                              "with the type injected",
				                              table_name, column.name);
			}
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
		auto &location_template = result->location_template;
		for (auto start = location_template.find("${"); start != string::npos;
		     start = location_template.find("${", start + 2)) {
			auto end = location_template.find('}', start);
			auto name = location_template.substr(start + 2, end == string::npos ? string::npos : end - start - 2);
			bool is_key = false;
			for (auto &key : result->keys) {
				is_key = is_key || key.name == name;
			}
			if (!is_key) {
				throw InvalidInputException("Hive table '%s' has a 'storage.location.template' with the placeholder "
				                            "${%s}, which is not one of its partition columns",
				                            table_name, name);
			}
		}
		while (!result->location_template.empty() && result->location_template.back() == '/') {
			result->location_template.pop_back();
		}
	}
	return std::move(result);
}

static string ZeroPad(int64_t value, idx_t digits) {
	auto magnitude = value < 0 ? ~static_cast<uint64_t>(value) + 1 : static_cast<uint64_t>(value);
	auto text = std::to_string(magnitude);
	// as Java's %0<digits>d: the width includes the sign
	auto width = value < 0 && digits > 0 ? digits - 1 : digits;
	if (text.size() < width) {
		text = string(width - text.size(), '0') + text;
	}
	return value < 0 ? "-" + text : text;
}

idx_t PartitionProjection::IntegerCount(idx_t key_index) const {
	auto &key = keys[key_index];
	auto count = (static_cast<uint64_t>(key.max) - static_cast<uint64_t>(key.min)) / static_cast<uint64_t>(key.step);
	return count == NumericLimits<uint64_t>::Maximum() ? count : count + 1;
}

idx_t PartitionProjection::MaxPartitions(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting("glue_projection_max_partitions", setting) && !setting.IsNull()) {
		return setting.GetValue<idx_t>();
	}
	return DEFAULT_MAX_PARTITIONS;
}

vector<ProjectedValue> PartitionProjection::Values(ClientContext &context, idx_t key_index, int64_t lo,
                                                   int64_t hi) const {
	auto &key = keys[key_index];
	// the values of one key are enumerated before the filters narrow them
	static constexpr idx_t MAX_VALUES = 10000000;
	auto too_many = [&]() {
		return InvalidInputException("Hive table '%s' projects more than %llu values for partition column '%s'",
		                             table_name, MAX_VALUES, key.name);
	};
	vector<ProjectedValue> result;
	switch (key.type) {
	case ProjectionType::ENUM:
		for (auto &value : key.values) {
			result.push_back({value, value});
		}
		break;
	case ProjectionType::INTEGER: {
		// the first value on the interval's grid from lo, and the last up to hi
		auto step = static_cast<uint64_t>(key.step);
		uint64_t first = 0;
		if (lo > key.min) {
			auto distance = static_cast<uint64_t>(lo) - static_cast<uint64_t>(key.min);
			first = distance / step + (distance % step != 0);
		}
		auto top = MinValue(key.max, hi);
		if (top < key.min || first > (static_cast<uint64_t>(top) - static_cast<uint64_t>(key.min)) / step) {
			break;
		}
		auto count = (static_cast<uint64_t>(top) - static_cast<uint64_t>(key.min)) / step - first + 1;
		if (count > MAX_VALUES) {
			throw too_many();
		}
		for (idx_t i = 0; i < count; i++) {
			auto value = ZeroPad(static_cast<int64_t>(static_cast<uint64_t>(key.min) + (first + i) * step), key.digits);
			result.push_back({value, value});
		}
		break;
	}
	case ProjectionType::DATE: {
		auto now = MetaTransaction::Get(context).GetCurrentTransactionStartTimestamp();
		auto parse = [&](const string &text, timestamp_t &result) {
			StrpTimeFormat::ParseResult parsed;
			if (!StrpTimeFormat::TryParse(key.format, text, parsed) || !parsed.TryToTimestamp(result)) {
				return false;
			}
			if (StringUtil::Contains(key.format, "%y") && Date::ExtractYear(Timestamp::GetDate(result)) < 2000) {
				// Java reads yy with the base year 2000, strptime from 1969
				result = AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(result, {1200, 0, 0});
			}
			return true;
		};
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
				result = AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(
				    now, sign < 0 ? Interval::Invert(offset) : offset);
			} else if (!parse(Trim(text), result) || StrfTimeFormat::Format(result, key.format) != Trim(text)) {
				// as Java's strict parse: a field that appears twice has to read the same value
				throw InvalidInputException("Hive table '%s' has the date '%s' in 'projection.%s.range', which "
				                            "does not match its format",
				                            table_name, text, key.name);
			}
			// truncated to what the format writes, as a value read back from a location
			timestamp_t truncated;
			return parse(StrfTimeFormat::Format(result, key.format), truncated) ? truncated : result;
		};
		auto start = bound(key.range_start);
		auto end = bound(key.range_end);
		if (start > end) {
			throw InvalidInputException("Hive table '%s' has a 'projection.%s.range' whose lower bound is after its "
			                            "upper bound",
			                            table_name, key.name);
		}
		// a format may spell the same directory again later ('yyyy a'), not only in consecutive steps
		unordered_set<string> seen_paths;
		for (auto current = start; current <= end;) {
			auto path = StrfTimeFormat::Format(current, key.format);
			if (seen_paths.insert(path).second) {
				if (result.size() == MAX_VALUES) {
					throw too_many();
				}
				// a DATE or TIMESTAMP key reads the directory back with the format, as Athena does
				string value = path;
				timestamp_t read_back;
				if (key.value_type != LogicalTypeId::VARCHAR && parse(path, read_back)) {
					value = key.value_type == LogicalTypeId::DATE ? Date::ToString(Timestamp::GetDate(read_back))
					                                              : Timestamp::ToString(read_back);
				}
				result.push_back({std::move(value), std::move(path)});
			}
			if (key.interval.months == 0) {
				// a step finer than the format: skip the steps that write the same directory, doubling the skip
				// a step too long for int64_t microseconds skips nothing
				int64_t step = 0;
				if (key.interval.days <=
				    (NumericLimits<int64_t>::Maximum() - key.interval.micros) / Interval::MICROS_PER_DAY) {
					step = key.interval.days * Interval::MICROS_PER_DAY + key.interval.micros;
				}
				for (int64_t skip = 1; step > 0 && (end.value - current.value) / step >= skip;) {
					timestamp_t probe(current.value + skip * step);
					if (StrfTimeFormat::Format(probe, key.format) != result.back().path) {
						if (skip == 1) {
							break;
						}
						skip = 1;
						continue;
					}
					current = probe;
					skip = skip <= NumericLimits<int64_t>::Maximum() / 2 ? skip * 2 : skip;
				}
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

string PartitionProjection::Location(const vector<const string *> &paths) const {
	if (location_template.empty()) {
		auto result = root_location;
		for (idx_t i = 0; i < keys.size(); i++) {
			result += "/" + keys[i].name + "=" + *paths[i];
		}
		return result;
	}
	auto result = location_template;
	for (idx_t i = 0; i < keys.size(); i++) {
		result = StringUtil::Replace(result, "${" + keys[i].name + "}", *paths[i]);
	}
	return result;
}

string PartitionProjection::WriteRoot() const {
	if (location_template.empty()) {
		return root_location;
	}
	auto scheme = location_template.find("://");
	auto path_start = scheme == string::npos ? 0 : scheme + 3;
	auto slash = location_template.rfind('/', location_template.find("${"));
	auto prefix = slash == string::npos || slash < path_start ? string() : location_template.substr(0, slash);
	auto path = prefix.substr(MinValue(path_start, prefix.size()));
	if (path.empty() || path[0] == '/' || StringUtil::Contains(path + "/", "//")) {
		throw NotImplementedException("Can not write to Hive table '%s': its 'storage.location.template' has no "
		                              "directory before its first placeholder, or an empty segment in it",
		                              table_name);
	}
	return prefix;
}

vector<GluePartitionInfo> PartitionProjection::Combine(ClientContext &context,
                                                       const vector<vector<ProjectedValue>> &values) const {
	D_ASSERT(values.size() == keys.size());
	auto max_partitions = MaxPartitions(context);
	uint64_t count = 1;
	for (auto &key_values : values) {
		if (key_values.empty()) {
			count = 0;
			break;
		}
	}
	for (idx_t i = 0; count > 0 && i < values.size(); i++) {
		if (values[i].size() > max_partitions / count) {
			throw InvalidInputException("Scan of Glue table '%s' would list more than %llu derived partitions after "
			                            "pruning (glue_projection_max_partitions); filter on the partition columns or "
			                            "raise the setting",
			                            table_name, max_partitions);
		}
		count *= values[i].size();
	}
	vector<GluePartitionInfo> result;
	if (count == 0) {
		return result;
	}
	result.reserve(count);
	vector<idx_t> positions(values.size(), 0);
	vector<const string *> current(values.size());
	while (true) {
		GluePartitionInfo partition;
		for (idx_t i = 0; i < values.size(); i++) {
			current[i] = &values[i][positions[i]].path;
			partition.values.push_back(values[i][positions[i]].value);
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

ProjectedPartitionWriter::ProjectedPartitionWriter(ClientContext &context,
                                                   shared_ptr<const PartitionProjection> projection_p,
                                                   const vector<LogicalType> &types)
    : projection(std::move(projection_p)), paths(projection->Keys().size()) {
	auto &keys = projection->Keys();
	for (idx_t i = 0; i < keys.size(); i++) {
		if (keys[i].type == ProjectionType::INJECTED || keys[i].type == ProjectionType::INTEGER) {
			continue;
		}
		for (auto &value : projection->Values(context, i)) {
			auto typed = GlueTypes::PartitionValue(context, keys[i].name, value.value, types[i]);
			paths[i].emplace(typed.ToString(), value.path);
		}
	}
}

string ProjectedPartitionWriter::Location(const vector<Value> &values) const {
	auto &keys = projection->Keys();
	vector<string> texts(keys.size());
	vector<const string *> key_paths(keys.size());
	for (idx_t i = 0; i < keys.size(); i++) {
		auto refuse = [&](const string &reason) {
			return InvalidInputException("Can not write to Hive table '%s': partition column '%s' has the value %s, %s",
			                             projection->TableName(), keys[i].name, values[i].ToSQLString(), reason);
		};
		if (values[i].IsNull()) {
			throw InvalidInputException("Can not write to Hive table '%s': partition column '%s' is NULL, and a "
			                            "projected partition holds no NULL",
			                            projection->TableName(), keys[i].name);
		}
		texts[i] = values[i].ToString();
		if (keys[i].type == ProjectionType::INJECTED) {
			if (texts[i].empty()) {
				throw refuse("and an injected key has no empty value");
			}
			key_paths[i] = &texts[i];
			continue;
		}
		auto not_derived = [&]() {
			return refuse(StringUtil::Format("which its projection (projection.%s.*) does not derive", keys[i].name));
		};
		if (keys[i].type == ProjectionType::INTEGER) {
			// in the range and on the interval's grid, and for a string column spelled padded as the scan reads it
			auto &key = keys[i];
			auto number = values[i].DefaultTryCastAs(LogicalType::BIGINT);
			if (!number) {
				throw not_derived();
			}
			auto value = number->GetValue<int64_t>();
			if (!values[i].type().IsIntegral() && values[i].type().id() != LogicalTypeId::VARCHAR) {
				// a fraction is no integer value
				auto back = number->DefaultTryCastAs(values[i].type());
				if (!back || *back != values[i]) {
					throw not_derived();
				}
			}
			if (value < key.min || value > key.max ||
			    (static_cast<uint64_t>(value) - static_cast<uint64_t>(key.min)) % static_cast<uint64_t>(key.step) !=
			        0) {
				throw not_derived();
			}
			auto path = ZeroPad(value, key.digits);
			if (values[i].type().id() == LogicalTypeId::VARCHAR && path != texts[i]) {
				throw not_derived();
			}
			texts[i] = std::move(path);
			key_paths[i] = &texts[i];
			continue;
		}
		auto entry = paths[i].find(texts[i]);
		if (entry == paths[i].end()) {
			throw not_derived();
		}
		key_paths[i] = &entry->second;
	}
	auto location = projection->Location(key_paths);
	auto scheme = location.find("://");
	auto path = location.substr(scheme == string::npos ? 0 : scheme + 3) + "/";
	if (StringUtil::Contains(path, "//") || StringUtil::Contains(path, "/./") || StringUtil::Contains(path, "/../")) {
		throw InvalidInputException("Can not write to Hive table '%s': the partition location '%s' has an empty, '.' "
		                            "or '..' segment",
		                            projection->TableName(), location);
	}
	return location;
}

} // namespace duckdb
