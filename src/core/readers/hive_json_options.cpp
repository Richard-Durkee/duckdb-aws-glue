#include "core/readers/hive_json_options.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

void HiveJSONOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "ignore_errors", ignore_errors);
}

HiveJSONOptions HiveJSONOptions::Deserialize(Deserializer &deserializer) {
	HiveJSONOptions result;
	result.ignore_errors = deserializer.ReadProperty<bool>(100, "ignore_errors");
	return result;
}

} // namespace duckdb
