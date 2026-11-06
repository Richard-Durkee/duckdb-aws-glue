#include "core/serde/hive_json_dialect.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

void HiveJSONDialect::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "ignore_errors", ignore_errors);
	serializer.WriteProperty(101, "case_insensitive_keys", case_insensitive_keys);
}

HiveJSONDialect HiveJSONDialect::Deserialize(Deserializer &deserializer) {
	HiveJSONDialect result;
	result.ignore_errors = deserializer.ReadProperty<bool>(100, "ignore_errors");
	result.case_insensitive_keys = deserializer.ReadProperty<bool>(101, "case_insensitive_keys");
	return result;
}

} // namespace duckdb
