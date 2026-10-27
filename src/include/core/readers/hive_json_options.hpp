#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Serializer;
class Deserializer;
struct SerDeProperties;

//! How the files of a json table are read
struct HiveJSONOptions {
	//! A line that fails to parse or a value that does not fit its column is NULL (OpenX JsonSerDe
	//! ignore.malformed.json / use.null.for.invalid.data)
	bool ignore_errors = false;

public:
	//! The options a JSON table's resolved SerDe properties give its reader
	static HiveJSONOptions FromSerDeProperties(const SerDeProperties &properties);
	void Serialize(Serializer &serializer) const;
	static HiveJSONOptions Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
