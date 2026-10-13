#pragma once

namespace duckdb {

class Serializer;
class Deserializer;

//! How the files of a json table are read
struct HiveJSONOptions {
	//! A line that fails to parse is a row of NULLs (OpenX JsonSerDe ignore.malformed.json)
	bool ignore_errors = false;

public:
	void Serialize(Serializer &serializer) const;
	static HiveJSONOptions Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
