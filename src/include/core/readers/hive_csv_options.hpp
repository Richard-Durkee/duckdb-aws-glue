#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

//! How the files of a csv table are read
struct HiveCSVOptions {
	string delimiter = ",";
	string quote = "\"";
	string escape = "\"";
	//! The header lines every file starts with
	idx_t skip_lines = 0;
	string null_string;
	//! Read fields as text and TRY_CAST them; short rows padded with NULL, extra fields ignored
	bool serde_fields = false;
	//! As read_csv names it
	string encoding = "utf-8";

public:
	void Serialize(Serializer &serializer) const;
	static HiveCSVOptions Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
