#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Java's Integer.parseInt: an optional sign and digits only ("1.5" and " 1" are not numbers)
bool TryParseJavaInteger(const string &value, int64_t min, int64_t max, int64_t &result);
//! The first character of a string, false when it is empty or not valid UTF-8
bool TryFirstCharacter(const string &value, string &result);

} // namespace duckdb
