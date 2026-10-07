#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb_re2 {
class RE2;
}

namespace duckdb {

struct HiveScanInfo;

//! Hive's RegexSerDe: every line of a text file is matched against 'input.regex' as a whole, and the data columns of
//! the table are its capture groups in order. A line the regex does not match reads as NULL in every data column, and
//! a group that does not convert to its column's type reads as NULL.
class HiveRegexSerDe {
public:
	//! Compile the table's 'input.regex'; throws when RE2 can not compile it, its groups are not one per data column or
	//! a data column is not of a primitive type
	static shared_ptr<HiveRegexSerDe> Create(const HiveScanInfo &info);

	//! The expression that reads capture group 'group' (1-based) of the line at 'line_index' of the scanned chunk as
	//! 'type'
	unique_ptr<Expression> GroupExpression(idx_t group, const LogicalType &type, idx_t line_index) const;
	//! Whether 'expr' is a GroupExpression, and the group and line index it reads
	static bool IsGroupExpression(const Expression &expr, idx_t &group, idx_t &line_index);
	//! Match every line once and write the capture group groups[i] of each line to results[i]
	void Extract(ClientContext &context, Vector &lines, idx_t count, const vector<idx_t> &groups,
	             const vector<reference<Vector>> &results) const;

private:
	shared_ptr<duckdb_re2::RE2> regex;
};

} // namespace duckdb
