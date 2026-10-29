#pragma once

#include "core/readers/hive_csv_options.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class ClientContext;

//! A LazySimpleSerDe parse of a field read as text into a value of 'type', in core SQL only, over 'field'
unique_ptr<ParsedExpression> HiveFieldConversionExpression(HiveFieldConversion conversion, const LogicalType &type,
                                                           const ParsedExpression &field);

//! The same parse bound over the bound expression 'field'
unique_ptr<Expression> BindHiveFieldConversion(ClientContext &context, HiveFieldConversion conversion,
                                               const LogicalType &type, const Expression &field);

} // namespace duckdb
