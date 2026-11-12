#include "planning/hive_projection_pushdown.hpp"
#include "core/glue_types.hpp"
#include "core/partition_projection.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

//! The partition key a filter reads, when it reads exactly one and no other column of the scan
static optional_idx SinglePartitionKeyOfFilter(const Expression &filter, TableIndex table_index,
                                               const vector<PartitionKeyProjection> &projections) {
	optional_idx key;
	bool other = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(filter, [&](const BoundColumnRefExpression &colref) {
		if (colref.Binding().table_index != table_index) {
			other = true;
			return;
		}
		auto column_index = colref.Binding().column_index.GetIndex();
		for (auto &projection : projections) {
			if (projection.projected_column_index == column_index) {
				if (key.IsValid() && key.GetIndex() != projection.partition_key_index) {
					other = true;
				}
				key = projection.partition_key_index;
				return;
			}
		}
		other = true;
	});
	return other ? optional_idx() : key;
}

//! The values 'filter' allows a string column, when it is '<column> = <constant>' or '<column> IN (<constants>)'
static bool TryGetEqualityValues(const Expression &filter, vector<string> &values, bool through_casts = false) {
	auto constant_string = [&](const Expression &expr) {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		auto &value = expr.Cast<BoundConstantExpression>().GetValue();
		if (!value.IsNull() && !value.ToString().empty()) {
			values.push_back(value.ToString());
		}
		return true;
	};
	auto is_column = [&](const Expression &expr) {
		auto current = &expr;
		while (through_casts && BoundCastExpression::IsCast(*current) && current->GetReturnType().IsIntegral()) {
			current = &BoundCastExpression::Child(current->Cast<BoundFunctionExpression>());
		}
		return current->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
	};
	if (filter.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
		auto &children = filter.Cast<BoundOperatorExpression>().GetChildren();
		if (children.size() == 1 && children[0]->GetExpressionType() == ExpressionType::OPERATOR_NOT) {
			auto &inner = children[0]->Cast<BoundOperatorExpression>().GetChildren();
			return inner.size() == 1 && TryGetEqualityValues(*inner[0], values, through_casts);
		}
		return false;
	}
	if (filter.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		for (auto &child : filter.Cast<BoundConjunctionExpression>().GetChildren()) {
			if (!TryGetEqualityValues(*child, values, through_casts)) {
				values.clear();
				return false;
			}
		}
		return true;
	}
	if (filter.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
		auto &children = filter.Cast<BoundOperatorExpression>().GetChildren();
		return children.size() == 1 && is_column(*children[0]);
	}
	if (is_column(filter) && filter.GetReturnType().id() == LogicalTypeId::BOOLEAN) {
		values.push_back(Value::BOOLEAN(true).ToString());
		return true;
	}
	if ((filter.GetExpressionType() == ExpressionType::COMPARE_EQUAL ||
	     filter.GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) &&
	    BoundComparisonExpression::IsComparison(filter)) {
		auto &comparison = filter.Cast<BoundFunctionExpression>();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		if (is_column(left)) {
			return constant_string(right);
		}
		if (is_column(right)) {
			return constant_string(left);
		}
		return false;
	}
	if (filter.GetExpressionType() == ExpressionType::COMPARE_IN) {
		auto &children = filter.Cast<BoundOperatorExpression>().GetChildren();
		if (children.empty() || !is_column(*children[0])) {
			return false;
		}
		for (idx_t i = 1; i < children.size(); i++) {
			if (!constant_string(*children[i])) {
				values.clear();
				return false;
			}
		}
		return true;
	}
	return false;
}

static const LogicalType &KeyType(const HiveScanInfo &info, idx_t key_index) {
	for (idx_t i = 0; i < info.names.size(); i++) {
		if (info.names[i].GetIdentifierName() == info.partition_keys[key_index]) {
			return info.types[i];
		}
	}
	throw InternalException("Hive scan has no column for partition key '%s'", info.partition_keys[key_index]);
}

//! Narrows [lo, hi] to the values a comparison, BETWEEN or IN of an integer column with constants allows
static void NarrowIntegerBounds(const Expression &filter, hugeint_t &lo, hugeint_t &hi) {
	auto is_column = [](const Expression &expr) {
		auto current = &expr;
		while (BoundCastExpression::IsCast(*current) && current->GetReturnType().IsIntegral()) {
			current = &BoundCastExpression::Child(current->Cast<BoundFunctionExpression>());
		}
		return current->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
	};
	auto constant = [](const Expression &expr, hugeint_t &result) {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		auto &value = expr.Cast<BoundConstantExpression>().GetValue();
		if (value.IsNull() || !value.type().IsIntegral()) {
			return false;
		}
		result = value.GetValue<hugeint_t>();
		return true;
	};
	auto apply = [&](ExpressionType type, hugeint_t value) {
		if (type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			type = ExpressionType::COMPARE_EQUAL;
		}
		if (type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			lo = MaxValue(lo, value);
		} else if (type == ExpressionType::COMPARE_GREATERTHAN) {
			lo = MaxValue(lo, value + 1);
		}
		if (type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
			hi = MinValue(hi, value);
		} else if (type == ExpressionType::COMPARE_LESSTHAN) {
			hi = MinValue(hi, value - 1);
		}
	};
	hugeint_t left_value, right_value;
	if (filter.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto union_lo = NumericLimits<hugeint_t>::Maximum();
		auto union_hi = NumericLimits<hugeint_t>::Minimum();
		for (auto &child : filter.Cast<BoundConjunctionExpression>().GetChildren()) {
			auto child_lo = lo;
			auto child_hi = hi;
			NarrowIntegerBounds(*child, child_lo, child_hi);
			if (child_lo <= child_hi) {
				union_lo = MinValue(union_lo, child_lo);
				union_hi = MaxValue(union_hi, child_hi);
			}
		}
		lo = union_lo;
		hi = union_hi;
	} else if (filter.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
		auto &children = filter.Cast<BoundOperatorExpression>().GetChildren();
		if (children.size() == 1 && is_column(*children[0])) {
			lo = NumericLimits<hugeint_t>::Maximum();
			hi = NumericLimits<hugeint_t>::Minimum();
		}
	} else if (filter.GetExpressionType() == ExpressionType::COMPARE_BETWEEN) {
		auto &between = filter.Cast<BoundFunctionExpression>();
		if (is_column(BoundBetweenExpression::Input(between))) {
			if (constant(BoundBetweenExpression::LowerBound(between), left_value)) {
				apply(BoundBetweenExpression::LowerComparisonType(between), left_value);
			}
			if (constant(BoundBetweenExpression::UpperBound(between), right_value)) {
				apply(BoundBetweenExpression::UpperComparisonType(between), right_value);
			}
		}
	} else if (filter.GetExpressionType() == ExpressionType::COMPARE_IN) {
		auto &children = filter.Cast<BoundOperatorExpression>().GetChildren();
		if (children.empty() || !is_column(*children[0])) {
			return;
		}
		auto in_lo = NumericLimits<hugeint_t>::Maximum();
		auto in_hi = NumericLimits<hugeint_t>::Minimum();
		for (idx_t i = 1; i < children.size(); i++) {
			if (!constant(*children[i], left_value)) {
				return;
			}
			in_lo = MinValue(in_lo, left_value);
			in_hi = MaxValue(in_hi, left_value);
		}
		lo = MaxValue(lo, in_lo);
		hi = MinValue(hi, in_hi);
	} else if (BoundComparisonExpression::IsComparison(filter)) {
		auto &comparison = filter.Cast<BoundFunctionExpression>();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		if (is_column(left) && constant(right, right_value)) {
			apply(filter.GetExpressionType(), right_value);
		} else if (is_column(right) && constant(left, left_value)) {
			apply(FlipComparisonExpression(filter.GetExpressionType()), left_value);
		}
	}
}

vector<GluePartitionInfo> ProjectPartitions(ClientContext &context, const HiveScanInfo &info, TableIndex table_index,
                                            const vector<PartitionKeyProjection> &projections,
                                            const vector<unique_ptr<Expression>> &filters, idx_t &projected,
                                            unordered_set<idx_t> &pruning_filters) {
	auto &projection = *info.projection;
	auto key_count = projection.Keys().size();
	vector<vector<idx_t>> key_filters(key_count);
	for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
		auto key = SinglePartitionKeyOfFilter(*filters[filter_index], table_index, projections);
		if (key.IsValid()) {
			key_filters[key.GetIndex()].push_back(filter_index);
		}
	}
	vector<vector<ProjectedValue>> values;
	projected = 1;
	for (idx_t key_index = 0; key_index < key_count; key_index++) {
		vector<ProjectedValue> candidates;
		optional_idx full_count;
		if (projection.Keys()[key_index].type == ProjectionType::INJECTED) {
			bool found = false;
			for (auto filter_index : key_filters[key_index]) {
				vector<string> allowed;
				if (TryGetEqualityValues(*filters[filter_index], allowed) && !allowed.empty()) {
					pruning_filters.insert(filter_index);
					unordered_set<string> seen;
					for (auto &value : allowed) {
						if (seen.insert(value).second) {
							candidates.push_back({value, value});
						}
					}
					found = true;
					break;
				}
			}
			if (!found) {
				projection.ThrowMissingInjectedFilter(key_index);
			}
		} else if (projection.Keys()[key_index].type == ProjectionType::INTEGER &&
		           KeyType(info, key_index).IsIntegral()) {
			auto lo = hugeint_t(NumericLimits<int64_t>::Minimum());
			auto hi = hugeint_t(NumericLimits<int64_t>::Maximum());
			for (auto filter_index : key_filters[key_index]) {
				auto before = make_pair(lo, hi);
				NarrowIntegerBounds(*filters[filter_index], lo, hi);
				if (before != make_pair(lo, hi)) {
					pruning_filters.insert(filter_index);
				}
			}
			full_count = projection.IntegerCount(key_index);
			// = and IN name their values, however far apart they are
			vector<string> allowed;
			optional_idx equality_filter;
			for (auto filter_index : key_filters[key_index]) {
				if (TryGetEqualityValues(*filters[filter_index], allowed, true)) {
					equality_filter = filter_index;
					break;
				}
			}
			if (equality_filter.IsValid()) {
				set<int64_t> values;
				for (auto &text : allowed) {
					int64_t value;
					if (TryCast::Operation<string_t, int64_t>(string_t(text), value) && hugeint_t(value) >= lo &&
					    hugeint_t(value) <= hi) {
						values.insert(value);
					}
				}
				for (auto value : values) {
					for (auto &candidate : projection.Values(context, key_index, value, value)) {
						candidates.push_back(std::move(candidate));
					}
				}
				pruning_filters.insert(equality_filter.GetIndex());
			} else if (lo <= hi) {
				auto int64_bound = [](hugeint_t value) {
					return Hugeint::Cast<int64_t>(
					    MaxValue(MinValue(value, hugeint_t(NumericLimits<int64_t>::Maximum())),
					             hugeint_t(NumericLimits<int64_t>::Minimum())));
				};
				candidates = projection.Values(context, key_index, int64_bound(lo), int64_bound(hi));
			}
		} else {
			candidates = projection.Values(context, key_index);
		}
		auto key_count_values = full_count.IsValid() ? full_count.GetIndex() : candidates.size();
		projected = key_count_values == 0 || projected <= NumericLimits<idx_t>::Maximum() / key_count_values
		                ? projected * key_count_values
		                : NumericLimits<idx_t>::Maximum();
		vector<ProjectedValue> kept;
		GluePartitionInfo partition;
		partition.values.resize(key_count);
		for (auto &candidate : candidates) {
			partition.values[key_index] = candidate.value;
			bool keep = true;
			for (auto filter_index : key_filters[key_index]) {
				auto filter_copy = filters[filter_index]->Copy();
				ReplacePartitionColumnRefs(context, filter_copy, table_index, projections, info, partition);
				Value result;
				if (filter_copy->IsScalar() && filter_copy->IsFoldable() &&
				    ExpressionExecutor::TryEvaluateScalar(context, *filter_copy, result) &&
				    (result.IsNull() || !result.GetValue<bool>())) {
					keep = false;
					pruning_filters.insert(filter_index);
					break;
				}
			}
			if (keep) {
				kept.push_back(std::move(candidate));
			}
		}
		values.push_back(std::move(kept));
	}
	return projection.Combine(context, values);
}

} // namespace duckdb
