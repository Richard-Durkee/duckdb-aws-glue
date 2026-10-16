#include "catalog/glue_view_names.hpp"

#include "catalog/glue_catalog.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

namespace {

//! called per base table, with the CTE names in scope
using BaseTableCallback = std::function<void(BaseTableRef &ref, const case_insensitive_set_t &visible_ctes)>;

//! Walks a view body with SQL CTE scoping; refuses what a Glue view can not store
class ViewBodyWalker {
public:
	explicit ViewBodyWalker(BaseTableCallback callback_p) : callback(std::move(callback_p)) {
	}

	void Walk(QueryNode &node) {
		case_insensitive_set_t visible;
		VisitNode(node, visible);
	}

private:
	void VisitNode(QueryNode &node, const case_insensitive_set_t &outer) {
		// a CTE is visible in its query and in later CTEs; in its own body only if recursive
		case_insensitive_set_t visible = outer;
		for (auto &entry : node.cte_map.map) {
			if (entry.second->query_node) {
				VisitNode(*entry.second->query_node, visible);
			}
			visible.insert(entry.first.GetIdentifierName());
		}
		switch (node.type) {
		case QueryNodeType::SELECT_NODE: {
			auto &select = node.Cast<SelectNode>();
			if (select.from_table) {
				VisitRef(*select.from_table, visible);
			}
			for (auto &expr : select.select_list) {
				VisitExpression(expr, visible);
			}
			for (auto &expr : select.groups.group_expressions) {
				VisitExpression(expr, visible);
			}
			VisitExpression(select.where_clause, visible);
			VisitExpression(select.having, visible);
			VisitExpression(select.qualify, visible);
			break;
		}
		case QueryNodeType::SET_OPERATION_NODE:
			for (auto &child : node.Cast<SetOperationNode>().children) {
				VisitNode(*child, visible);
			}
			break;
		case QueryNodeType::RECURSIVE_CTE_NODE: {
			auto &recursive = node.Cast<RecursiveCTENode>();
			auto with_self = visible;
			with_self.insert(recursive.ctename.GetIdentifierName());
			VisitNode(*recursive.left, with_self);
			VisitNode(*recursive.right, with_self);
			break;
		}
		default:
			throw BinderException("Glue views can not store a query of type %s", EnumUtil::ToString(node.type));
		}
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(
		    node, [&](unique_ptr<ParsedExpression> &child) { VisitExpression(child, visible); });
	}

	void VisitRef(TableRef &ref, const case_insensitive_set_t &visible) {
		switch (ref.type) {
		case TableReferenceType::BASE_TABLE:
			callback(ref.Cast<BaseTableRef>(), visible);
			break;
		case TableReferenceType::JOIN: {
			auto &join = ref.Cast<JoinRef>();
			VisitRef(*join.left, visible);
			VisitRef(*join.right, visible);
			VisitExpression(join.condition, visible);
			break;
		}
		case TableReferenceType::SUBQUERY:
			VisitNode(*ref.Cast<SubqueryRef>().subquery->node, visible);
			break;
		case TableReferenceType::PIVOT: {
			auto &pivot = ref.Cast<PivotRef>();
			VisitRef(*pivot.source, visible);
			for (auto &aggregate : pivot.aggregates) {
				VisitExpression(aggregate, visible);
			}
			break;
		}
		case TableReferenceType::EXPRESSION_LIST:
			for (auto &row : ref.Cast<ExpressionListRef>().values) {
				for (auto &value : row) {
					VisitExpression(value, visible);
				}
			}
			break;
		case TableReferenceType::EMPTY_FROM:
			break;
		case TableReferenceType::TABLE_FUNCTION:
			// a source outside the Glue catalog
			throw BinderException("Glue views can not read from table functions (\"%s\"): a Glue view can only "
			                      "reference tables in Glue catalogs",
			                      ref.ToString());
		default:
			throw BinderException("Glue views can not store a FROM clause of type %s", EnumUtil::ToString(ref.type));
		}
	}

	void VisitExpression(unique_ptr<ParsedExpression> &expr, const case_insensitive_set_t &visible) {
		if (!expr) {
			return;
		}
		if (expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
			auto &subquery = expr->Cast<SubqueryExpression>().SubqueryMutable();
			if (subquery && subquery->node) {
				VisitNode(*subquery->node, visible);
			}
		}
		ParsedExpressionIterator::EnumerateChildren(
		    *expr, [&](unique_ptr<ParsedExpression> &child) { VisitExpression(child, visible); });
	}

	BaseTableCallback callback;
};

optional_ptr<CatalogEntry> LookupTable(ClientContext &context, QualifiedName name) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, std::move(name));
	return Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
}

} // namespace

void GlueViewNames::QualifyForStorage(ClientContext &context, GlueCatalog &view_catalog, const Identifier &view_schema,
                                      QueryNode &query, bool defer_binding) {
	auto view_catalog_name = view_catalog.GetName();
	ViewBodyWalker walker([&](BaseTableRef &ref, const case_insensitive_set_t &visible_ctes) {
		auto name = ref.GetQualifiedName();
		auto catalog_name = name.Catalog();
		if (catalog_name.empty() && !name.Schema().empty()) {
			// x.t names catalog x when one is attached under that name, as DuckDB binds it
			auto schema_name = name.Schema();
			Binder::BindSchemaOrCatalog(context, catalog_name, schema_name);
		}
		if (!catalog_name.empty()) {
			// core has stripped the view's own catalog name, so this is another catalog
			throw BinderException("Glue view references \"%s\" in catalog \"%s\": a Glue view can only reference "
			                      "tables of its own Glue catalog",
			                      name.ToString(), catalog_name.GetIdentifierName());
		}
		if (!name.Schema().empty() || visible_ctes.count(name.Name().GetIdentifierName())) {
			// db.t of the view's own catalog (PinToCatalog keeps it there when read), or a CTE in scope
			return;
		}
		// a bare name binds through the session search path too: store it as db.t, resolved as DuckDB binds a view
		// (a temporary table first)
		auto entry =
		    LookupTable(context, QualifiedName(Identifier(TEMP_CATALOG), Identifier(DEFAULT_SCHEMA), name.Name()));
		if (!entry) {
			entry = LookupTable(context, QualifiedName(view_catalog_name, view_schema, name.Name()));
		}
		if (!entry) {
			entry = LookupTable(context, name);
		}
		if (!entry && defer_binding) {
			// not there yet: the view's own database
			ref.SetQualifiedName(Identifier(), view_schema, name.Name());
			return;
		}
		if (!entry) {
			throw BinderException("Glue view references table \"%s\", which was not found", name.ToString());
		}
		if (&entry->ParentCatalog() != &view_catalog) {
			throw BinderException("Glue view references table \"%s\", which is not in the view's Glue catalog but in "
			                      "catalog \"%s\" (through the search path)",
			                      name.ToString(), entry->ParentCatalog().GetName().GetIdentifierName());
		}
		ref.SetQualifiedName(Identifier(), entry->ParentSchema().name, entry->name);
	});
	walker.Walk(query);
}

void GlueViewNames::PinToCatalog(QueryNode &query, const Identifier &catalog_name) {
	ViewBodyWalker walker([&](BaseTableRef &ref, const case_insensitive_set_t &) {
		auto name = ref.GetQualifiedName();
		if (name.Catalog().empty() && !name.Schema().empty()) {
			ref.SetQualifiedName(catalog_name, name.Schema(), name.Name());
		}
	});
	walker.Walk(query);
}

} // namespace duckdb
