#include "grammar/glue_grammar.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/parser/grammar_change.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

namespace {

bool IsRule(const ParseResult &parse_result, const char *name) {
	return StringUtil::CIEquals(parse_result.Name(), name);
}

unique_ptr<ParsedExpression> Constant(const Value &value) {
	return ConstantExpression::FromValue(value);
}

unique_ptr<ParsedExpression> Call(const char *function_name, vector<FunctionArgument> arguments) {
	return make_uniq<FunctionExpression>(Identifier(function_name), std::move(arguments));
}

//! GluePartitionSpec <- 'PARTITION' Parens(List(GluePartitionValue))
//! GluePartitionValue <- ColumnName '=' Expression
//! -> list_value(struct_pack(key := 'dt', value := CAST(<expr> AS VARCHAR)), ...)
unique_ptr<ParsedExpression> TransformPartitionSpec(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &spec = parse_result.Cast<ListParseResult>();
	auto entries = PEGTransformerFactory::ExtractParseResultsFromList(
	    PEGTransformerFactory::ExtractResultFromParens(spec.GetChild(1)));
	vector<FunctionArgument> pairs;
	for (auto &entry : entries) {
		auto &key_value = entry.get().Cast<ListParseResult>();
		// ColumnName is matched as a bare identifier: read it, it has no transform of its own
		auto &key_result = key_value.GetChild(0);
		auto &key_identifier = key_result.type == ParseResultType::IDENTIFIER
		                           ? key_result.Cast<IdentifierParseResult>()
		                           : key_result.Cast<ListParseResult>().Child<IdentifierParseResult>(0);
		auto key = key_identifier.identifier;
		auto value = transformer.Transform<unique_ptr<ParsedExpression>>(key_value.GetChild(2));
		vector<FunctionArgument> fields;
		fields.emplace_back(Identifier("key"), Constant(Value(key.GetIdentifierName())));
		fields.emplace_back(Identifier("value"), make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(value)));
		pairs.emplace_back(Call("struct_pack", std::move(fields)));
	}
	return Call("list_value", std::move(pairs));
}

//! GlueLocation <- 'LOCATION' StringLiteral
unique_ptr<ParsedExpression> TransformLocation(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &location = parse_result.Cast<ListParseResult>();
	return Constant(Value(transformer.Transform<string>(location.GetChild(1))));
}

unique_ptr<ParsedExpression> MakeAction(const char *action, bool if_not_exists, bool if_exists,
                                        unique_ptr<ParsedExpression> partition,
                                        unique_ptr<ParsedExpression> new_partition,
                                        unique_ptr<ParsedExpression> location) {
	vector<FunctionArgument> fields;
	fields.emplace_back(Identifier("action"), Constant(Value(action)));
	fields.emplace_back(Identifier("if_not_exists"), Constant(Value::BOOLEAN(if_not_exists)));
	fields.emplace_back(Identifier("if_exists"), Constant(Value::BOOLEAN(if_exists)));
	fields.emplace_back(Identifier("partition"), partition ? std::move(partition) : Constant(Value()));
	fields.emplace_back(Identifier("new_partition"), new_partition ? std::move(new_partition) : Constant(Value()));
	fields.emplace_back(Identifier("location"), location ? std::move(location) : Constant(Value()));
	return Call("struct_pack", std::move(fields));
}

void TransformAction(PEGTransformer &transformer, ParseResult &action_result, vector<FunctionArgument> &actions) {
	// GluePartitionAction <- GlueAddPartitions / GlueDropPartitions / GlueRenamePartition / ...
	auto &action = action_result.Cast<ListParseResult>().Child<ChoiceParseResult>(0).GetResult();
	auto &list = action.Cast<ListParseResult>();
	if (IsRule(action, "GlueAddPartitions")) {
		// 'ADD' IfNotExists? GluePartitionWithLocation+
		bool if_not_exists = list.Child<OptionalParseResult>(1).HasResult();
		for (auto &entry : list.Child<RepeatParseResult>(2).GetChildren()) {
			// GluePartitionWithLocation <- GluePartitionSpec GlueLocation?
			auto &with_location = entry.get().Cast<ListParseResult>();
			auto partition = TransformPartitionSpec(transformer, with_location.GetChild(0));
			unique_ptr<ParsedExpression> location;
			auto &optional_location = with_location.Child<OptionalParseResult>(1);
			if (optional_location.HasResult()) {
				location = TransformLocation(transformer, optional_location.GetResult());
			}
			actions.emplace_back(
			    MakeAction("add", if_not_exists, false, std::move(partition), nullptr, std::move(location)));
		}
		return;
	}
	if (IsRule(action, "GlueDropPartitions")) {
		// 'DROP' IfExists? List(GluePartitionSpec)
		bool if_exists = list.Child<OptionalParseResult>(1).HasResult();
		for (auto &entry : PEGTransformerFactory::ExtractParseResultsFromList(list.GetChild(2))) {
			auto partition = TransformPartitionSpec(transformer, entry.get());
			actions.emplace_back(MakeAction("drop", false, if_exists, std::move(partition), nullptr, nullptr));
		}
		return;
	}
	if (IsRule(action, "GlueRenamePartition")) {
		// GluePartitionSpec 'RENAME' 'TO' GluePartitionSpec
		auto partition = TransformPartitionSpec(transformer, list.GetChild(0));
		auto new_partition = TransformPartitionSpec(transformer, list.GetChild(3));
		actions.emplace_back(
		    MakeAction("rename", false, false, std::move(partition), std::move(new_partition), nullptr));
		return;
	}
	if (IsRule(action, "GluePartitionSetLocation")) {
		// GluePartitionSpec 'SET' GlueLocation
		auto partition = TransformPartitionSpec(transformer, list.GetChild(0));
		auto location = TransformLocation(transformer, list.GetChild(2));
		actions.emplace_back(
		    MakeAction("set_partition_location", false, false, std::move(partition), nullptr, std::move(location)));
		return;
	}
	if (IsRule(action, "GlueTableSetLocation")) {
		// 'SET' GlueLocation
		auto location = TransformLocation(transformer, list.GetChild(1));
		actions.emplace_back(MakeAction("set_table_location", false, false, nullptr, nullptr, std::move(location)));
		return;
	}
	throw InternalException("Unknown Glue partition action rule '%s'", action.Name());
}

//! GluePartitionActions <- GluePartitionAction+
//! -> an ALTER TABLE action 'glue_alter_table' with payload {actions: [...]}, the actions glue_alter_table takes
arena_ptr<TransformResultValue> FinalizeGluePartitionActions(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	vector<FunctionArgument> actions;
	for (auto &action : list.Child<RepeatParseResult>(0).GetChildren()) {
		TransformAction(transformer, action.get(), actions);
	}
	vector<string> action_sql;
	for (auto &action : actions) {
		action_sql.push_back(action.GetExpression().ToString());
	}
	vector<FunctionArgument> payload;
	payload.emplace_back(Identifier("actions"), Call("list_value", std::move(actions)));
	unique_ptr<AlterTableInfo> result = make_uniq<ExtensionAlterTableInfo>(
	    AlterEntryData(), "glue_alter_table", Call("struct_pack", std::move(payload)),
	    "PARTITION ACTIONS " + StringUtil::Join(action_sql, ", "));
	return transformer.MakeResult<unique_ptr<AlterTableInfo>>(std::move(result));
}

arena_ptr<TransformProcess> StartGluePartitionActionsTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, FinalizeGluePartitionActions);
}

//! GlueReplaceColumns / GlueAddColumns <- GluePartitionSpec? ('REPLACE' / 'ADD') 'COLUMNS'
//!                                       Parens(List(GlueColumnDefinition)) 'CASCADE'?
//! -> an ALTER TABLE action 'glue_replace_columns' / 'glue_add_columns' with payload
//!    {partition: [{key, value}, ...] or NULL, columns: [{name, type, comment}, ...], cascade}
arena_ptr<TransformResultValue> TransformColumnsAction(PEGTransformer &transformer, ParseResult &parse_result,
                                                       const char *alter_name, const char *verb) {
	auto &list = parse_result.Cast<ListParseResult>();
	string action_sql;
	unique_ptr<ParsedExpression> partition;
	auto &optional_partition = list.Child<OptionalParseResult>(0);
	if (optional_partition.HasResult()) {
		partition = TransformPartitionSpec(transformer, optional_partition.GetResult());
		action_sql += "PARTITION (...) ";
	}
	vector<FunctionArgument> columns;
	vector<string> column_sql;
	for (auto &entry : PEGTransformerFactory::ExtractParseResultsFromList(
	         PEGTransformerFactory::ExtractResultFromParens(list.GetChild(3)))) {
		// GlueColumnDefinition <- ColumnName Type GlueColumnComment?
		auto &definition = entry.get().Cast<ListParseResult>();
		auto &name_result = definition.GetChild(0);
		auto &name_identifier = name_result.type == ParseResultType::IDENTIFIER
		                            ? name_result.Cast<IdentifierParseResult>()
		                            : name_result.Cast<ListParseResult>().Child<IdentifierParseResult>(0);
		auto name = name_identifier.identifier.GetIdentifierName();
		auto type = transformer.Transform<LogicalType>(definition.GetChild(1)).ToString();
		Value comment(LogicalType::VARCHAR);
		auto &optional_comment = definition.Child<OptionalParseResult>(2);
		string sql = SQLIdentifier::ToString(name) + " " + type;
		if (optional_comment.HasResult()) {
			// GlueColumnComment <- 'COMMENT' StringLiteral
			comment =
			    Value(transformer.Transform<string>(optional_comment.GetResult().Cast<ListParseResult>().GetChild(1)));
			sql += " COMMENT " + SQLString::ToString(comment.ToString());
		}
		column_sql.push_back(std::move(sql));
		vector<FunctionArgument> fields;
		fields.emplace_back(Identifier("name"), Constant(Value(name)));
		fields.emplace_back(Identifier("type"), Constant(Value(type)));
		fields.emplace_back(Identifier("comment"), Constant(comment));
		columns.emplace_back(Call("struct_pack", std::move(fields)));
	}
	bool cascade = list.Child<OptionalParseResult>(4).HasResult();
	action_sql += string(verb) + " COLUMNS (" + StringUtil::Join(column_sql, ", ") + ")";
	if (cascade) {
		action_sql += " CASCADE";
	}
	vector<FunctionArgument> payload;
	payload.emplace_back(Identifier("partition"),
	                     partition ? std::move(partition) : Constant(Value(LogicalType::SQLNULL)));
	payload.emplace_back(Identifier("columns"), Call("list_value", std::move(columns)));
	payload.emplace_back(Identifier("cascade"), Constant(Value::BOOLEAN(cascade)));
	unique_ptr<AlterTableInfo> result = make_uniq<ExtensionAlterTableInfo>(
	    AlterEntryData(), alter_name, Call("struct_pack", std::move(payload)), std::move(action_sql));
	return transformer.MakeResult<unique_ptr<AlterTableInfo>>(std::move(result));
}

arena_ptr<TransformResultValue> FinalizeGlueReplaceColumns(PEGTransformer &transformer, ParseResult &parse_result) {
	return TransformColumnsAction(transformer, parse_result, "glue_replace_columns", "REPLACE");
}

arena_ptr<TransformProcess> StartGlueReplaceColumnsTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, FinalizeGlueReplaceColumns);
}

arena_ptr<TransformResultValue> FinalizeGlueAddColumns(PEGTransformer &transformer, ParseResult &parse_result) {
	return TransformColumnsAction(transformer, parse_result, "glue_add_columns", "ADD");
}

arena_ptr<TransformProcess> StartGlueAddColumnsTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, FinalizeGlueAddColumns);
}

class GlueHiveDDLGrammar final : public GrammarExtension {
public:
	GlueHiveDDLGrammar()
	    : GrammarExtension("glue_hive_ddl",
	                       "Hive partition DDL for Hive tables in Glue: ALTER TABLE ... "
	                       "ADD / DROP PARTITION, RENAME PARTITION, SET LOCATION, [PARTITION (...)] REPLACE "
	                       "COLUMNS (...) [CASCADE]") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(
		    GrammarChange::AddRule("GluePartitionActions <- GluePartitionAction+", StartGluePartitionActionsTransform));
		changes.push_back(GrammarChange::AddRule(
		    "GluePartitionAction <- GlueAddPartitions / GlueDropPartitions / GlueRenamePartition / "
		    "GluePartitionSetLocation / GlueTableSetLocation"));
		changes.push_back(GrammarChange::AddRule("GlueAddPartitions <- 'ADD' IfNotExists? GluePartitionWithLocation+"));
		changes.push_back(GrammarChange::AddRule("GluePartitionWithLocation <- GluePartitionSpec GlueLocation?"));
		changes.push_back(GrammarChange::AddRule("GlueDropPartitions <- 'DROP' IfExists? List(GluePartitionSpec)"));
		changes.push_back(
		    GrammarChange::AddRule("GlueRenamePartition <- GluePartitionSpec 'RENAME' 'TO' GluePartitionSpec"));
		changes.push_back(GrammarChange::AddRule("GluePartitionSetLocation <- GluePartitionSpec 'SET' GlueLocation"));
		changes.push_back(GrammarChange::AddRule("GlueTableSetLocation <- 'SET' GlueLocation"));
		changes.push_back(GrammarChange::AddRule("GluePartitionSpec <- 'PARTITION' Parens(List(GluePartitionValue))"));
		changes.push_back(GrammarChange::AddRule("GluePartitionValue <- ColumnName '=' Expression"));
		changes.push_back(GrammarChange::AddRule("GlueLocation <- 'LOCATION' StringLiteral"));
		changes.push_back(GrammarChange::AddRule(
		    "GlueReplaceColumns <- GluePartitionSpec? 'REPLACE' 'COLUMNS' Parens(List(GlueColumnDefinition)) "
		    "'CASCADE'?",
		    StartGlueReplaceColumnsTransform));
		changes.push_back(GrammarChange::AddRule(
		    "GlueAddColumns <- GluePartitionSpec? 'ADD' 'COLUMNS' Parens(List(GlueColumnDefinition)) 'CASCADE'?",
		    StartGlueAddColumnsTransform));
		changes.push_back(GrammarChange::AddRule("GlueColumnDefinition <- ColumnName Type GlueColumnComment?"));
		changes.push_back(GrammarChange::AddRule("GlueColumnComment <- 'COMMENT' StringLiteral"));
		// actions of the built-in ALTER TABLE statement, executed by the catalog that owns the table; tried before the
		// built-in actions, they fail on anything else, so the built-in ALTER TABLE forms are unaffected
		changes.push_back(GrammarChange::PrependChoice("AlterTableOptions", "GluePartitionActions"));
		changes.push_back(GrammarChange::PrependChoice("AlterTableOptions", "GlueReplaceColumns"));
		changes.push_back(GrammarChange::PrependChoice("AlterTableOptions", "GlueAddColumns"));
		return changes;
	}
};

} // namespace

void RegisterGlueGrammarExtension(DatabaseInstance &db) {
	GrammarExtension::Register(db, make_shared_ptr<GlueHiveDDLGrammar>());
}

} // namespace duckdb
