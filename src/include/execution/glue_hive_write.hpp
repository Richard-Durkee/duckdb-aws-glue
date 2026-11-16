#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string.hpp"

#include "api/glue_api.hpp"

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! What an INSERT or CREATE TABLE AS into a Hive table changed, so that a failed statement can be taken back
struct GlueHiveWrite {
	string write_id;
	string database_name;
	string table_name;
	//! The files of the write are named <write_id>_..., below this location
	string location;
	//! CREATE TABLE AS created the table, and the client to drop it with (the rollback runs when the transaction has
	//! ended, too late to look up the secret)
	bool created_table = false;
	std::shared_ptr<Aws::Glue::GlueClient> client;
	//! The statement deleted its files itself (a failed partition registration does), so the undo need not look
	bool files_deleted = false;
	//! CREATE TABLE AS sent the change that removes the write id from its table, which Glue may have applied: the
	//! VersionId and CreateTime (in milliseconds) of the table that change was sent for
	bool removal_sent = false;
	string removal_version_id;
	optional_idx removal_create_time;

public:
	//! Whether 'table' is the one CREATE TABLE AS created: it carries the write id or, once the change that removes it
	//! was sent, was created when that table was and has been changed since
	bool OwnsTable(const GlueTableInfo &table) const;
	//! Throws if 'table' is not the one CREATE TABLE AS created: another client dropped it and created it again
	void ThrowIfNotOwnTable(const GlueTableInfo &table) const;
	//! Undo a write whose statement failed: delete its files and, for CREATE TABLE AS, drop the table it created.
	//! Best effort: a step that fails is logged
	void Undo(ClientContext &context, GlueCatalog &catalog);
};

} // namespace duckdb
