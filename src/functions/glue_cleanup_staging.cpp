#include "functions/glue_cleanup_staging.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"

#include "execution/glue_hive_write.hpp"
#include "functions/glue_functions.hpp"

namespace duckdb {

namespace {

struct GlueCleanupStagingBindData : public TableFunctionData {
	string root;
	int64_t older_than_micros = Interval::MICROS_PER_DAY;

	bool SupportStatementCache() const override {
		return false;
	}
};

struct CleanedStagingRoot {
	string staging_root;
	timestamp_t written_at;
	bool rolled_back;
	idx_t deleted_files;
};

struct GlueCleanupStagingState : public GlobalTableFunctionState {
	vector<CleanedStagingRoot> cleaned;
	idx_t offset = 0;
	bool applied = false;
};

unique_ptr<FunctionData> GlueCleanupStagingBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueCleanupStagingBindData>();
	auto target = ResolveGlueTable(context, "glue_cleanup_staging", input.inputs[0], false);
	result->root = target.table.location;
	StringUtil::RTrim(result->root, "/");
	if (result->root.empty()) {
		throw InvalidInputException("Table '%s' has no location in Glue", target.TableName());
	}
	for (auto &option : input.named_parameters) {
		if (StringUtil::Lower(option.first.GetIdentifierName()) != "older_than" || option.second.IsNull()) {
			continue;
		}
		result->older_than_micros = Interval::GetMicro(option.second.GetValue<interval_t>());
		if (result->older_than_micros < 0) {
			throw InvalidInputException("glue_cleanup_staging: older_than must not be negative");
		}
	}
	names.emplace_back("staging_root");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("written_at");
	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("rolled_back");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("deleted_files");
	return_types.emplace_back(LogicalType::BIGINT);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueCleanupStagingInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlueCleanupStagingState>();
}

//! The time a UUIDv7 write id was generated, from its first 48 bits; false for any other name
bool WriteIdTime(const string &write_id, timestamp_t &result) {
	if (write_id.size() != 36 || write_id[8] != '-' || write_id[14] != '7') {
		return false;
	}
	auto hex = write_id.substr(0, 8) + write_id.substr(9, 4);
	int64_t millis = 0;
	for (auto c : hex) {
		int64_t digit;
		if (c >= '0' && c <= '9') {
			digit = c - '0';
		} else if (c >= 'a' && c <= 'f') {
			digit = c - 'a' + 10;
		} else {
			return false;
		}
		millis = millis * 16 + digit;
	}
	result = Timestamp::FromEpochMs(millis);
	return true;
}

vector<string> ReadJournal(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto size = NumericCast<idx_t>(handle->GetFileSize());
	string contents(size, '\0');
	handle->Read(&contents[0], size);
	vector<string> result;
	for (auto &line : StringUtil::Split(contents, '\n')) {
		if (!line.empty()) {
			result.push_back(line);
		}
	}
	return result;
}

void GlueCleanupStagingApply(ClientContext &context, const GlueCleanupStagingBindData &bind_data,
                             GlueCleanupStagingState &state) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto root_prefix = bind_data.root + "/";
	// one listing of the keys that start with the staging prefix
	map<string, vector<string>> roots;
	for (auto &file : fs.Glob(root_prefix + STAGING_DIRECTORY_PREFIX + "*/**")) {
		if (FileSystem::IsDirectory(file)) {
			continue;
		}
		auto relative = file.path.substr(root_prefix.size());
		auto separator = relative.find('/');
		if (separator == string::npos) {
			continue;
		}
		roots[relative.substr(0, separator)].push_back(file.path);
	}
	auto now = Timestamp::GetCurrentTimestamp();
	for (auto &entry : roots) {
		auto &directory = entry.first;
		auto &files = entry.second;
		timestamp_t written_at;
		if (!WriteIdTime(directory.substr(strlen(STAGING_DIRECTORY_PREFIX)), written_at)) {
			continue;
		}
		if (Timestamp::GetEpochMicroSeconds(now) - Timestamp::GetEpochMicroSeconds(written_at) <
		    bind_data.older_than_micros) {
			// most likely a write that is still running
			continue;
		}
		auto staging_root = root_prefix + directory;
		auto journal = staging_root + "/" + STAGING_JOURNAL_NAME;
		vector<string> to_delete;
		bool rolled_back = false;
		if (std::find(files.begin(), files.end(), journal) != files.end()) {
			// a commit that did not finish: undo what it published
			rolled_back = true;
			for (auto &final_path : ReadJournal(fs, journal)) {
				if (fs.FileExists(final_path)) {
					to_delete.push_back(final_path);
				}
			}
		}
		// the journal last: a cleanup that fails part way still finds the commit to undo
		for (auto &file : files) {
			if (file != journal) {
				to_delete.push_back(file);
			}
		}
		if (rolled_back) {
			to_delete.push_back(journal);
		}
		if (!to_delete.empty()) {
			fs.RemoveFiles(to_delete);
		}
		state.cleaned.push_back({staging_root, written_at, rolled_back, to_delete.size()});
	}
}

void GlueCleanupStagingScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<GlueCleanupStagingBindData>();
	auto &state = data.global_state->Cast<GlueCleanupStagingState>();
	if (!state.applied) {
		state.applied = true;
		GlueCleanupStagingApply(context, bind_data, state);
	}
	idx_t count = 0;
	while (state.offset < state.cleaned.size() && count < STANDARD_VECTOR_SIZE) {
		auto &cleaned = state.cleaned[state.offset++];
		output.data[0].Append(Value(cleaned.staging_root));
		output.data[1].Append(Value::TIMESTAMP(cleaned.written_at));
		output.data[2].Append(Value::BOOLEAN(cleaned.rolled_back));
		output.data[3].Append(Value::BIGINT(NumericCast<int64_t>(cleaned.deleted_files)));
		count++;
	}
}

} // namespace

TableFunction GetGlueCleanupStagingFunction() {
	TableFunction function("glue_cleanup_staging", {LogicalType::VARCHAR}, GlueCleanupStagingScan,
	                       GlueCleanupStagingBind, GlueCleanupStagingInit);
	function.GetSignature().WithTypedKwargs(
	    "options", [](TypedKwargs &options) { options.Add("older_than", LogicalType::INTERVAL); });
	return function;
}

} // namespace duckdb
