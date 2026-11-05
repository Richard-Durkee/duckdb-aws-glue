#include "core/glue_trace.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/extension_callback.hpp"

#include <algorithm>
#include <map>
#include <sstream>

namespace duckdb {

static constexpr const char *GLUE_TRACE_SETTING = "glue_trace";

atomic<idx_t> glue_trace_active_connections {0};

const char *GlueQueryPhaseName(GlueQueryPhase phase) {
	switch (phase) {
	case GlueQueryPhase::BIND:
		return "bind";
	case GlueQueryPhase::OPTIMIZE:
		return "optimize";
	case GlueQueryPhase::PHYSICAL_PLAN:
		return "physical_plan";
	case GlueQueryPhase::EXECUTE:
		return "execute";
	default:
		return "none";
	}
}

//===--------------------------------------------------------------------===//
// Tags
//===--------------------------------------------------------------------===//
static thread_local GlueTraceTag *current_tag = nullptr;

GlueTraceTag::GlueTraceTag(const char *purpose_p, const string &database, const string &table_p)
    : purpose(purpose_p), parent(current_tag) {
	if (!table_p.empty()) {
		table = database.empty() ? table_p : database + "." + table_p;
	}
	current_tag = this;
}

GlueTraceTag::GlueTraceTag(const Snapshot &snapshot_p)
    : purpose(nullptr), table(snapshot_p.table), snapshot(&snapshot_p), parent(current_tag) {
	current_tag = this;
}

GlueTraceTag::~GlueTraceTag() {
	current_tag = parent.get();
}

string GlueTraceTag::CurrentPurpose() {
	vector<string> parts;
	for (auto tag = current_tag; tag; tag = tag->parent.get()) {
		if (tag->snapshot) {
			if (!tag->snapshot->purpose.empty()) {
				parts.push_back(tag->snapshot->purpose);
			}
			break;
		}
		parts.push_back(tag->purpose);
	}
	std::reverse(parts.begin(), parts.end());
	return StringUtil::Join(parts, "/");
}

string GlueTraceTag::CurrentTable() {
	for (auto tag = current_tag; tag; tag = tag->parent.get()) {
		if (!tag->table.empty()) {
			return tag->table;
		}
		if (tag->snapshot) {
			break;
		}
	}
	return string();
}

GlueTraceTag::Snapshot GlueTraceTag::Capture() {
	return Snapshot {CurrentPurpose(), CurrentTable()};
}

//===--------------------------------------------------------------------===//
// HTTP counters
//===--------------------------------------------------------------------===//
//! httpfs keeps per connection request counters in its 'http_state', reset at the end of every statement. It publishes
//! them only as the text of its profiler box, so they are read from there.
static idx_t ParseCounter(const string &text, const char *label) {
	auto pos = text.find(label);
	if (pos == string::npos) {
		return 0;
	}
	pos += strlen(label);
	idx_t value = 0;
	while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9') {
		value = value * 10 + idx_t(text[pos] - '0');
		pos++;
	}
	return value;
}

static GlueHTTPCounters ReadHTTPCounters(ClientContext &context) {
	GlueHTTPCounters result;
	result.valid = true;
	auto state = context.registered_state->Get<ClientContextState>("http_state");
	if (!state) {
		return result;
	}
	std::stringstream ss;
	state->WriteProfilingInformation(ss);
	auto text = ss.str();
	result.head = ParseCounter(text, "#HEAD: ");
	result.get = ParseCounter(text, "#GET: ");
	result.put = ParseCounter(text, "#PUT: ");
	result.post = ParseCounter(text, "#POST: ");
	result.del = ParseCounter(text, "#DELETE: ");
	return result;
}

static GlueHTTPCounters Delta(const GlueHTTPCounters &start, const GlueHTTPCounters &end) {
	GlueHTTPCounters result;
	// httpfs resets its counters at the end of a statement; a drop means that happened first
	result.valid = start.valid && end.valid && end.head >= start.head && end.get >= start.get && end.put >= start.put &&
	               end.post >= start.post && end.del >= start.del;
	if (result.valid) {
		result.head = end.head - start.head;
		result.get = end.get - start.get;
		result.put = end.put - start.put;
		result.post = end.post - start.post;
		result.del = end.del - start.del;
	}
	return result;
}

//===--------------------------------------------------------------------===//
// GlueTraceState
//===--------------------------------------------------------------------===//
shared_ptr<GlueTraceState> GlueTraceState::GetActive(ClientContext &context) {
	if (glue_trace_active_connections.load(std::memory_order_relaxed) == 0) {
		return nullptr;
	}
	auto state = context.registered_state->Get<GlueTraceState>(NAME);
	if (!state || !state->Active()) {
		return nullptr;
	}
	return state;
}

void GlueTraceState::Register(ClientContext &context) {
	if (context.registered_state->Get<GlueTraceState>(NAME)) {
		return;
	}
	context.registered_state->GetOrCreate<GlueTraceState>(NAME);
	glue_trace_active_connections++;
}

double GlueTraceState::ElapsedMs() const {
	return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - query_start).count();
}

void GlueTraceState::QueryBegin(ClientContext &context) {
	Value value;
	bool enabled =
	    context.TryGetCurrentSetting(GLUE_TRACE_SETTING, value) && !value.IsNull() && BooleanValue::Get(value);
	if (!enabled) {
		active = false;
		return;
	}
	lock_guard<mutex> guard(lock);
	current = GlueTraceQuery();
	current.query = context.GetCurrentQuery();
	if (context.transaction.HasActiveTransaction()) {
		current.query_id = context.transaction.GetActiveQuery();
	}
	trace_reader = false;
	latest_counters = GlueHTTPCounters();
	query_start = std::chrono::steady_clock::now();
	phase = static_cast<uint8_t>(GlueQueryPhase::NONE);
	EnterPhaseLocked(context, GlueQueryPhase::BIND);
	active = true;
}

void GlueTraceState::EnterPhaseLocked(ClientContext &context, GlueQueryPhase next) {
	auto now_ms = ElapsedMs();
	auto counters = ReadHTTPCounters(context);
	if (!current.phases.empty()) {
		auto &previous = current.phases.back();
		previous.duration_ms = now_ms - previous.start_ms;
		previous.http = Delta(previous.http_start, counters);
		if (!previous.http.valid && next == GlueQueryPhase::NONE && latest_counters.valid) {
			previous.http = Delta(previous.http_start, latest_counters);
		}
	}
	if (next != GlueQueryPhase::NONE) {
		GlueTracePhase entry;
		entry.phase = next;
		entry.start_time = Timestamp::GetCurrentTimestamp();
		entry.start_ms = now_ms;
		entry.http_start = counters;
		current.phases.push_back(std::move(entry));
	}
	phase = static_cast<uint8_t>(next);
}

void GlueTraceState::Advance(ClientContext &context, GlueQueryPhase from, GlueQueryPhase next) {
	if (!Active() || Phase() != from) {
		return;
	}
	lock_guard<mutex> guard(lock);
	if (Phase() != from) {
		return;
	}
	EnterPhaseLocked(context, next);
}

void GlueTraceState::OnTaskStart(ClientContext &context) {
	if (!Active() || own_tasks.load() > 0 || Phase() != GlueQueryPhase::PHYSICAL_PLAN) {
		return;
	}
	Advance(context, GlueQueryPhase::PHYSICAL_PLAN, GlueQueryPhase::EXECUTE);
}

void GlueTraceState::OnTaskStop(ClientContext &context) {
	if (!Active() || Phase() != GlueQueryPhase::EXECUTE) {
		return;
	}
	unique_lock<mutex> guard(lock, std::try_to_lock);
	if (!guard.owns_lock()) {
		return;
	}
	latest_counters = ReadHTTPCounters(context);
}

GlueTraceOwnTasks::GlueTraceOwnTasks(ClientContext &context) : trace(GlueTraceState::GetActive(context)) {
	if (trace) {
		trace->own_tasks++;
	}
}

GlueTraceOwnTasks::~GlueTraceOwnTasks() {
	if (trace) {
		trace->own_tasks--;
	}
}

void GlueTraceState::Record(GlueTraceRequest request) {
	lock_guard<mutex> guard(lock);
	current.requests.push_back(std::move(request));
}

void GlueTraceState::Summarize(GlueTraceQuery &query) {
	for (auto &phase : query.phases) {
		std::map<string, idx_t> operations;
		vector<pair<double, double>> intervals;
		phase.glue_requests = 0;
		phase.glue_ms = 0;
		for (auto &request : query.requests) {
			if (request.phase != phase.phase) {
				continue;
			}
			phase.glue_requests++;
			phase.glue_ms += request.duration_ms;
			operations[request.operation]++;
			intervals.emplace_back(request.start_ms, request.start_ms + request.duration_ms);
		}
		// the time at least one request was in flight: what the phase waited on Glue at most
		std::sort(intervals.begin(), intervals.end());
		double wall = 0;
		double covered_until = -1;
		for (auto &interval : intervals) {
			auto begin = MaxValue(interval.first, covered_until);
			if (interval.second > begin) {
				wall += interval.second - begin;
				covered_until = interval.second;
			}
		}
		phase.glue_wall_ms = wall;
		vector<string> parts;
		for (auto &entry : operations) {
			parts.push_back(entry.first + ":" + to_string(entry.second));
		}
		phase.glue_operations = StringUtil::Join(parts, " ");
	}
}

void GlueTraceState::QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) {
	if (!Active()) {
		return;
	}
	lock_guard<mutex> guard(lock);
	EnterPhaseLocked(context, GlueQueryPhase::NONE);
	Summarize(current);
	for (auto &phase : current.phases) {
		DUCKDB_LOG(context, GlueTraceLogType, phase, current.query_id);
	}
	if (!trace_reader) {
		last = std::move(current);
	}
	current = GlueTraceQuery();
	active = false;
}

GlueTraceQuery GlueTraceState::LastQuery() {
	lock_guard<mutex> guard(lock);
	return last;
}

static string FormatMs(double ms) {
	return StringUtil::Format("%.1fms", ms);
}

void GlueTraceState::WriteProfilingInformation(std::ostream &ss) {
	GlueTraceQuery query;
	{
		lock_guard<mutex> guard(lock);
		if (Active()) {
			// EXPLAIN ANALYZE renders while the statement runs: close the open phase at now
			query = current;
			if (!query.phases.empty()) {
				query.phases.back().duration_ms = ElapsedMs() - query.phases.back().start_ms;
			}
		} else {
			query = last;
		}
	}
	if (query.phases.empty()) {
		return;
	}
	Summarize(query);
	ss << "Glue Trace (per phase: wall time | Glue requests, summed and "
	      "in-flight time | other HTTP requests)\n";
	for (auto &phase : query.phases) {
		ss << StringUtil::Format("  %-13s %10s | glue %3llu  %10s  %10s  %s", GlueQueryPhaseName(phase.phase),
		                         FormatMs(phase.duration_ms), phase.glue_requests, FormatMs(phase.glue_ms),
		                         FormatMs(phase.glue_wall_ms), phase.glue_operations);
		if (phase.http.valid) {
			ss << StringUtil::Format(" | http GET %llu HEAD %llu PUT %llu DELETE %llu", phase.http.get, phase.http.head,
			                         phase.http.put, phase.http.del);
		}
		ss << "\n";
	}
}

//===--------------------------------------------------------------------===//
// Log type
//===--------------------------------------------------------------------===//
constexpr LogLevel GlueTraceLogType::LEVEL;

GlueTraceLogType::GlueTraceLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType GlueTraceLogType::GetLogType() {
	child_list_t<LogicalType> children = {
	    {"trace_query_id", LogicalType::UBIGINT}, {"phase", LogicalType::VARCHAR},
	    {"start_time", LogicalType::TIMESTAMP},   {"duration_ms", LogicalType::DOUBLE},
	    {"glue_requests", LogicalType::UBIGINT},  {"glue_ms", LogicalType::DOUBLE},
	    {"glue_wall_ms", LogicalType::DOUBLE},    {"glue_operations", LogicalType::VARCHAR},
	    {"http_get", LogicalType::UBIGINT},       {"http_head", LogicalType::UBIGINT},
	    {"http_put", LogicalType::UBIGINT},       {"http_post", LogicalType::UBIGINT},
	    {"http_delete", LogicalType::UBIGINT}};
	return LogicalType::STRUCT(children);
}

static Value HTTPCount(const GlueHTTPCounters &counters, idx_t value) {
	return counters.valid ? Value::UBIGINT(value) : Value(LogicalType::UBIGINT);
}

string GlueTraceLogType::ConstructLogMessage(const GlueTracePhase &phase, optional_idx query_id) {
	child_list_t<Value> children = {
	    {"trace_query_id", query_id.IsValid() ? Value::UBIGINT(query_id.GetIndex()) : Value(LogicalType::UBIGINT)},
	    {"phase", Value(GlueQueryPhaseName(phase.phase))},
	    {"start_time", Value::TIMESTAMP(phase.start_time)},
	    {"duration_ms", Value::DOUBLE(phase.duration_ms)},
	    {"glue_requests", Value::UBIGINT(phase.glue_requests)},
	    {"glue_ms", Value::DOUBLE(phase.glue_ms)},
	    {"glue_wall_ms", Value::DOUBLE(phase.glue_wall_ms)},
	    {"glue_operations", Value(phase.glue_operations)},
	    {"http_get", HTTPCount(phase.http, phase.http.get)},
	    {"http_head", HTTPCount(phase.http, phase.http.head)},
	    {"http_put", HTTPCount(phase.http, phase.http.put)},
	    {"http_post", HTTPCount(phase.http, phase.http.post)},
	    {"http_delete", HTTPCount(phase.http, phase.http.del)}};
	return Value::STRUCT(std::move(children)).ToString();
}

//===--------------------------------------------------------------------===//
// Hooks
//===--------------------------------------------------------------------===//
static void GlueTraceSettingChanged(ClientContext &context, SetScope scope, Value &parameter) {
	if (!parameter.IsNull() && BooleanValue::Get(parameter)) {
		GlueTraceState::Register(context);
	}
}

class GlueTraceConnectionCallback : public ExtensionCallback {
public:
	void OnConnectionOpened(ClientContext &context) override {
		Value value;
		if (context.db->TryGetCurrentSetting(GLUE_TRACE_SETTING, value) && !value.IsNull() &&
		    BooleanValue::Get(value)) {
			GlueTraceState::Register(context);
		}
	}
};

static void GlueTracePreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &) {
	auto state = GlueTraceState::GetActive(input.context);
	if (state) {
		state->Advance(input.context, GlueQueryPhase::BIND, GlueQueryPhase::OPTIMIZE);
	}
}

static void GlueTracePostOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &) {
	auto state = GlueTraceState::GetActive(input.context);
	if (state) {
		state->Advance(input.context, GlueQueryPhase::OPTIMIZE, GlueQueryPhase::PHYSICAL_PLAN);
	}
}

//===--------------------------------------------------------------------===//
// glue_trace() / glue_trace_phases()
//===--------------------------------------------------------------------===//
namespace {

struct GlueTraceBindData : public TableFunctionData {
	GlueTraceQuery query;
};

struct GlueTraceScanState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

GlueTraceQuery ReadLastTrace(ClientContext &context) {
	auto state = context.registered_state->Get<GlueTraceState>(GlueTraceState::NAME);
	if (!state) {
		return GlueTraceQuery();
	}
	state->MarkTraceReader();
	auto query = state->LastQuery();
	return query;
}

unique_ptr<FunctionData> GlueTraceBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueTraceBindData>();
	result->query = ReadLastTrace(context);
	names = {"seq",         "phase",  "operation", "target",       "purpose",       "start_time",     "start_ms",
	         "duration_ms", "status", "attempt",   "continuation", "request_bytes", "response_bytes", "error"};
	return_types = {LogicalType::UBIGINT, LogicalType::VARCHAR,   LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::DOUBLE,  LogicalType::DOUBLE,
	                LogicalType::UBIGINT, LogicalType::UBIGINT,   LogicalType::BOOLEAN, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GlueTraceInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GlueTraceScanState>();
}

Value NullIfEmpty(const string &value) {
	return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
}

void GlueTraceScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueTraceScanState>();
	auto &requests = data.bind_data->Cast<GlueTraceBindData>().query.requests;
	idx_t count = 0;
	for (; state.offset < requests.size() && count < STANDARD_VECTOR_SIZE; state.offset++, count++) {
		auto &request = requests[state.offset];
		output.data[0].Append(Value::UBIGINT(state.offset + 1));
		output.data[1].Append(Value(GlueQueryPhaseName(request.phase)));
		output.data[2].Append(NullIfEmpty(request.operation));
		output.data[3].Append(NullIfEmpty(request.target));
		output.data[4].Append(NullIfEmpty(request.purpose));
		output.data[5].Append(Value::TIMESTAMP(request.start_time));
		output.data[6].Append(Value::DOUBLE(request.start_ms));
		output.data[7].Append(Value::DOUBLE(request.duration_ms));
		output.data[8].Append(request.status.IsValid() ? Value::UBIGINT(request.status.GetIndex())
		                                               : Value(LogicalType::UBIGINT));
		output.data[9].Append(Value::UBIGINT(request.attempt));
		output.data[10].Append(Value::BOOLEAN(request.continuation));
		output.data[11].Append(Value::UBIGINT(request.request_bytes));
		output.data[12].Append(Value::UBIGINT(request.response_bytes));
		output.data[13].Append(NullIfEmpty(request.error));
	}
	output.CheckCardinality(count);
}

unique_ptr<FunctionData> GlueTracePhasesBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GlueTraceBindData>();
	result->query = ReadLastTrace(context);
	names = {"phase",           "start_time", "start_ms",  "duration_ms", "glue_requests", "glue_ms",    "glue_wall_ms",
	         "glue_operations", "http_get",   "http_head", "http_put",    "http_post",     "http_delete"};
	return_types = {LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::DOUBLE,  LogicalType::DOUBLE,
	                LogicalType::UBIGINT, LogicalType::DOUBLE,    LogicalType::DOUBLE,  LogicalType::VARCHAR,
	                LogicalType::UBIGINT, LogicalType::UBIGINT,   LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT};
	return std::move(result);
}

void GlueTracePhasesScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GlueTraceScanState>();
	auto &phases = data.bind_data->Cast<GlueTraceBindData>().query.phases;
	idx_t count = 0;
	for (; state.offset < phases.size() && count < STANDARD_VECTOR_SIZE; state.offset++, count++) {
		auto &phase = phases[state.offset];
		output.data[0].Append(Value(GlueQueryPhaseName(phase.phase)));
		output.data[1].Append(Value::TIMESTAMP(phase.start_time));
		output.data[2].Append(Value::DOUBLE(phase.start_ms));
		output.data[3].Append(Value::DOUBLE(phase.duration_ms));
		output.data[4].Append(Value::UBIGINT(phase.glue_requests));
		output.data[5].Append(Value::DOUBLE(phase.glue_ms));
		output.data[6].Append(Value::DOUBLE(phase.glue_wall_ms));
		output.data[7].Append(NullIfEmpty(phase.glue_operations));
		output.data[8].Append(HTTPCount(phase.http, phase.http.get));
		output.data[9].Append(HTTPCount(phase.http, phase.http.head));
		output.data[10].Append(HTTPCount(phase.http, phase.http.put));
		output.data[11].Append(HTTPCount(phase.http, phase.http.post));
		output.data[12].Append(HTTPCount(phase.http, phase.http.del));
	}
	output.CheckCardinality(count);
}

} // namespace

void RegisterGlueTrace(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption(GLUE_TRACE_SETTING,
	                          "Trace the Glue requests of every statement on "
	                          "this connection by phase (bind, "
	                          "optimize, physical_plan, execute): read with "
	                          "glue_trace() / glue_trace_phases() after "
	                          "the statement, in EXPLAIN ANALYZE, and in the "
	                          "GlueTrace log. Default false.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), GlueTraceSettingChanged);
	db.GetLogManager().RegisterLogType(make_uniq<GlueTraceLogType>());
	ExtensionCallback::Register(config, make_shared_ptr<GlueTraceConnectionCallback>());

	OptimizerExtension pre;
	pre.pre_optimize_function = GlueTracePreOptimize;
	OptimizerExtension::Register(config, std::move(pre));
	OptimizerExtension post;
	post.optimize_function = GlueTracePostOptimize;
	OptimizerExtension::Register(config, std::move(post));

	TableFunction trace("glue_trace", {}, GlueTraceScan, GlueTraceBind, GlueTraceInit);
	loader.RegisterFunction(trace);
	TableFunction phases("glue_trace_phases", {}, GlueTracePhasesScan, GlueTracePhasesBind, GlueTraceInit);
	loader.RegisterFunction(phases);
}

} // namespace duckdb
