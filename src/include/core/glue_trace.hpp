#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/main/client_context_state.hpp"

#include <chrono>

namespace duckdb {
class ExtensionLoader;

//! The phase of the statement a request was sent in. BIND covers parsing, binding and logical planning; the transitions
//! are QueryBegin, the optimizer extension hooks before and after the built-in optimizers, and the first executor task
//! after the plan is built.
enum class GlueQueryPhase : uint8_t { NONE = 0, BIND, OPTIMIZE, PHYSICAL_PLAN, EXECUTE };
const char *GlueQueryPhaseName(GlueQueryPhase phase);

//! What a request is for: a thread local stack of tags set around the code that sends Glue requests
class GlueTraceTag {
public:
	GlueTraceTag(const char *purpose, const string &database = string(), const string &table = string());
	~GlueTraceTag();

	//! 'catalog lookup/insert: existing partitions', or empty
	static string CurrentPurpose();
	//! The innermost table a tag names, or empty
	static string CurrentTable();

	//! Copy of the current tags, to hand to a task on another thread
	struct Snapshot {
		string purpose;
		string table;
	};
	static Snapshot Capture();
	explicit GlueTraceTag(const Snapshot &snapshot);

private:
	const char *purpose;
	string table;
	optional_ptr<const Snapshot> snapshot;
	optional_ptr<GlueTraceTag> parent;
};

struct GlueTraceRequest {
	GlueQueryPhase phase = GlueQueryPhase::NONE;
	string operation;
	string target;
	string purpose;
	timestamp_t start_time;
	//! steady clock, relative to the start of the statement
	double start_ms = 0;
	double duration_ms = 0;
	optional_idx status;
	idx_t attempt = 1;
	bool continuation = false;
	idx_t request_bytes = 0;
	idx_t response_bytes = 0;
	string error;
};

//! Request counters of the HTTP layer (httpfs) for this connection; 'valid' is false when httpfs keeps none
struct GlueHTTPCounters {
	bool valid = false;
	idx_t head = 0;
	idx_t get = 0;
	idx_t put = 0;
	idx_t post = 0;
	idx_t del = 0;
};

struct GlueTracePhase {
	GlueQueryPhase phase = GlueQueryPhase::NONE;
	timestamp_t start_time;
	double start_ms = 0;
	double duration_ms = 0;
	idx_t glue_requests = 0;
	//! sum of the request durations, and the time at least one Glue request was in flight
	double glue_ms = 0;
	double glue_wall_ms = 0;
	string glue_operations;
	GlueHTTPCounters http_start;
	//! HTTP requests other than the Glue ones, sent in this phase (NULL counts when httpfs reported none)
	GlueHTTPCounters http;
};

struct GlueTraceQuery {
	//! the query_id of the statement's log entries: GlueTrace entries are written at its end, when the log context no
	//! longer carries it
	optional_idx query_id;
	string query;
	vector<GlueTraceRequest> requests;
	vector<GlueTracePhase> phases;
};

//! Per connection request tracing, enabled with SET glue_trace = true
class GlueTraceState : public ClientContextState {
public:
	static constexpr const char *NAME = "glue_trace";

	//! The state of an active trace on this connection, or nullptr
	static shared_ptr<GlueTraceState> GetActive(ClientContext &context);
	static void Register(ClientContext &context);

	void QueryBegin(ClientContext &context) override;
	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override;
	void OnTaskStart(ClientContext &context) override;
	void OnTaskStop(ClientContext &context) override;
	void WriteProfilingInformation(std::ostream &ss) override;

	bool Active() const {
		return active.load(std::memory_order_relaxed);
	}
	GlueQueryPhase Phase() const {
		return static_cast<GlueQueryPhase>(phase.load(std::memory_order_relaxed));
	}
	//! Move to 'next' when the statement is in 'from'
	void Advance(ClientContext &context, GlueQueryPhase from, GlueQueryPhase next);
	double ElapsedMs() const;
	void Record(GlueTraceRequest request);
	//! The previous traced statement of this connection
	GlueTraceQuery LastQuery();
	//! The statement that reads the trace does not replace it
	void MarkTraceReader() {
		trace_reader = true;
	}
	//! Executor tasks the extension starts itself (parallel GetPartitions) are not the start of execution
	atomic<idx_t> own_tasks {0};

private:
	void EnterPhaseLocked(ClientContext &context, GlueQueryPhase next);
	static void Summarize(GlueTraceQuery &query);

private:
	atomic<bool> active {false};
	atomic<uint8_t> phase {0};
	bool trace_reader = false;
	std::chrono::steady_clock::time_point query_start;
	mutex lock;
	GlueTraceQuery current;
	GlueTraceQuery last;
	//! httpfs resets its counters in its own QueryEnd, which may run before ours: the counters as of the last task
	GlueHTTPCounters latest_counters;
};

//! Marks the executor tasks the extension starts itself while it is alive
class GlueTraceOwnTasks {
public:
	explicit GlueTraceOwnTasks(ClientContext &context);
	~GlueTraceOwnTasks();

private:
	shared_ptr<GlueTraceState> trace;
};

class GlueTraceLogType : public LogType {
public:
	static constexpr const char *NAME = "GlueTrace";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	GlueTraceLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(const GlueTracePhase &phase, optional_idx query_id);
};

//! Number of connections with an active trace: lets the request path skip the state lookup when nobody traces
extern atomic<idx_t> glue_trace_active_connections;

void RegisterGlueTrace(ExtensionLoader &loader);

} // namespace duckdb
