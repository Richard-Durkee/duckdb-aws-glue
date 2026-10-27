#include "api/glue_http_client.hpp"

#include "duckdb/common/http_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/http/http_transport_manager.hpp"

#include "core/glue_log_types.hpp"
#include "core/glue_trace.hpp"

#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#ifdef _WIN32
#include <aws/core/http/windows/WinHttpSyncHttpClient.h>
#else
#include <aws/core/http/curl/CurlHttpClient.h>
#endif

#include <mutex>
#include <sstream>

// Bridge between the AWS SDK and DuckDB's HTTP layer, modeled after duckdb-aws PR #173: an aws-sdk-cpp
// HttpClientFactory whose HttpClient forwards every request to HTTPUtil::Get(db). The concrete transport is whatever
// the DatabaseInstance has registered (httpfs's curl client on native). Requests go through a transport manager
// session rather than the client directly, so that DuckDB's connection pool, retry and HTTP logging all apply.

namespace duckdb {

static constexpr const char *NETWORK_VIA_DUCKDB_SETTING = "glue_network_calls_via_duckdb";

//===--------------------------------------------------------------------===//
// Context scope
//===--------------------------------------------------------------------===//
static thread_local optional_ptr<ClientContext> current_glue_context;

GlueHttpClientContextScope::GlueHttpClientContextScope(ClientContext &context) : previous(current_glue_context) {
	current_glue_context = &context;
}

GlueHttpClientContextScope::~GlueHttpClientContextScope() {
	current_glue_context = previous;
}

optional_ptr<ClientContext> GlueHttpClientContextScope::Current() {
	return current_glue_context;
}

namespace {

//! Read the toggle, preferring the calling connection (a plain SET is session scoped) over the database settings.
//! Defaults to true.
bool NetworkCallsViaDuckDB(DatabaseInstance &db) {
	Value value;
	auto context = GlueHttpClientContextScope::Current();
	if (context && context->TryGetCurrentSetting(NETWORK_VIA_DUCKDB_SETTING, value) && !value.IsNull()) {
		return BooleanValue::Get(value);
	}
	if (db.TryGetCurrentSetting(NETWORK_VIA_DUCKDB_SETTING, value) && !value.IsNull()) {
		return BooleanValue::Get(value);
	}
	return true;
}

RequestType ToDuckDBRequestType(Aws::Http::HttpMethod method) {
	switch (method) {
	case Aws::Http::HttpMethod::HTTP_GET:
		return RequestType::GET_REQUEST;
	case Aws::Http::HttpMethod::HTTP_PUT:
		return RequestType::PUT_REQUEST;
	case Aws::Http::HttpMethod::HTTP_HEAD:
		return RequestType::HEAD_REQUEST;
	case Aws::Http::HttpMethod::HTTP_DELETE:
		return RequestType::DELETE_REQUEST;
	default:
		// Glue (JSON protocol) and STS (query protocol) both POST
		return RequestType::POST_REQUEST;
	}
}

//! HTTPUtil::DecomposeURL requires a '/' after the authority. The AWS SDK serializes some endpoints as a bare host
//! ("https://glue.eu-central-1.amazonaws.com"), so add the missing '/'. SigV4 canonicalizes an empty path to "/"
//! as well, so this stays consistent with what was signed.
string EnsureUrlHasPath(string url) {
	auto scheme_pos = url.find("://");
	idx_t authority_start = (scheme_pos == string::npos) ? 0 : scheme_pos + 3;
	auto sep_pos = url.find_first_of("/?#", authority_start);
	if (sep_pos == string::npos) {
		return url + "/";
	}
	if (url[sep_pos] != '/') {
		url.insert(sep_pos, "/");
	}
	return url;
}

//! Read the request body fully. The SDK already read the stream to hash it for the SigV4 payload signature, so
//! rewind before reading and again afterwards.
string ReadRequestBody(const std::shared_ptr<Aws::Http::HttpRequest> &request) {
	const auto &body = request->GetContentBody();
	if (!body) {
		return string();
	}
	body->clear();
	body->seekg(0, std::ios_base::beg);
	std::stringstream ss;
	ss << body->rdbuf();
	body->clear();
	body->seekg(0, std::ios_base::beg);
	return ss.str();
}

class GlueDuckDBHttpClient : public Aws::Http::HttpClient {
public:
	explicit GlueDuckDBHttpClient(DatabaseInstance &db_p) : db(db_p) {
	}

	std::shared_ptr<Aws::Http::HttpResponse>
	MakeRequest(const std::shared_ptr<Aws::Http::HttpRequest> &request, Aws::Utils::RateLimits::RateLimiterInterface *,
	            Aws::Utils::RateLimits::RateLimiterInterface *) const override {
		auto aws_response = Aws::MakeShared<Aws::Http::Standard::StandardHttpResponse>("GlueDuckDBHttp", request);
		GlueLogTimer timer;
		auto start_time = Timestamp::GetCurrentTimestamp();
		string body_buffer;
		try {
			string url = EnsureUrlHasPath(request->GetUri().GetURIString(true).c_str());

			// Pooled session; the calling connection's parameters carry its logger, so the request shows in that
			// connection's HTTP log. Calls outside a GlueHttpClientContextScope (e.g. credential refreshes) fall
			// back to the database level parameters.
			auto context = GlueHttpClientContextScope::Current();
			auto &manager = DBConfig::GetConfig(db).GetHTTPTransportManager();
			auto session = context ? manager.CreateSession(*context, url) : manager.CreateSession(db, url);
			auto &params = session.Parameters();

			HTTPHeaders headers(db);
			for (const auto &header : request->GetHeaders()) {
				// DuckDB's client sets these itself, from the URL and the body
				auto lower = StringUtil::Lower(header.first.c_str());
				if (lower == "host" || lower == "content-length") {
					continue;
				}
				headers.Insert(header.first.c_str(), header.second.c_str());
			}

			unique_ptr<HTTPResponse> response;
			switch (ToDuckDBRequestType(request->GetMethod())) {
			case RequestType::GET_REQUEST: {
				GetRequestInfo info(
				    url, headers, params, [](const HTTPResponse &) { return true; },
				    [&](const_data_ptr_t data, idx_t len) {
					    aws_response->GetResponseBody().write(const_char_ptr_cast(data), NumericCast<int64_t>(len));
					    return true;
				    });
				info.try_request = true;
				response = session.Request(info);
				break;
			}
			case RequestType::POST_REQUEST: {
				body_buffer = ReadRequestBody(request);
				PostRequestInfo info(url, headers, params, const_data_ptr_cast(body_buffer.c_str()),
				                     body_buffer.size());
				info.try_request = true;
				response = session.Request(info);
				if (response) {
					aws_response->GetResponseBody().write(info.buffer_out.data(),
					                                      NumericCast<int64_t>(info.buffer_out.size()));
				}
				break;
			}
			case RequestType::PUT_REQUEST: {
				body_buffer = ReadRequestBody(request);
				string content_type = request->GetContentType().c_str();
				PutRequestInfo info(url, headers, params, const_data_ptr_cast(body_buffer.c_str()), body_buffer.size(),
				                    content_type);
				info.try_request = true;
				response = session.Request(info);
				break;
			}
			case RequestType::HEAD_REQUEST: {
				HeadRequestInfo info(url, headers, params);
				info.try_request = true;
				response = session.Request(info);
				break;
			}
			case RequestType::DELETE_REQUEST: {
				DeleteRequestInfo info(url, headers, params);
				info.try_request = true;
				response = session.Request(info);
				break;
			}
			default:
				break;
			}

			LogRequest(context, *request, response.get(), string(), timer.ElapsedMillis(),
			           aws_response->GetResponseBody(), body_buffer, start_time);
			if (!response) {
				aws_response->SetResponseCode(Aws::Http::HttpResponseCode::REQUEST_NOT_MADE);
				return aws_response;
			}
			if (response->HasRequestError()) {
				// A transport failure (e.g. a pooled connection the peer closed while idle) never reached the
				// service, so mark it like the catch below instead of casting status 0 into a response code:
				// the AWS SDK classifies NETWORK_CONNECTION as retryable and re-issues the request, on a fresh
				// connection because the session does not return the errored client to the pool.
				aws_response->SetResponseCode(Aws::Http::HttpResponseCode::REQUEST_NOT_MADE);
				aws_response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
				aws_response->SetClientErrorMessage(response->GetRequestError().c_str());
				return aws_response;
			}
			aws_response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(static_cast<int>(response->status)));
			for (const auto &header : response->headers) {
				aws_response->AddHeader(header.first.c_str(), header.second.c_str());
			}
			// Bodies that did not stream through a handler (PUT / HEAD / DELETE) are copied here
			if (!response->body.empty() && request->GetMethod() != Aws::Http::HttpMethod::HTTP_GET &&
			    request->GetMethod() != Aws::Http::HttpMethod::HTTP_POST) {
				aws_response->GetResponseBody().write(response->body.data(),
				                                      NumericCast<int64_t>(response->body.size()));
			}
		} catch (std::exception &ex) {
			LogRequest(GlueHttpClientContextScope::Current(), *request, nullptr, ex.what(), timer.ElapsedMillis(),
			           aws_response->GetResponseBody(), body_buffer, start_time);
			aws_response->SetResponseCode(Aws::Http::HttpResponseCode::REQUEST_NOT_MADE);
			aws_response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
			aws_response->SetClientErrorMessage(ex.what());
		}
		return aws_response;
	}

private:
	void LogRequest(optional_ptr<ClientContext> context, const Aws::Http::HttpRequest &request,
	                optional_ptr<HTTPResponse> response, const string &exception, double duration_ms,
	                Aws::IOStream &body, const string &request_body, timestamp_t start_time) const {
		auto &logger = context ? Logger::Get(*context) : Logger::Get(db);
		bool log = logger.ShouldLog(GlueRequestLogType::NAME, GlueRequestLogType::LEVEL);
		auto trace = context ? GlueTraceState::GetActive(*context) : nullptr;
		if (!log && !trace) {
			return;
		}
		GlueTraceRequest entry;
		entry.operation =
		    request.HasHeader("x-amz-target") ? string(request.GetHeaderValue("x-amz-target").c_str()) : string();
		if (!exception.empty()) {
			entry.error = exception;
		} else if (!response) {
			entry.error = "request not made";
		} else if (response->HasRequestError()) {
			entry.error = response->GetRequestError();
		} else {
			entry.status = static_cast<idx_t>(response->status);
		}
		entry.response_bytes = NumericCast<idx_t>(MaxValue<int64_t>(static_cast<int64_t>(body.tellp()), 0));
		entry.request_bytes = request_body.size();
		entry.duration_ms = duration_ms;
		entry.start_time = start_time;
		entry.target = GlueRequestTarget(request_body);
		entry.continuation = JSONValueStart(request_body, "NextToken") != string::npos;
		entry.attempt = SDKAttempt(request);
		entry.purpose = GlueTraceTag::CurrentPurpose();
		if (trace) {
			entry.phase = trace->Phase();
			entry.start_ms = trace->ElapsedMs() - duration_ms;
		}
		if (log) {
			logger.WriteLog(GlueRequestLogType::NAME, GlueRequestLogType::LEVEL,
			                GlueRequestLogType::ConstructLogMessage(entry));
		}
		if (trace) {
			trace->Record(std::move(entry));
		}
	}

	//! The position after '"key":' in the JSON the SDK sends (pretty printed), or npos
	static idx_t JSONValueStart(const string &body, const char *key) {
		auto needle = string("\"") + key + "\"";
		auto pos = body.find(needle);
		if (pos == string::npos) {
			return pos;
		}
		pos = body.find_first_not_of(" \t\r\n", pos + needle.size());
		if (pos == string::npos || body[pos] != ':') {
			return string::npos;
		}
		return body.find_first_not_of(" \t\r\n", pos + 1);
	}

	static string JSONField(const string &body, const char *key) {
		auto pos = JSONValueStart(body, key);
		if (pos == string::npos || body[pos] != '"') {
			return string();
		}
		auto end = body.find('"', pos + 1);
		return end == string::npos ? string() : body.substr(pos + 1, end - pos - 1);
	}

	//! 'database.table', 'database' or the segment of a GetPartitions request: what the request is about
	static string GlueRequestTarget(const string &body) {
		auto database = JSONField(body, "DatabaseName");
		auto table = JSONField(body, "TableName");
		if (table.empty()) {
			table = JSONField(body, "Name");
		}
		string result = database;
		if (!table.empty()) {
			result += result.empty() ? table : "." + table;
		}
		auto segment = JSONValueStart(body, "SegmentNumber");
		if (segment != string::npos) {
			auto end = body.find_first_not_of("0123456789", segment);
			result += "#" + body.substr(segment, end - segment);
		}
		return result;
	}

	//! The SDK numbers its attempts in the amz-sdk-request header: 'attempt=2; max=3'
	static idx_t SDKAttempt(const Aws::Http::HttpRequest &request) {
		if (!request.HasHeader("amz-sdk-request")) {
			return 1;
		}
		string value = request.GetHeaderValue("amz-sdk-request").c_str();
		auto pos = value.find("attempt=");
		if (pos == string::npos) {
			return 1;
		}
		return NumericCast<idx_t>(MaxValue<int64_t>(std::atoll(value.c_str() + pos + 8), 1));
	}

	DatabaseInstance &db;
};

//! The factory is process global and outlives any one DatabaseInstance, so it holds none: the database comes from the
//! connection in the calling GlueHttpClientContextScope.
class GlueDuckDBHttpClientFactory : public Aws::Http::HttpClientFactory {
public:
	std::shared_ptr<Aws::Http::HttpClient>
	CreateHttpClient(const Aws::Client::ClientConfiguration &config) const override {
		// Use DuckDB's transport for clients built inside a GlueHttpClientContextScope, unless disabled or there is no
		// DuckDB HTTP transport yet (httpfs provides it). Anything else, e.g. the SDK's own EC2 metadata client or
		// other extensions' AWS clients, gets the SDK's own transport.
		auto context = GlueHttpClientContextScope::Current();
		if (context) {
			auto &db = DatabaseInstance::GetDatabase(*context);
			if (NetworkCallsViaDuckDB(db) && db.ExtensionIsLoaded("httpfs")) {
				return Aws::MakeShared<GlueDuckDBHttpClient>("GlueDuckDBHttp", db);
			}
		}
#ifdef _WIN32
		return Aws::MakeShared<Aws::Http::WinHttpSyncHttpClient>("GlueDuckDBHttp", config);
#else
		return Aws::MakeShared<Aws::Http::CurlHttpClient>("GlueDuckDBHttp", config);
#endif
	}

	std::shared_ptr<Aws::Http::HttpRequest>
	CreateHttpRequest(const Aws::String &uri, Aws::Http::HttpMethod method,
	                  const Aws::IOStreamFactory &stream_factory) const override {
		return CreateHttpRequest(Aws::Http::URI(uri), method, stream_factory);
	}

	std::shared_ptr<Aws::Http::HttpRequest>
	CreateHttpRequest(const Aws::Http::URI &uri, Aws::Http::HttpMethod method,
	                  const Aws::IOStreamFactory &stream_factory) const override {
		auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("GlueDuckDBHttp", uri, method);
		request->SetResponseStreamFactory(stream_factory);
		return request;
	}
};

} // namespace

bool GlueNetworkCallsViaDuckDB(DatabaseInstance &db) {
	return NetworkCallsViaDuckDB(db);
}

void RegisterGlueHttpClientFactory() {
	// SetHttpClientFactory swaps process global SDK state and is not thread safe, so it must not run again for every
	// database that loads the extension
	static std::once_flag registered;
	std::call_once(registered, [] {
		Aws::Http::SetHttpClientFactory(Aws::MakeShared<GlueDuckDBHttpClientFactory>("GlueDuckDBHttp"));
	});
}

} // namespace duckdb
