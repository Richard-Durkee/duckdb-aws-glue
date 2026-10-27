#pragma once

#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "duckdb/common/exception.hpp"

#include <aws/glue/GlueClient.h>
#include <aws/glue/GlueErrors.h>
#include <aws/glue/model/Column.h>
#include <aws/glue/model/Database.h>
#include <aws/glue/model/Table.h>

namespace duckdb {

string ToStdString(const Aws::String &input);
unordered_map<string, string> ToStdMap(const Aws::Map<Aws::String, Aws::String> &input);
vector<GlueColumn> ToColumns(const Aws::Vector<Aws::Glue::Model::Column> &input);
GlueDatabaseInfo ToDatabaseInfo(const Aws::Glue::Model::Database &database);
GlueTableInfo ToTableInfo(const Aws::Glue::Model::Table &table);
Aws::Map<Aws::String, Aws::String> ToAwsMap(const unordered_map<string, string> &input);
Aws::Vector<Aws::Glue::Model::Column> ToAwsColumns(const vector<GlueColumn> &input);
Aws::Vector<Aws::String> ToAwsValues(const vector<string> &values);
string PartitionValuesToString(const vector<string> &values);
void CheckWritable(const GlueCatalog &catalog, const string &operation);

template <class OUTCOME>
[[noreturn]] void ThrowGlueError(const OUTCOME &outcome, const string &operation) {
	auto &error = outcome.GetError();
	auto message = Exception::ConstructMessage("Glue %s failed: %s (%s)", operation, ToStdString(error.GetMessage()),
	                                           ToStdString(error.GetExceptionName()));
	throw GlueException(message, error.GetErrorType() == Aws::Glue::GlueErrors::ACCESS_DENIED);
}

//! Glue denied the principal the request: AccessDeniedException, or a 403 the SDK could not name. The same request is
//! denied again, unlike one Glue throttles, fails on its side, or fails for a reason that may pass (an expired token)
template <class OUTCOME>
bool IsClientRefusal(const OUTCOME &outcome) {
	auto &error = outcome.GetError();
	return error.GetErrorType() == Aws::Glue::GlueErrors::ACCESS_DENIED ||
	       (static_cast<int>(error.GetResponseCode()) == 403 && error.GetErrorType() == Aws::Glue::GlueErrors::UNKNOWN);
}

//! Glue throttled the request (ThrottlingException, a 429) or failed it on its side (a 5xx) after the SDK's retries: it
//! may answer the same request later
template <class OUTCOME>
bool IsUnavailable(const OUTCOME &outcome) {
	auto &error = outcome.GetError();
	auto status = static_cast<int>(error.GetResponseCode());
	return status == 429 || status >= 500 || error.GetErrorType() == Aws::Glue::GlueErrors::THROTTLING ||
	       error.GetErrorType() == Aws::Glue::GlueErrors::SLOW_DOWN;
}

//! While one is active on a thread, the SDK retries the Glue requests the thread sends at most LOOKUP_RETRIES times,
//! instead of as often as its retry strategy allows (10 times, over 25 seconds of back-off): a lookup of only some
//! partitions of a table that Glue throttles is answered sooner by fetching all of them, with all retries
class GlueLookupRetryScope {
public:
	//! Active when 'active' is set or an enclosing scope on the thread is active
	explicit GlueLookupRetryScope(bool active = true);
	~GlueLookupRetryScope();

	static constexpr long LOOKUP_RETRIES = 2;
	static bool Active();

private:
	bool previous;
};

template <class OUTCOME>
bool IsEntityNotFound(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ENTITY_NOT_FOUND;
}

template <class OUTCOME>
bool IsAccessDenied(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ACCESS_DENIED;
}

template <class OUTCOME>
bool IsAlreadyExists(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ALREADY_EXISTS;
}

template <class REQUEST>
void SetCatalogId(REQUEST &request, const GlueCatalog &catalog) {
	if (!catalog.options.catalog_id.empty()) {
		request.SetCatalogId(catalog.options.catalog_id);
	}
}

} // namespace duckdb
