#include "api/glue_api_util.hpp"
#include "api/glue_http_client.hpp"
#include "catalog/glue_transaction.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/http/http_retry_budget.hpp"
#include "duckdb/main/http/http_util.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <aws/glue/model/BatchCreatePartitionRequest.h>
#include <aws/glue/model/BatchGetPartitionRequest.h>
#include <aws/glue/model/CreatePartitionRequest.h>
#include <aws/glue/model/DeletePartitionRequest.h>
#include <aws/glue/model/GetPartitionRequest.h>
#include <aws/glue/model/GetPartitionsRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/Segment.h>
#include <aws/glue/model/UpdatePartitionRequest.h>

namespace duckdb {

void GlueAPI::SetPartitionLocation(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                   const string &table_name, const vector<string> &values, const string &location) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "UpdatePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetPartitionRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetTableName(table_name);
	get_request.SetPartitionValues(ToAwsValues(values));
	auto get_outcome = client->GetPartition(get_request);
	if (!get_outcome.IsSuccess()) {
		if (IsEntityNotFound(get_outcome)) {
			throw CatalogException("Partition [%s] does not exist in Glue table '%s.%s'",
			                       PartitionValuesToString(values), database_name, table_name);
		}
		ThrowGlueError(get_outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                               PartitionValuesToString(values)));
	}
	auto &partition = get_outcome.GetResult().GetPartition();
	auto storage_descriptor = partition.GetStorageDescriptor();
	storage_descriptor.SetLocation(location);
	Aws::Glue::Model::PartitionInput input;
	input.SetValues(partition.GetValues());
	input.SetStorageDescriptor(storage_descriptor);
	input.SetParameters(partition.GetParameters());
	Aws::Glue::Model::UpdatePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValueList(ToAwsValues(values));
	request.SetPartitionInput(input);
	auto outcome = client->UpdatePartition(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome, StringUtil::Format("UpdatePartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(values)));
	}
}

bool GlueAPI::GetPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                           const string &table_name, const vector<string> &values, GluePartitionInfo &result) {
	GluePartitionDetails details;
	if (!GetPartition(context, catalog, database_name, table_name, values, details)) {
		return false;
	}
	result.values = std::move(details.values);
	result.location = std::move(details.location);
	return true;
}

bool GlueAPI::GetPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                           const string &table_name, const vector<string> &values, GluePartitionDetails &result,
                           string *raw_json) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetPartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValues(ToAwsValues(values));
	auto outcome = client->GetPartition(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(values)));
	}
	auto &partition = outcome.GetResult().GetPartition();
	result.values.clear();
	for (auto &value : partition.GetValues()) {
		result.values.push_back(ToStdString(value));
	}
	auto &storage_descriptor = partition.GetStorageDescriptor();
	result.location = ToStdString(storage_descriptor.GetLocation());
	result.serde_library = ToStdString(storage_descriptor.GetSerdeInfo().GetSerializationLibrary());
	result.serde_parameters = ToStdMap(storage_descriptor.GetSerdeInfo().GetParameters());
	result.columns = ToColumns(storage_descriptor.GetColumns());
	result.parameters = ToStdMap(partition.GetParameters());
	if (raw_json) {
		*raw_json = ToStdString(partition.Jsonize().View().WriteReadable());
	}
	return true;
}

bool GlueAPI::CreatePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const GluePartitionInput &partition, bool if_not_exists) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "CreatePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	// A partition carries its own StorageDescriptor: the table's, with the partition's location
	Aws::Glue::Model::GetTableRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetName(table_name);
	auto get_outcome = client->GetTable(get_request);
	if (!get_outcome.IsSuccess()) {
		ThrowGlueError(get_outcome, StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto descriptor = get_outcome.GetResult().GetTable().GetStorageDescriptor();
	descriptor.SetLocation(partition.location);

	Aws::Glue::Model::PartitionInput input;
	input.SetValues(ToAwsValues(partition.values));
	input.SetStorageDescriptor(descriptor);
	Aws::Glue::Model::CreatePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionInput(input);
	auto outcome = client->CreatePartition(request);
	if (!outcome.IsSuccess()) {
		if (IsAlreadyExists(outcome)) {
			if (if_not_exists) {
				return false;
			}
			throw CatalogException("Partition [%s] already exists in Glue table '%s.%s'",
			                       PartitionValuesToString(partition.values), database_name, table_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("CreatePartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(partition.values)));
	}
	return true;
}

bool GlueAPI::DeletePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const vector<string> &values) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "DeletePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::DeletePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValues(ToAwsValues(values));
	auto outcome = client->DeletePartition(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("DeletePartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(values)));
	}
	return true;
}

void GlueAPI::RenamePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const vector<string> &values,
                              const vector<string> &new_values) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "UpdatePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	// the partition to rename, with everything it carries (location, SerDe, parameters)
	Aws::Glue::Model::GetPartitionRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetTableName(table_name);
	get_request.SetPartitionValues(ToAwsValues(values));
	auto get_outcome = client->GetPartition(get_request);
	if (!get_outcome.IsSuccess()) {
		if (IsEntityNotFound(get_outcome)) {
			throw CatalogException("Partition [%s] does not exist in Glue table '%s.%s'",
			                       PartitionValuesToString(values), database_name, table_name);
		}
		ThrowGlueError(get_outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                               PartitionValuesToString(values)));
	}
	auto &partition = get_outcome.GetResult().GetPartition();

	// the new values must be free
	Aws::Glue::Model::GetPartitionRequest check_request;
	SetCatalogId(check_request, catalog);
	check_request.SetDatabaseName(database_name);
	check_request.SetTableName(table_name);
	check_request.SetPartitionValues(ToAwsValues(new_values));
	auto check_outcome = client->GetPartition(check_request);
	if (check_outcome.IsSuccess()) {
		throw CatalogException("Partition [%s] already exists in Glue table '%s.%s'",
		                       PartitionValuesToString(new_values), database_name, table_name);
	}
	if (!IsEntityNotFound(check_outcome)) {
		ThrowGlueError(check_outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                                 PartitionValuesToString(new_values)));
	}

	Aws::Glue::Model::PartitionInput input;
	input.SetValues(ToAwsValues(new_values));
	input.SetStorageDescriptor(partition.GetStorageDescriptor());
	input.SetParameters(partition.GetParameters());
	Aws::Glue::Model::UpdatePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValueList(ToAwsValues(values));
	request.SetPartitionInput(input);
	auto outcome = client->UpdatePartition(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome,
		               StringUtil::Format("UpdatePartition '%s.%s' [%s] -> [%s]", database_name, table_name,
		                                  PartitionValuesToString(values), PartitionValuesToString(new_values)));
	}
}

//! The partitions Glue returns per request. 1000 is the maximum the API allows.
static constexpr int GLUE_PARTITIONS_PAGE_SIZE = 1000;
//! The requests to run at the same time when the catalog is AWS and the setting leaves the choice open
static constexpr idx_t GLUE_DEFAULT_PARTITION_SEGMENTS = 8;
//! The maximum Glue accepts for Segment::TotalSegments
static constexpr idx_t GLUE_MAX_PARTITION_SEGMENTS = 10;

//! One chain of GetPartitions requests: pages through the partitions of segment 'segment_number' (the whole table
//! when 'total_segments' is 1) and appends them to 'result'
static void FetchPartitionSegment(Aws::Glue::GlueClient &client, const GlueCatalog &catalog,
                                  const string &database_name, const string &table_name, int segment_number,
                                  int total_segments, vector<GluePartitionInfo> &result) {
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetPartitionsRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		request.SetTableName(table_name);
		// Only the values and the location of a partition are used. The column schema every partition repeats is a
		// large part of the response, and fewer bytes per partition means more partitions per page.
		request.SetExcludeColumnSchema(true);
		request.SetMaxResults(GLUE_PARTITIONS_PAGE_SIZE);
		if (total_segments > 1) {
			Aws::Glue::Model::Segment segment;
			segment.SetSegmentNumber(segment_number);
			segment.SetTotalSegments(total_segments);
			request.SetSegment(segment);
		}
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client.GetPartitions(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("GetPartitions '%s.%s'", database_name, table_name));
		}
		auto &partitions = outcome.GetResult();
		for (auto &partition : partitions.GetPartitions()) {
			GluePartitionInfo info;
			for (auto &value : partition.GetValues()) {
				info.values.push_back(ToStdString(value));
			}
			info.location = ToStdString(partition.GetStorageDescriptor().GetLocation());
			result.push_back(std::move(info));
		}
		next_token = partitions.GetNextToken();
	} while (!next_token.empty());
}

class FetchPartitionSegmentTask : public BaseExecutorTask {
public:
	FetchPartitionSegmentTask(TaskExecutor &executor, ClientContext &context, Aws::Glue::GlueClient &client,
	                          const GlueCatalog &catalog, const string &database_name, const string &table_name,
	                          int segment_number, int total_segments, vector<GluePartitionInfo> &result)
	    : BaseExecutorTask(executor), context(context), client(client), catalog(catalog), database_name(database_name),
	      table_name(table_name), segment_number(segment_number), total_segments(total_segments), result(result) {
	}

	void ExecuteTask() override {
		// thread local: without it a worker's requests use the database level HTTP settings and stay out of this
		// connection's HTTP log
		GlueHttpClientContextScope http_scope(context);
		FetchPartitionSegment(client, catalog, database_name, table_name, segment_number, total_segments, result);
	}
	string TaskType() const override {
		return "FetchPartitionSegmentTask";
	}

private:
	ClientContext &context;
	Aws::Glue::GlueClient &client;
	const GlueCatalog &catalog;
	const string &database_name;
	const string &table_name;
	int segment_number;
	int total_segments;
	vector<GluePartitionInfo> &result;
};

//! The number of GetPartitions requests to run at the same time: the setting when it is given, otherwise one request
//! against a server given with ENDPOINT (moto ignores Segment and answers every segment with the whole table) and
//! GLUE_DEFAULT_PARTITION_SEGMENTS against AWS.
static int GetPartitionSegmentCount(ClientContext &context, const GlueCatalog &catalog) {
	idx_t segments = 0;
	Value setting;
	if (context.TryGetCurrentSetting("glue_get_partitions_segments", setting) && !setting.IsNull()) {
		segments = setting.GetValue<idx_t>();
	}
	if (segments == 0) {
		segments = catalog.options.endpoint.empty() ? GLUE_DEFAULT_PARTITION_SEGMENTS : 1;
	}
	if (segments > GLUE_MAX_PARTITION_SEGMENTS) {
		segments = GLUE_MAX_PARTITION_SEGMENTS;
	}
	return static_cast<int>(segments);
}

vector<GluePartitionInfo> GlueAPI::GetPartitions(ClientContext &context, GlueCatalog &catalog,
                                                 const string &database_name, const string &table_name) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	auto total_segments = GetPartitionSegmentCount(context, catalog);
	vector<vector<GluePartitionInfo>> segment_results(NumericCast<idx_t>(total_segments));

	if (total_segments == 1) {
		FetchPartitionSegment(*client, catalog, database_name, table_name, 0, 1, segment_results[0]);
	} else {
		// the segments do not overlap, so their requests can run at the same time
		TaskExecutor executor(context, TaskSchedulerType::ASYNC);
		for (int segment = 0; segment < total_segments; segment++) {
			executor.ScheduleTask(make_uniq<FetchPartitionSegmentTask>(
			    executor, context, *client, catalog, database_name, table_name, segment, total_segments,
			    segment_results[NumericCast<idx_t>(segment)]));
		}
		executor.WorkOnTasks();
	}

	vector<GluePartitionInfo> result;
	unordered_set<string> seen;
	for (auto &segment_result : segment_results) {
		for (auto &partition : segment_result) {
			// A Glue compatible server that ignores Segment answers every segment with the whole table: the values
			// identify the partition, so what was seen already is dropped here
			if (total_segments > 1 && !seen.insert(PartitionValuesKey(partition.values)).second) {
				continue;
			}
			result.push_back(std::move(partition));
		}
	}
	return result;
}

//! The partitions one BatchGetPartition request asks for: Glue takes up to 1000, but answers a smaller request sooner,
//! and the requests run in parallel. Against a Glue mock that answers in 50 ms plus 100 ms per 1000 partitions, 250 was
//! as fast as 100 for 200 and 1000 partitions with less than half the requests, and 1000 slower for 1000.
static constexpr idx_t GLUE_BATCH_GET_PARTITION_SIZE = 250;

//! The partitions among values[offset, offset + count) that exist, appended to 'result'. The keys Glue leaves
//! unprocessed (UnprocessedKeys, how it throttles the request) are asked for again after a back-off, as DuckDB retries
//! an HTTP request (http_retries, http_retry_wait_ms, http_retry_backoff, with jitter), which an interrupt ends: false
//! when some are still left after that, or when Glue throttles the request or fails it on its side, which the SDK
//! retries only GlueLookupRetryScope::LOOKUP_RETRIES times. A request Glue refuses throws an InvalidInputException for
//! a value it does not accept, a PermissionException when it denies the principal (IsClientRefusal), which refuses
//! every lookup by value of the table, and the Glue error otherwise.
static bool BatchGetPartitionBatch(ClientContext &context, Aws::Glue::GlueClient &client, const GlueCatalog &catalog,
                                   const string &database_name, const string &table_name,
                                   const vector<vector<string>> &values, idx_t offset, idx_t count,
                                   vector<GluePartitionInfo> &result) {
	Aws::Vector<Aws::Glue::Model::PartitionValueList> keys;
	for (idx_t i = offset; i < offset + count; i++) {
		Aws::Glue::Model::PartitionValueList key;
		key.SetValues(ToAwsValues(values[i]));
		keys.push_back(std::move(key));
	}
	auto params = HTTPUtil::Get(*context.db).InitializeParameters(context, string());
	RandomEngine random;
	GlueLookupRetryScope lookup_retries;
	for (uint64_t retry = 0;; retry++) {
		Aws::Glue::Model::BatchGetPartitionRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		request.SetTableName(table_name);
		request.SetPartitionsToGet(keys);
		auto outcome = client.BatchGetPartition(request);
		if (!outcome.IsSuccess()) {
			auto error_type = outcome.GetError().GetErrorType();
			auto message = ToStdString(outcome.GetError().GetMessage());
			if (error_type == Aws::Glue::GlueErrors::INVALID_INPUT || error_type == Aws::Glue::GlueErrors::VALIDATION) {
				throw InvalidInputException("Glue BatchGetPartition '%s.%s' refused the partition values: %s",
				                            database_name, table_name, message);
			}
			if (IsClientRefusal(outcome)) {
				throw PermissionException("Glue BatchGetPartition '%s.%s' refused: %s (%s)", database_name, table_name,
				                          message, ToStdString(outcome.GetError().GetExceptionName()));
			}
			if (IsUnavailable(outcome)) {
				// not answered, as when keys are left unprocessed: listing all partitions answers the scan
				DUCKDB_LOG_INFO(context, "Glue BatchGetPartition '%s.%s' failed after the retries: %s (%s)",
				                database_name, table_name, message, ToStdString(outcome.GetError().GetExceptionName()));
				return false;
			}
			ThrowGlueError(outcome, StringUtil::Format("BatchGetPartition '%s.%s'", database_name, table_name));
		}
		for (auto &partition : outcome.GetResult().GetPartitions()) {
			GluePartitionInfo info;
			for (auto &value : partition.GetValues()) {
				info.values.push_back(ToStdString(value));
			}
			info.location = ToStdString(partition.GetStorageDescriptor().GetLocation());
			result.push_back(std::move(info));
		}
		keys = outcome.GetResult().GetUnprocessedKeys();
		if (keys.empty()) {
			return true;
		}
		if (retry >= params->retries) {
			break;
		}
		// not as throttled: that would wait for several more, longer, back-offs, where listing all partitions
		// instead answers the scan
		ThreadUtil::SleepMs(HTTPRetryBudget::BackoffMs(params->retry_wait_ms, params->retry_backoff, retry + 1, 0,
		                                               random.NextRandomInteger64()),
		                    &context);
	}
	DUCKDB_LOG_INFO(context, "Glue BatchGetPartition '%s.%s' left %d of %d partitions unprocessed after %d retries",
	                database_name, table_name, keys.size(), count, params->retries);
	return false;
}

class BatchGetPartitionTask : public BaseExecutorTask {
public:
	BatchGetPartitionTask(TaskExecutor &executor, ClientContext &context, Aws::Glue::GlueClient &client,
	                      const GlueCatalog &catalog, const string &database_name, const string &table_name,
	                      const vector<vector<string>> &values, idx_t offset, idx_t count,
	                      vector<GluePartitionInfo> &result, atomic<bool> &complete)
	    : BaseExecutorTask(executor), context(context), client(client), catalog(catalog), database_name(database_name),
	      table_name(table_name), values(values), offset(offset), count(count), result(result), complete(complete) {
	}

	void ExecuteTask() override {
		// thread local: without it a worker's requests use the database level HTTP settings and stay out of this
		// connection's HTTP log
		GlueHttpClientContextScope http_scope(context);
		if (!BatchGetPartitionBatch(context, client, catalog, database_name, table_name, values, offset, count,
		                            result)) {
			complete = false;
		}
	}
	string TaskType() const override {
		return "BatchGetPartitionTask";
	}

private:
	ClientContext &context;
	Aws::Glue::GlueClient &client;
	const GlueCatalog &catalog;
	const string &database_name;
	const string &table_name;
	const vector<vector<string>> &values;
	idx_t offset;
	idx_t count;
	vector<GluePartitionInfo> &result;
	atomic<bool> &complete;
};

bool GlueAPI::BatchGetPartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                 const string &table_name, const vector<vector<string>> &values,
                                 vector<GluePartitionInfo> &result) {
	result.clear();
	if (values.empty()) {
		return true;
	}
	for (auto &partition_values : values) {
		idx_t bytes = 0;
		for (auto &value : partition_values) {
			bytes += value.size();
		}
		if (bytes > MAX_LOOKUP_PARTITION_BYTES) {
			throw InvalidInputException("Glue BatchGetPartition '%s.%s' refuses the values of a partition that total "
			                            "%d bytes, more than %d",
			                            database_name, table_name, bytes, idx_t(MAX_LOOKUP_PARTITION_BYTES));
		}
	}
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	auto batch_count = (values.size() + GLUE_BATCH_GET_PARTITION_SIZE - 1) / GLUE_BATCH_GET_PARTITION_SIZE;
	vector<vector<GluePartitionInfo>> batch_results(batch_count);
	atomic<bool> complete(true);
	if (batch_count == 1) {
		complete = BatchGetPartitionBatch(context, *client, catalog, database_name, table_name, values, 0,
		                                  values.size(), batch_results[0]);
	} else {
		// the batches do not overlap, so their requests can run at the same time
		TaskExecutor executor(context, TaskSchedulerType::ASYNC);
		for (idx_t batch = 0; batch < batch_count; batch++) {
			auto offset = batch * GLUE_BATCH_GET_PARTITION_SIZE;
			auto count = MinValue<idx_t>(GLUE_BATCH_GET_PARTITION_SIZE, values.size() - offset);
			executor.ScheduleTask(make_uniq<BatchGetPartitionTask>(executor, context, *client, catalog, database_name,
			                                                       table_name, values, offset, count,
			                                                       batch_results[batch], complete));
		}
		executor.WorkOnTasks();
	}
	if (!complete) {
		return false;
	}
	// in the order the values were asked for, as Glue answers in any order: the partitions of a scan, and with them
	// the order its files are listed in, do not change from one query to the next
	unordered_map<string, idx_t> requested;
	for (idx_t i = 0; i < values.size(); i++) {
		requested.emplace(PartitionValuesKey(values[i]), i);
	}
	vector<pair<idx_t, GluePartitionInfo>> found;
	for (auto &batch_result : batch_results) {
		for (auto &partition : batch_result) {
			auto entry = requested.find(PartitionValuesKey(partition.values));
			found.emplace_back(entry == requested.end() ? values.size() : entry->second, std::move(partition));
		}
	}
	std::stable_sort(found.begin(), found.end(),
	                 [](const pair<idx_t, GluePartitionInfo> &a, const pair<idx_t, GluePartitionInfo> &b) {
		                 return a.first < b.first;
	                 });
	for (auto &partition : found) {
		result.push_back(std::move(partition.second));
	}
	return true;
}

void GlueAPI::BatchCreatePartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                    const string &table_name, const vector<GluePartitionInput> &partitions) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "BatchCreatePartition");
	if (partitions.empty()) {
		return;
	}
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	// A partition carries its own StorageDescriptor: the table's, with the partition's location
	Aws::Glue::Model::GetTableRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetName(table_name);
	auto get_outcome = client->GetTable(get_request);
	if (!get_outcome.IsSuccess()) {
		ThrowGlueError(get_outcome, StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto table_descriptor = get_outcome.GetResult().GetTable().GetStorageDescriptor();

	// BatchCreatePartition accepts at most 100 partitions per call
	constexpr idx_t BATCH_SIZE = 100;
	for (idx_t offset = 0; offset < partitions.size(); offset += BATCH_SIZE) {
		Aws::Vector<Aws::Glue::Model::PartitionInput> inputs;
		for (idx_t i = offset; i < MinValue<idx_t>(offset + BATCH_SIZE, partitions.size()); i++) {
			auto &partition = partitions[i];
			Aws::Glue::Model::PartitionInput input;
			Aws::Vector<Aws::String> values(partition.values.begin(), partition.values.end());
			input.SetValues(values);
			auto descriptor = table_descriptor;
			descriptor.SetLocation(partition.location);
			input.SetStorageDescriptor(descriptor);
			inputs.push_back(std::move(input));
		}
		Aws::Glue::Model::BatchCreatePartitionRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		request.SetTableName(table_name);
		request.SetPartitionInputList(inputs);
		auto outcome = client->BatchCreatePartition(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("BatchCreatePartition '%s.%s'", database_name, table_name));
		}
		for (auto &error : outcome.GetResult().GetErrors()) {
			auto code = ToStdString(error.GetErrorDetail().GetErrorCode());
			if (code == "AlreadyExistsException") {
				// appending to an existing partition
				continue;
			}
			vector<string> values;
			for (auto &value : error.GetPartitionValues()) {
				values.push_back(ToStdString(value));
			}
			throw IOException("Glue BatchCreatePartition '%s.%s' failed for partition [%s]: %s (%s)", database_name,
			                  table_name, StringUtil::Join(values, ", "),
			                  ToStdString(error.GetErrorDetail().GetErrorMessage()), code);
		}
	}
}

} // namespace duckdb
