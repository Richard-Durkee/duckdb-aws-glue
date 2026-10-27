#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread.hpp"

#include <condition_variable>
#include <deque>
#include <functional>

namespace duckdb {
class ClientContext;

//! A bounded pool of threads the extension owns for listings: a listing waits on the object store rather than working
//! a CPU, so DuckDB's threads should not bound how many are in flight, and starting a thread per listing is wasted
//! work. One pool per process, sized by hive_listing_threads (it only grows).
class HiveListingPool {
public:
	static HiveListingPool &Get(idx_t threads);
	~HiveListingPool();

	//! Call 'task' for every index below 'count', at most 'max_in_flight' at a time. The caller works on the tasks too,
	//! so a task may call Run itself without waiting on a pool thread. The first error is thrown once no task runs.
	void Run(idx_t count, idx_t max_in_flight, const std::function<void(idx_t)> &task);
	//! Run 'task' on a pool thread
	void Submit(std::function<void()> task);

private:
	void Grow(idx_t threads);
	void Work();

private:
	mutex lock;
	std::condition_variable has_work;
	std::deque<std::function<void()>> queue;
	vector<thread> workers;
	bool stopping = false;
};

//! Call 'task' for every index below 'count': with hive_partition_listing_concurrency 0 as tasks on DuckDB's threads,
//! otherwise on the listing pool with that many in flight
void RunListingTasks(ClientContext &context, idx_t count, const std::function<void(idx_t)> &task);
//! hive_partition_listing_concurrency
idx_t ListingConcurrency(ClientContext &context);
//! Run 'task' in the background on the listing pool
void SubmitListingTask(ClientContext &context, std::function<void()> task);

} // namespace duckdb
