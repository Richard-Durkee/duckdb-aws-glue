#include "planning/hive_listing_pool.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"

namespace duckdb {

HiveListingPool &HiveListingPool::Get(idx_t threads) {
	static HiveListingPool pool;
	pool.Grow(threads);
	return pool;
}

HiveListingPool::~HiveListingPool() {
	{
		lock_guard<mutex> guard(lock);
		stopping = true;
	}
	has_work.notify_all();
	for (auto &worker : workers) {
		worker.join();
	}
}

void HiveListingPool::Grow(idx_t threads) {
	lock_guard<mutex> guard(lock);
	while (workers.size() < threads) {
		workers.emplace_back([this]() { Work(); });
	}
}

void HiveListingPool::Work() {
	while (true) {
		std::function<void()> task;
		{
			std::unique_lock<mutex> guard(lock);
			has_work.wait(guard, [&]() { return stopping || !queue.empty(); });
			if (queue.empty()) {
				return;
			}
			task = std::move(queue.front());
			queue.pop_front();
		}
		task();
	}
}

void HiveListingPool::Submit(std::function<void()> task) {
	{
		lock_guard<mutex> guard(lock);
		queue.push_back(std::move(task));
	}
	has_work.notify_one();
}

namespace {
//! The state of one Run, shared with the helpers queued on the pool: a helper that starts after the caller finished
//! leaves without touching the task
struct ListingBatch {
	const std::function<void(idx_t)> *task;
	idx_t count;
	atomic<idx_t> next {0};
	atomic<bool> failed {false};
	mutex lock;
	std::condition_variable idle;
	bool closed = false;
	idx_t active = 0;
	ErrorData error;

	void Drain() {
		while (!failed) {
			auto i = next++;
			if (i >= count) {
				return;
			}
			try {
				(*task)(i);
			} catch (std::exception &ex) {
				lock_guard<mutex> guard(lock);
				if (!error.HasError()) {
					error = ErrorData(ex);
				}
				failed = true;
			}
		}
	}
};
} // namespace

void HiveListingPool::Run(idx_t count, idx_t max_in_flight, const std::function<void(idx_t)> &task) {
	if (count == 0) {
		return;
	}
	auto batch = make_shared_ptr<ListingBatch>();
	batch->task = &task;
	batch->count = count;
	auto helpers = MinValue<idx_t>(count, MaxValue<idx_t>(max_in_flight, 1)) - 1;
	for (idx_t i = 0; i < helpers; i++) {
		Submit([batch]() {
			{
				lock_guard<mutex> guard(batch->lock);
				if (batch->closed) {
					return;
				}
				batch->active++;
			}
			batch->Drain();
			lock_guard<mutex> guard(batch->lock);
			batch->active--;
			batch->idle.notify_all();
		});
	}
	batch->Drain();
	std::unique_lock<mutex> guard(batch->lock);
	batch->closed = true;
	batch->idle.wait(guard, [&]() { return batch->active == 0; });
	if (batch->error.HasError()) {
		batch->error.Throw();
	}
}

idx_t ListingConcurrency(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting("hive_partition_listing_concurrency", setting) && !setting.IsNull()) {
		return setting.GetValue<idx_t>();
	}
	return 0;
}

static idx_t ListingThreads(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting("hive_listing_threads", setting) && !setting.IsNull()) {
		return MaxValue<idx_t>(setting.GetValue<idx_t>(), 1);
	}
	return 64;
}

namespace {
class ListingTask : public BaseExecutorTask {
public:
	ListingTask(TaskExecutor &executor, const std::function<void(idx_t)> &task, idx_t index)
	    : BaseExecutorTask(executor), task(task), index(index) {
	}

	void ExecuteTask() override {
		task(index);
	}
	string TaskType() const override {
		return "HiveListingTask";
	}

private:
	const std::function<void(idx_t)> &task;
	idx_t index;
};
} // namespace

void RunListingTasks(ClientContext &context, idx_t count, const std::function<void(idx_t)> &task) {
	if (count == 0) {
		return;
	}
	if (count == 1) {
		task(0);
		return;
	}
	auto concurrency = ListingConcurrency(context);
	if (concurrency == 0) {
		TaskExecutor executor(context);
		vector<unique_ptr<BaseExecutorTask>> tasks;
		for (idx_t i = 0; i < count; i++) {
			tasks.push_back(make_uniq<ListingTask>(executor, task, i));
		}
		executor.ScheduleTasks(std::move(tasks));
		executor.WorkOnTasks();
		return;
	}
	HiveListingPool::Get(ListingThreads(context)).Run(count, concurrency, task);
}

} // namespace duckdb
