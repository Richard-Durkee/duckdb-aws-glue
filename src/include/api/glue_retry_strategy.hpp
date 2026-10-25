#pragma once

#include "duckdb/common/typedefs.hpp"

#include <aws/core/client/RetryStrategy.h>

#include <cstdint>
#include <memory>

namespace duckdb {

//! The Glue client's retry strategy: glue_max_retries retries when set (>= 0), else the retry mode of AWS_RETRY_MODE
//! or the profile's retry_mode, else the SDK's standard mode (exponential backoff with jitter and a retry quota, three
//! attempts unless AWS_MAX_ATTEMPTS / max_attempts says otherwise) instead of its legacy ten retries without jitter
std::shared_ptr<Aws::Client::RetryStrategy> CreateGlueRetryStrategy(int64_t glue_max_retries);

//! Wraps a retry strategy to log each retry it decides on as GlueRetry, to the connection making the call, and count
//! it for GlueRetryCounter
class GlueRetryStrategy : public Aws::Client::RetryStrategy {
public:
	explicit GlueRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> strategy_p);

	bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
	                 long attempted_retries) const override;
	//! The SDK asks for the delay before it asks whether to retry, on the thread making the call
	long CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
	                                   long attempted_retries) const override;
	long GetMaxAttempts() const override;
	void GetSendToken() override;
	bool HasSendToken() override;
	void RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome) override;
	void RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome,
	                        const Aws::Client::AWSError<Aws::Client::CoreErrors> &last_error) override;
	const char *GetStrategyName() const override;

private:
	std::shared_ptr<Aws::Client::RetryStrategy> strategy;
	static thread_local int64_t last_delay_ms;
};

//! Whether the AWS SDK sent a Glue call made on this thread while the counter lives more than once. A write whose
//! response was lost is sent again, and the repeat then sees the effect of the first one (AlreadyExists /
//! EntityNotFound), which this tells apart from a real conflict.
class GlueRetryCounter {
public:
	GlueRetryCounter();
	bool Retried() const;

private:
	friend class GlueRetryStrategy;
	//! The SDK runs a synchronous call and its retries on the calling thread
	static thread_local idx_t retries;
	idx_t start;
};

} // namespace duckdb
