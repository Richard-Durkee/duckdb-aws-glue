#pragma once

#include "duckdb/common/typedefs.hpp"

#include <aws/core/client/RetryStrategy.h>

#include <memory>

namespace duckdb {

//! Wraps the client's retry strategy to log each retry it decides on as GlueRetry, to the connection making the call,
//! and count it for GlueRetryCounter
class GlueLoggingRetryStrategy : public Aws::Client::RetryStrategy {
public:
	explicit GlueLoggingRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> strategy_p);

	bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
	                 long attempted_retries) const override;
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
};

//! Whether the AWS SDK sent a Glue call made on this thread again, while the counter lives, after an attempt Glue may
//! have carried out: one without a response or with a server error, not one Glue refused (e.g. throttled). A write
//! whose response was lost is sent again, and the repeat then finds the effect of the first attempt (AlreadyExists /
//! EntityNotFound / ConcurrentModification), which this tells apart from a real conflict.
class GlueRetryCounter {
public:
	GlueRetryCounter();
	bool Retried() const;

private:
	friend class GlueLoggingRetryStrategy;
	//! The SDK runs a synchronous call and its retries on the calling thread
	static thread_local idx_t retries;
	idx_t start;
};

} // namespace duckdb
