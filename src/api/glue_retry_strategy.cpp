#include "api/glue_retry_strategy.hpp"
#include "api/glue_http_client.hpp"
#include "core/glue_log_types.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

#include <aws/core/client/AWSError.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/config/ConfigAndCredentialsCacheManager.h>
#include <aws/core/platform/Environment.h>

namespace duckdb {

thread_local int64_t GlueRetryStrategy::last_delay_ms = 0;
thread_local idx_t GlueRetryCounter::retries = 0;

std::shared_ptr<Aws::Client::RetryStrategy> CreateGlueRetryStrategy(int64_t glue_max_retries) {
	std::shared_ptr<Aws::Client::RetryStrategy> strategy;
	if (glue_max_retries >= 0) {
		// StandardRetryStrategy counts total attempts: N retries is N + 1 attempts
		strategy = std::make_shared<Aws::Client::StandardRetryStrategy>(static_cast<long>(glue_max_retries + 1));
	} else {
		auto retry_mode = Aws::Environment::GetEnv("AWS_RETRY_MODE");
		if (retry_mode.empty()) {
			retry_mode = Aws::Config::GetCachedConfigValue("retry_mode");
		}
		if (retry_mode.empty()) {
			retry_mode = "standard";
		}
		strategy = Aws::Client::InitRetryStrategy(retry_mode);
	}
	return std::make_shared<GlueRetryStrategy>(std::move(strategy));
}

GlueRetryCounter::GlueRetryCounter() : start(retries) {
}

bool GlueRetryCounter::Retried() const {
	return retries != start;
}

GlueRetryStrategy::GlueRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> strategy_p)
    : strategy(std::move(strategy_p)) {
}

bool GlueRetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
                                    long attempted_retries) const {
	auto retry = strategy->ShouldRetry(error, attempted_retries);
	auto delay_ms = last_delay_ms;
	last_delay_ms = 0;
	if (retry) {
		GlueRetryCounter::retries++;
	}
	auto context = GlueHttpClientContextScope::Current();
	if (retry && context && Logger::Get(*context).ShouldLog(GlueRetryLogType::NAME, GlueRetryLogType::LEVEL)) {
		GlueRetryEvent event;
		event.error = error.GetExceptionName().c_str();
		event.message = error.GetMessage().c_str();
		auto status = static_cast<int>(error.GetResponseCode());
		if (status > 0) {
			event.status = NumericCast<idx_t>(status);
		}
		event.retry = NumericCast<idx_t>(attempted_retries + 1);
		if (strategy->GetMaxAttempts() > 0) {
			event.max_retries = NumericCast<idx_t>(strategy->GetMaxAttempts() - 1);
		}
		event.delay_ms = NumericCast<idx_t>(MaxValue<int64_t>(delay_ms, 0));
		event.request_id = error.GetRequestId().c_str();
		DUCKDB_LOG(*context, GlueRetryLogType, event);
	}
	return retry;
}

long GlueRetryStrategy::CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &error,
                                                      long attempted_retries) const {
	auto delay_ms = strategy->CalculateDelayBeforeNextRetry(error, attempted_retries);
	last_delay_ms = delay_ms;
	return delay_ms;
}

long GlueRetryStrategy::GetMaxAttempts() const {
	return strategy->GetMaxAttempts();
}

void GlueRetryStrategy::GetSendToken() {
	strategy->GetSendToken();
}

bool GlueRetryStrategy::HasSendToken() {
	return strategy->HasSendToken();
}

void GlueRetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome) {
	strategy->RequestBookkeeping(outcome);
}

void GlueRetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome &outcome,
                                           const Aws::Client::AWSError<Aws::Client::CoreErrors> &last_error) {
	strategy->RequestBookkeeping(outcome, last_error);
}

const char *GlueRetryStrategy::GetStrategyName() const {
	return strategy->GetStrategyName();
}

} // namespace duckdb
