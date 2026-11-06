/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <chrono>
#include <cstddef>
#include <exception>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// The values one row's call produced: the first is what the call evaluates to, `ASYNC_OUTPUT(call, i)` reads the i-th.
using AsyncCallResult = std::vector<std::string>;

/// The results of one batch, one entry per row in the batch's order. A row without a result, e.g. one the service left
/// out of its answer, is called again on its own.
using AsyncBatchResult = std::vector<std::optional<AsyncCallResult>>;

/// Thrown by an async function for a failure that may pass when the same call is repeated a little later, e.g. a service
/// that is overloaded or briefly unavailable (HTTP 429, 503). The Async source retries such a call with exponential
/// backoff and jitter, at least `retryAfter` later if the service said when, within the stage's `transient_retries` and
/// `transient_retry_seconds`; after that the query fails with `failure`, as with any other failure. Every other failure
/// is retried `max_retries` times, as before.
class AsyncTransientFailure final : public std::exception
{
public:
    AsyncTransientFailure(Exception failure, std::optional<std::chrono::milliseconds> retryAfter)
        : failure_(std::move(failure)), retryAfter_(retryAfter)
    {
    }

    [[nodiscard]] const char* what() const noexcept override { return failure_.what(); }

    /// What the query fails with once the retries are used up.
    [[nodiscard]] const Exception& failure() const { return failure_; }

    /// How long the service asked to wait, e.g. its `Retry-After`.
    [[nodiscard]] std::optional<std::chrono::milliseconds> retryAfter() const { return retryAfter_; }

private:
    Exception failure_;
    std::optional<std::chrono::milliseconds> retryAfter_;
};

/// A limit on calls in flight that a function shares with every async call in the process that names the same
/// `resource`, whichever stage or query it belongs to (`AsyncResourceLimiter`).
struct AsyncSharedLimit
{
    /// Names what the calls go to, e.g. a service endpoint and model; the function decides what is shared.
    std::string resource;
    size_t limit;
};

/// The implementation of an `AsyncLogicalFunction`, run by the `Async` source on its event loop.
/// An instance serves one call at a time, so it may keep per-connection state between calls.
///
/// Most functions only implement `call`. One that produces several values per row, e.g. one per OUTPUT field of a model,
/// overrides `callValues` instead.
///
/// A function whose service takes several rows in one request sets `maxBatchSize` and implements `callBatch`. A function
/// whose calls must not overlap or reorder for rows that share a value, e.g. one that keeps state per key, names that
/// argument in `orderingKey`. Both are off by default, and the source then runs the function exactly as before.
class AsyncFunction
{
public:
    virtual ~AsyncFunction() = default;

    /// One row: its arguments in, its value out.
    virtual boost::asio::awaitable<std::string> call(std::vector<std::string> arguments);

    /// One row: its arguments in, all of its values out. By default the single value of `call`.
    virtual boost::asio::awaitable<AsyncCallResult> callValues(std::vector<std::string> arguments);

    /// Several rows in one go: per row its arguments in, its values out (see `AsyncBatchResult`). The source hands at
    /// most `maxBatchSize` rows. By default `callValues` for one row after the other.
    virtual boost::asio::awaitable<AsyncBatchResult> callBatch(std::vector<std::vector<std::string>> rows);

    /// Rows whose calls of this function may be in flight at once, if the function itself sets a bound.
    [[nodiscard]] virtual std::optional<size_t> maxConcurrency() const { return std::nullopt; }

    /// A limit this function's calls share with other async calls to the same resource, if it sets one. Each request
    /// the source makes holds a permit of that limit while it is in flight; `maxConcurrency` still caps the stage.
    [[nodiscard]] virtual std::optional<AsyncSharedLimit> sharedLimit() const { return std::nullopt; }

    /// Rows `callBatch` takes at once. 1, the default, means the source never calls `callBatch`.
    [[nodiscard]] virtual size_t maxBatchSize() const { return 1; }

    /// Called on the instance whose call failed, before the source waits `delay` and calls again; `transient` says whether
    /// the failure was an `AsyncTransientFailure`. For statistics: nothing has to be done here.
    virtual void retryScheduled(std::chrono::milliseconds /*delay*/, bool /*transient*/) { }

    /// The argument whose value orders the calls: rows with equal values are called one at a time, in stream order, and
    /// rows with different values in parallel. Must be a handed-off field or a constant, not the value of another call.
    [[nodiscard]] virtual std::optional<size_t> orderingKey() const { return std::nullopt; }
};

/// Where one argument of an async call comes from: a handed-off field, a constant, or the result of an
/// earlier call in the same list.
struct AsyncCallArgument
{
    std::optional<std::string> field;
    std::optional<std::string> constant;
    std::optional<size_t> call;
    /// Which value of the earlier call.
    size_t output = 0;
};

/// A field of the source's output that holds one value of a call.
struct AsyncCallOutput
{
    size_t index;
    std::string field;
};

/// One async call as the `Async` source receives it in its `CALLS` config. Calls are listed after every call
/// they read. Only calls whose values are fields of the source's output have outputs; the others feed calls only.
struct AsyncCall
{
    std::vector<AsyncCallOutput> outputs;
    std::string function;
    /// Handed to the implementation when the source creates it; see `AsyncLogicalFunction::getAsyncConfiguration`.
    std::string configuration;
    std::vector<AsyncCallArgument> arguments;
};

}
