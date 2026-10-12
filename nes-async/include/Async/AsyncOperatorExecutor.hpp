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

#include <cstddef>
#include <exception>
#include <expected>
#include <functional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <Async/AsyncRecordLayout.hpp>

namespace NES
{

/// One output field an executor produced for a record, addressed by its index in the
/// output layout. The value is text; for a field that is not VARSIZED the framework
/// converts it to the declared type before writing.
struct AsyncFieldValue
{
    size_t fieldIndex = 0;
    std::string value;
};

/// What an executor produces for a single input record.
struct AsyncRecordResult
{
    std::vector<AsyncFieldValue> fields;
};

/// Everything an executor needs to know about the operator it implements. Handed over
/// once, when the consumer source opens.
struct AsyncOperatorContext
{
    /// Schema of the records arriving from the producer half.
    const AsyncRecordLayout* inputLayout = nullptr;
    /// Schema the operator produces; usually the input plus the appended fields.
    const AsyncRecordLayout* outputLayout = nullptr;
    /// Operator-specific settings, opaque to the framework.
    std::unordered_map<std::string, std::string> config;
    /// How many records the framework will put into one `process` call at most.
    size_t batchSize = 1;
};

/// What one batch produced, or the reason it produced nothing. The error travels as an
/// `exception_ptr` because an event loop cannot throw at its caller: the framework rethrows it on
/// the source thread, which is where the engine sees it.
using AsyncBatchOutcome = std::expected<std::vector<AsyncRecordResult>, std::exception_ptr>;

/// Called once per submitted batch, on a thread the operator's event loop owns. It must not block.
using AsyncBatchCompletion = std::move_only_function<void(AsyncBatchOutcome)>;

/// What a concrete operator implements to be executed asynchronously. Instances are created
/// through `AsyncExecutorRegistry`, one per consumer source.
///
/// An operator implements exactly one of the two derived interfaces, and which one it is decides
/// what the framework has to spend on it. That is why it is a type and not a flag: an executor that
/// blocks costs a thread per call in flight, and at the concurrency such operators are configured
/// with — easily 256 against a model server — that is 256 threads doing nothing but waiting.
class AsyncOperatorExecutor
{
public:
    AsyncOperatorExecutor() = default;
    AsyncOperatorExecutor(const AsyncOperatorExecutor&) = delete;
    AsyncOperatorExecutor& operator=(const AsyncOperatorExecutor&) = delete;
    AsyncOperatorExecutor(AsyncOperatorExecutor&&) = delete;
    AsyncOperatorExecutor& operator=(AsyncOperatorExecutor&&) = delete;
    virtual ~AsyncOperatorExecutor() = default;
};

/// An operator whose call blocks the thread that makes it.
///
/// The framework runs `maxConcurrency` threads of its own and calls `process` on them, outside the
/// engine's worker pool, so **blocking is allowed and expected**. This is the right interface for
/// work that has no asynchronous form — a local computation, or a test double that sleeps.
class BlockingAsyncExecutor : public AsyncOperatorExecutor
{
public:
    /// Handles one batch of records. Must return exactly one result per input record, in the same
    /// order — the framework matches them positionally.
    ///
    /// An implementation that cannot produce a value for a record returns an empty result for it
    /// rather than dropping it; the framework then leaves the output fields at their default.
    /// Throwing fails the query, so a well-behaved executor exhausts its own retries first.
    ///
    /// Must be safe to call from several threads at once when `maxConcurrency` exceeds one.
    virtual std::vector<AsyncRecordResult> process(std::span<const AsyncRecordView> batch) = 0;
};

/// An operator that hands its call to an event loop and is told when it finished.
///
/// The framework runs no threads for it at all: it submits up to `maxConcurrency` batches and
/// submits the next one whenever a completion arrives. Requests in flight are then bounded by that
/// number rather than by a thread count, which is the point of the whole interface.
class EventLoopAsyncExecutor : public AsyncOperatorExecutor
{
public:
    /// Starts the batch and arranges for `done` to be called exactly once with the outcome.
    ///
    /// Must return promptly — it is called from the framework's intake thread and from completions,
    /// and blocking here would defeat the purpose. `batch` and the records it views stay valid
    /// until `done` has run, so an implementation may keep the span until then.
    ///
    /// The same contract as `process` applies to the results: one per record, in input order, and
    /// an empty result rather than a dropped record. A failure travels in the outcome instead of
    /// being thrown, and fails the query just the same.
    virtual void submit(std::span<const AsyncRecordView> batch, AsyncBatchCompletion done) = 0;
};

}
