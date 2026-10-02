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

/// The interface a concrete operator implements to be executed asynchronously.
///
/// Implementations run on threads the consumer source owns, outside the engine's worker
/// pool, so **blocking is allowed and expected** — that is the entire point of this
/// framework. Batching, concurrency, buffer handling, record order and the handling of
/// failures are the framework's job.
///
/// Instances are created through `AsyncExecutorRegistry`, one per consumer source; they
/// must be safe to call from several threads at once when `maxConcurrency` exceeds one.
class AsyncOperatorExecutor
{
public:
    AsyncOperatorExecutor() = default;
    AsyncOperatorExecutor(const AsyncOperatorExecutor&) = delete;
    AsyncOperatorExecutor& operator=(const AsyncOperatorExecutor&) = delete;
    AsyncOperatorExecutor(AsyncOperatorExecutor&&) = delete;
    AsyncOperatorExecutor& operator=(AsyncOperatorExecutor&&) = delete;
    virtual ~AsyncOperatorExecutor() = default;

    /// Handles one batch of records. Must return exactly one result per input record, in
    /// the same order — the framework matches them positionally.
    ///
    /// An implementation that cannot produce a value for a record returns an empty result
    /// for it rather than dropping it; the framework then leaves the output fields at
    /// their default. Throwing fails the query, so a well-behaved executor exhausts its
    /// own retries first.
    virtual std::vector<AsyncRecordResult> process(std::span<const AsyncRecordView> batch) = 0;
};

}
