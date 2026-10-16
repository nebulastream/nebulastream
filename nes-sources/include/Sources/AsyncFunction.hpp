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
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/awaitable.hpp>

namespace NES
{

/// The values one row's call produced: the first is what the call evaluates to, `ASYNC_OUTPUT(call, i)` reads the i-th.
using AsyncCallResult = std::vector<std::string>;

/// The implementation of an `AsyncLogicalFunction`, run by the `Async` source on its event loop.
/// An instance serves one call at a time, so it may keep per-connection state between calls.
///
/// Most functions only implement `call`. A function that answers several rows with one request (an LLM that classifies
/// a batch of rows in one prompt) overrides `callBatch` and `maxBatchSize`; one that produces several values per row
/// overrides `callBatch` as well.
class AsyncFunction
{
public:
    virtual ~AsyncFunction() = default;

    /// One row: its arguments in, its value out.
    virtual boost::asio::awaitable<std::string> call(std::vector<std::string> arguments);

    /// Up to `maxBatchSize` rows of one handed-off buffer: their arguments in, one result per row out, in order.
    /// By default each row is a `call` of its own, one after the other.
    virtual boost::asio::awaitable<std::vector<AsyncCallResult>> callBatch(std::vector<std::vector<std::string>> rows);

    /// Rows the source passes to one `callBatch` at most.
    [[nodiscard]] virtual size_t maxBatchSize() const { return 1; }

    /// Rows whose calls of this function may be in flight at once, if the function itself sets a bound.
    [[nodiscard]] virtual std::optional<size_t> maxConcurrency() const { return std::nullopt; }
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
