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

#include <functional>
#include <memory>
#include <unordered_map>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncFunction.hpp>
#include <boost/asio/awaitable.hpp>
#include <SharedAsyncResult.hpp>

namespace NES
{

/// One row's lazy execution of an immutable graph. Node identity, not structural equality, keys memoization.
/// All methods run on the executor strand. The callbacks retain whatever input/kernel state this row needs.
class AsyncGraphExecution final : public std::enable_shared_from_this<AsyncGraphExecution>
{
public:
    using Invoke = std::function<boost::asio::awaitable<AsyncValues>(AsyncCall::Ptr, AsyncValues)>;
    using ReadInput = std::function<RuntimeValue(const InputFieldReference&)>;
    AsyncGraphExecution(
        SharedAsyncResult::Executor executor, std::shared_ptr<const AsyncCallGraph> graph, ReadInput readInput, Invoke invoke);
    AsyncGraphExecution(const AsyncGraphExecution&) = delete;
    AsyncGraphExecution& operator=(const AsyncGraphExecution&) = delete;
    [[nodiscard]] boost::asio::awaitable<AsyncValues> run();
    /// A subscription to one producer. getUntil() times out only that subscription, never other consumers/the producer.
    [[nodiscard]] std::shared_ptr<SharedAsyncResult> evaluate(const AsyncCall::Ptr& node);
    void cancel();

private:
    static boost::asio::awaitable<AsyncValues> runImpl(std::shared_ptr<AsyncGraphExecution> self);
    static boost::asio::awaitable<AsyncValues> invokeNode(std::shared_ptr<AsyncGraphExecution> self, AsyncCall::Ptr node);
    boost::asio::awaitable<void> cancelAndDrain();
    SharedAsyncResult::Executor executor;
    std::shared_ptr<const AsyncCallGraph> graph;
    ReadInput readInput;
    Invoke invoke;
    std::unordered_map<AsyncCall::Ptr, std::shared_ptr<SharedAsyncResult>> cells;
    bool cancelled = false;
    bool runStarted = false;
};

}
