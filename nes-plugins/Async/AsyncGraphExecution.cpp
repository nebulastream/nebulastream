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
#include <AsyncGraphExecution.hpp>

#include <algorithm>
#include <exception>
#include <optional>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
/// co_spawn default-constructs its result on failure; AsyncValues deliberately has no default constructor.
boost::asio::awaitable<std::optional<AsyncValues>> optionalResult(boost::asio::awaitable<AsyncValues> operation)
{
    co_return co_await std::move(operation);
}

using Operation = decltype(boost::asio::co_spawn(
    std::declval<SharedAsyncResult::Executor>(),
    std::declval<boost::asio::awaitable<std::optional<AsyncValues>>>(),
    boost::asio::deferred));

boost::asio::awaitable<void> waitAll(std::vector<Operation> operations)
{
    if (operations.empty())
    {
        co_return;
    }
    auto [order, failures, values] = co_await boost::asio::experimental::make_parallel_group(std::move(operations))
                                         .async_wait(boost::asio::experimental::wait_for_one_error(), boost::asio::use_awaitable);
    /// Completion order preserves the initiating error instead of replacing it with a cancelled sibling's error.
    for (const auto index : order)
    {
        if (failures[index])
        {
            std::rethrow_exception(failures[index]);
        }
    }
}
}

AsyncGraphExecution::AsyncGraphExecution(
    SharedAsyncResult::Executor executor, std::shared_ptr<const AsyncCallGraph> graph, ReadInput readInput, Invoke invoke)
    : executor(std::move(executor)), graph(std::move(graph)), readInput(std::move(readInput)), invoke(std::move(invoke))
{
}

boost::asio::awaitable<AsyncValues> AsyncGraphExecution::run()
{
    return runImpl(shared_from_this());
}

std::shared_ptr<SharedAsyncResult> AsyncGraphExecution::evaluate(const AsyncCall::Ptr& node)
{
    PRECONDITION(executor.running_in_this_thread(), "Async graph execution must run on its strand");
    if (const auto found = cells.find(node); found != cells.end())
    {
        return found->second;
    }
    if (std::ranges::find(graph->getCalls(), node) == graph->getCalls().end())
    {
        throw InvalidQuerySyntax("Producer is not part of this async graph's reachable definitions");
    }
    if (cancelled)
    {
        throw boost::system::system_error{boost::asio::error::operation_aborted};
    }
    auto cell = std::make_shared<SharedAsyncResult>(executor, [self = weak_from_this(), node] { return invokeNode(self.lock(), node); });
    cells.emplace(node, cell);
    return cell;
}

void AsyncGraphExecution::cancel()
{
    PRECONDITION(executor.running_in_this_thread(), "Cancel graph execution on its strand");
    cancelled = true;
    for (const auto& [node, cell] : cells)
    {
        cell->cancel();
    }
}

boost::asio::awaitable<AsyncValues> AsyncGraphExecution::invokeNode(std::shared_ptr<AsyncGraphExecution> self, AsyncCall::Ptr node)
{
    if (!self || self->cancelled)
    {
        throw boost::system::system_error{boost::asio::error::operation_aborted};
    }
    std::vector<Operation> dependencies;
    std::unordered_map<AsyncCall::Ptr, std::shared_ptr<SharedAsyncResult>> producers;
    for (const auto& argument : node->getArguments())
    {
        if (const auto* reference = std::get_if<AsyncResultReference>(&argument))
        {
            const auto& producer = reference->getProducer();
            if (!producers.contains(producer))
            {
                auto cell = self->evaluate(producer);
                producers.emplace(producer, cell);
                dependencies.push_back(boost::asio::co_spawn(self->executor, optionalResult(cell->get()), boost::asio::deferred));
            }
        }
    }
    /// We await all needed depdencies together instead of one-by-one
    co_await waitAll(std::move(dependencies));
    if (self->cancelled)
    {
        throw boost::system::system_error{boost::asio::error::operation_aborted};
    }
    std::vector<RuntimeValue> arguments;
    for (const auto& argument : node->getArguments())
    {
        if (const auto* constant = std::get_if<RuntimeValue>(&argument))
        {
            arguments.push_back(*constant);
        }
        else if (const auto* field = std::get_if<InputFieldReference>(&argument))
        {
            auto value = self->readInput(*field);
            INVARIANT(
                value.getDataType() == field->getDataType(),
                "async input {} has type {}, expected {}",
                field->getField(),
                value.getDataType(),
                field->getDataType());
            arguments.push_back(std::move(value));
        }
        else
        {
            const auto& reference = std::get<AsyncResultReference>(argument);
            /// This should not suspend since we already awaited all inputs
            const auto values = co_await producers.at(reference.getProducer())->get();
            arguments.push_back(values[reference.getOutputIndex()].toOwned());
        }
    }
    auto returnValues = co_await self->invoke(node, AsyncValues{std::move(arguments)});
    INVARIANT(
        returnValues.size() == node->getResultTypes().size(),
        "async function {} produced {} values, but declares {}",
        node->getFunctionType(),
        returnValues.size(),
        node->getResultTypes().size());
    IF_INVARIANT(for (size_t index = 0; index < returnValues.size(); ++index) {
        const auto value = returnValues[index];
        const auto expected = node->getResultTypes()[index];
        INVARIANT(
            value.getDataType() == expected && (!value.isNull() || expected.nullable),
            "async function {} output {} has type {} (null: {}), expected {}",
            node->getFunctionType(),
            index,
            value.getDataType(),
            value.isNull(),
            expected);
    })
    co_return returnValues;
}

boost::asio::awaitable<void> AsyncGraphExecution::cancelAndDrain()
{
    cancel();
    for (const auto& [node, cell] : cells)
    {
        co_await cell->join();
    }
}

boost::asio::awaitable<AsyncValues> AsyncGraphExecution::runImpl(std::shared_ptr<AsyncGraphExecution> self)
{
    PRECONDITION(self->executor.running_in_this_thread(), "Run async graph execution on its strand");
    if (self->runStarted)
    {
        throw InvalidQuerySyntax("A row's async graph execution can only be run once");
    }
    self->runStarted = true;
    std::optional<AsyncValues> result;
    std::exception_ptr failure;
    try
    {
        std::vector<Operation> roots;
        std::unordered_map<AsyncCall::Ptr, std::shared_ptr<SharedAsyncResult>> producers;
        for (const auto& output : self->graph->getOutputs())
        {
            const auto& producer = output.result.getProducer();
            if (!producers.contains(producer))
            {
                auto cell = self->evaluate(producer);
                producers.emplace(producer, cell);
                roots.push_back(boost::asio::co_spawn(self->executor, optionalResult(cell->get()), boost::asio::deferred));
            }
        }
        co_await waitAll(std::move(roots));
        std::vector<RuntimeValue> values;
        for (const auto& output : self->graph->getOutputs())
        {
            /// This should not suspend since the waitAll should have completed all the awaitables
            const auto outputs = co_await producers.at(output.result.getProducer())->get();
            values.push_back(outputs[output.result.getOutputIndex()].toOwned());
        }
        result = AsyncValues{std::move(values)};
    }
    catch (...) /// NOLINT(no-raw-catch-all) preserve the initiating error across shielded cleanup
    {
        failure = std::current_exception();
    }
    if (!failure && (co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        failure = std::make_exception_ptr(boost::system::system_error{boost::asio::error::operation_aborted});
    }
    if (failure)
    {
        /// Shield only cleanup, not the caller's subsequent work. Preserve its cancellation state/filters.
        const bool throwIfCancelled = co_await boost::asio::this_coro::throw_if_cancelled();
        co_await boost::asio::this_coro::throw_if_cancelled(false);
        try
        {
            co_await boost::asio::co_spawn(
                self->executor,
                self->cancelAndDrain(),
                boost::asio::bind_cancellation_slot(boost::asio::cancellation_slot{}, boost::asio::use_awaitable));
        }
        catch (...) /// NOLINT(no-raw-catch-all) preserve the initiating error and restore caller policy even if cleanup initiation fails
        {
        }
        co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
        std::rethrow_exception(failure);
    }
    INVARIANT(result.has_value(), "Successful async graph execution must produce a result");
    IF_INVARIANT(for (const auto& [node, cell] : self->cells) {
        INVARIANT(cell->finished(), "Successful async graph execution left function {} unfinished", node->getFunctionType());
    })
    co_return std::move(*result);
}

}
