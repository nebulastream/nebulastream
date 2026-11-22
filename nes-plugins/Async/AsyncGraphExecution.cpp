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

#include <chrono>
#include <cstddef>
#include <expected>
#include <variant>
#include <algorithm>
#include <exception>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/RuntimeValue.hpp>
#include <SharedAsyncResult.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/system/error_code.hpp> /// NOLINT(misc-include-cleaner) Public Boost.System provider; diagnostics misattribute error_code.
#include <Util/Logger/Logger.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp> /// NOLINT(misc-include-cleaner) Public Asio provider; diagnostics misattribute co_spawn overloads.
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <ErrorHandling.hpp>
#include <Sources/AsyncError.hpp>

namespace NES
{
namespace
{
using Cell = std::shared_ptr<SharedAsyncResult>;

struct WaitState
{
    explicit WaitState(SharedAsyncResult::Strand strand, size_t size) : changed(std::move(strand)), remaining(size), values(size)
    {
        changed.expires_at(std::chrono::steady_clock::time_point::max());
    }

    boost::asio::steady_timer changed;
    size_t remaining;
    std::vector<std::optional<SharedRuntimeValues>> values;
    std::optional<AsyncError> failure;
};

boost::asio::awaitable<AsyncResult<std::vector<SharedRuntimeValues>>> waitCells(SharedAsyncResult::Strand executor, std::vector<Cell> cells)
{
    if (cells.empty())
    {
        co_return std::vector<SharedRuntimeValues>{};
    }
    const bool throwIfCancelled = co_await boost::asio::this_coro::throw_if_cancelled();
    co_await boost::asio::this_coro::throw_if_cancelled(false);
    auto state = std::make_shared<WaitState>(executor, cells.size());
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) callbacks retain equally sized cells and values
    for (size_t i = 0; i < cells.size(); ++i)
    {
        boost::asio::co_spawn( /// NOLINT(misc-include-cleaner) Declared by the public boost/asio/co_spawn.hpp provider.
            executor,
            cells[i]->get(),
            [state, cells, i](std::exception_ptr exception, AsyncResult<SharedRuntimeValues> result)
            {
                if (exception)
                {
                    /// Exceptional completion is only a terminal infrastructure fallback; operational failures are returned as values.
                    if (!state->failure)
                    {
                        state->failure = AsyncError{
                            .exception = IOError("Unexpected async producer task failure"), .kind = AsyncError::Type::Permanent};
                        for (size_t sibling = 0; sibling < cells.size(); ++sibling)
                        {
                            if (sibling != i)
                            {
                                cells[sibling]->cancel();
                            }
                        }
                    }
                }
                else if (!result && !state->failure)
                {
                    state->failure = result.error();
                    for (size_t sibling = 0; sibling < cells.size(); ++sibling)
                    {
                        if (sibling != i)
                        {
                            cells[sibling]->cancel();
                        }
                    }
                }
                else if (result)
                {
                    state->values[i] = std::move(*result);
                }
                --state->remaining;
                state->changed.cancel();
            });
    }
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    while (state->remaining != 0U)
    {
        state->changed.expires_at(std::chrono::steady_clock::time_point::max());
        boost::system::error_code error; /// NOLINT(misc-include-cleaner) Declared by the public boost/system/error_code.hpp provider.
        co_await state->changed.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, error));
        if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
        {
            if (!state->failure)
            {
                state->failure = AsyncError::cancelled();
            }
            for (const auto& cell : cells)
            {
                cell->cancel();
            }
        }
    }
    co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
    if (state->failure)
    {
        co_return std::unexpected(std::move(*state->failure));
    }
    std::vector<SharedRuntimeValues> values;
    values.reserve(state->values.size());
    for (auto& value : state->values)
    {
        INVARIANT(value.has_value(), "A successfully completed async cell must have a value");
        values.push_back(std::move(*value));
    }
    co_return values;
}
}

AsyncGraphExecution::AsyncGraphExecution(
    SharedAsyncResult::Strand executor, std::shared_ptr<const AsyncCallGraph> graph, ReadInput readInput, Invoke invoke)
    : executor(std::move(executor)), graph(std::move(graph)), readInput(std::move(readInput)), invoke(std::move(invoke))
{
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> AsyncGraphExecution::run()
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
    PRECONDITION(
        std::ranges::find(graph->getCalls(), node) != graph->getCalls().end(),
        "Producer is not part of this async graph's reachable definitions");
    if (cancelled)
    {
        return std::make_shared<SharedAsyncResult>(
            executor,
            []() -> boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> { co_return std::unexpected(AsyncError::cancelled()); });
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
        static_cast<void>(node);
        cell->cancel();
    }
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>>
AsyncGraphExecution::invokeNode(std::shared_ptr<AsyncGraphExecution> self, AsyncCall::Ptr node)
{
    if (!self || self->cancelled)
    {
        co_return std::unexpected(AsyncError::cancelled());
    }
    std::vector<Cell> dependencies;
    std::unordered_map<AsyncCall::Ptr, size_t> producerIndices;
    for (const auto& argument : node->getArguments())
    {
        if (const auto* reference = std::get_if<AsyncResultReference>(&argument))
        {
            auto producer = reference->getProducer();
            if (!producerIndices.contains(producer))
            {
                producerIndices.emplace(producer, dependencies.size());
                dependencies.push_back(self->evaluate(producer));
            }
        }
    }
    auto dependencyResults = co_await waitCells(self->executor, dependencies);
    if (!dependencyResults)
    {
        co_return std::unexpected(std::move(dependencyResults.error()));
    }
    if (self->cancelled)
    {
        co_return std::unexpected(AsyncError::cancelled());
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
            /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) dependency map and output handles are validated
            const auto& producerValues = (*dependencyResults)[producerIndices.at(reference.getProducer())];
            arguments.push_back(producerValues[reference.getOutputIndex()].toOwned());
            /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        }
    }
    auto result = co_await self->invoke(node, SharedRuntimeValues{std::move(arguments)});
    if (!result)
    {
        co_return std::unexpected(std::move(result.error()));
    }
    INVARIANT(
        result->size() == node->getResultTypes().size(),
        "async function {} produced {} values, but declares {}",
        node->getFunctionType(),
        result->size(),
        node->getResultTypes().size());
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,readability-simplify-boolean-expr) checked bounds; explicit type/nullability contract
    IF_INVARIANT(for (size_t index = 0; index < result->size(); ++index) {
        const auto value = (*result)[index];
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
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,readability-simplify-boolean-expr)
    co_return std::move(*result);
}

boost::asio::awaitable<void> AsyncGraphExecution::cancelAndDrain()
{
    cancel();
    for (const auto& [node, cell] : cells)
    {
        static_cast<void>(node);
        co_await cell->join();
    }
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> AsyncGraphExecution::runImpl(std::shared_ptr<AsyncGraphExecution> self)
{
    PRECONDITION(self->executor.running_in_this_thread(), "Run async graph execution on its strand");
    PRECONDITION(!self->runStarted, "A row's async graph execution can only be run once");
    self->runStarted = true;
    const bool throwIfCancelled = co_await boost::asio::this_coro::throw_if_cancelled();
    co_await boost::asio::this_coro::throw_if_cancelled(false);
    if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
        co_return std::unexpected(AsyncError::cancelled());
    }

    std::vector<Cell> roots;
    std::unordered_map<AsyncCall::Ptr, size_t> indices;
    for (const auto& output : self->graph->getOutputs())
    {
        const auto& producer = output.result.getProducer();
        if (!indices.contains(producer))
        {
            indices.emplace(producer, roots.size());
            roots.push_back(self->evaluate(producer));
        }
    }
    auto rootResults = co_await waitCells(self->executor, roots);
    AsyncResult<SharedRuntimeValues> result;
    if (!rootResults)
    {
        result = std::unexpected(std::move(rootResults.error()));
    }
    else if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        result = std::unexpected(AsyncError::cancelled());
    }
    else
    {
        std::vector<RuntimeValue> values;
        for (const auto& output : self->graph->getOutputs())
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) root map and output handles are validated
            values.push_back((*rootResults)[indices.at(output.result.getProducer())][output.result.getOutputIndex()].toOwned());
        }
        result = SharedRuntimeValues{std::move(values)};
    }
    if (!result)
    {
        /// Cancel and join every started producer before releasing row state; keep the initiating error.
        const auto& failure = result.error();
        const auto [cleanupFailure] = co_await boost::asio::co_spawn( /// NOLINT(misc-include-cleaner) Public co_spawn.hpp provider.
            self->executor,
            self->cancelAndDrain(),
            boost::asio::bind_cancellation_slot(boost::asio::cancellation_slot{}, boost::asio::as_tuple(boost::asio::use_awaitable)));
        if (cleanupFailure)
        {
            NES_WARNING("Async cleanup failed unexpectedly; preserving the initiating error");
        }
        co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
        co_return std::unexpected(failure);
    }
    IF_INVARIANT(for (const auto& [node, cell] : self->cells) {
        INVARIANT(cell->finished(), "Successful async graph execution left function {} unfinished", node->getFunctionType());
    })
    co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
    co_return result;
}
}
