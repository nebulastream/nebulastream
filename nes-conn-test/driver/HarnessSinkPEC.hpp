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
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <unordered_map>
#include <utility>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::ConnTest
{

/// The pipeline context that the sink session passes to the sink, in place of the engine's.
/// Each thread that writes has its own, so the contexts share no state.
/// A sink repeats a task the way it does in the engine: a buffer goes back to the session's queue, and a stop is called again.
/// Starting and stopping write no buffer, so their contexts have no queue to put one back into.
/// The harness does not link the test utilities, so this is not the context in `nes-executable/tests/TestUtils`.
class HarnessSinkPEC final : public PipelineExecutionContext
{
public:
    HarnessSinkPEC(
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        const WorkerThreadId workerThreadId,
        const std::uint64_t numberOfWorkerThreads,
        std::function<void(const TupleBuffer&)> requeue)
        : workerThreadId(workerThreadId)
        , numberOfWorkerThreads(numberOfWorkerThreads)
        , bufferProvider(std::move(bufferProvider))
        , requeue(std::move(requeue))
    {
    }

    /// Set once the sink requests that its stop be called again, until the request is cleared.
    [[nodiscard]] bool repeatRequested() const noexcept { return isRepeatRequested; }

    /// How long the sink requested to wait before its stop is called again.
    [[nodiscard]] std::chrono::milliseconds repeatInterval() const noexcept { return requestedInterval; }

    void clearRepeatRequest() noexcept { isRepeatRequested = false; }

    /// A sink emits nothing downstream.
    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }

    void repeatTask(const TupleBuffer& buffer, const std::chrono::milliseconds retryInterval) override
    {
        if (!buffer)
        {
            /// An empty buffer means that the stop is called again.
            isRepeatRequested = true;
            requestedInterval = retryInterval;
            return;
        }
        /// The engine runs a repeated task after the interval, so a sink whose channel is full retries at that rate, not in a busy loop.
        if (requeue)
        {
            std::this_thread::sleep_for(retryInterval);
            requeue(buffer);
        }
    }

    TupleBuffer allocateTupleBuffer() override { return bufferProvider->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return workerThreadId; }

    [[nodiscard]] std::uint64_t getNumberOfWorkerThreads() const override { return numberOfWorkerThreads; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferProvider; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers) override
    {
        operatorHandlers = handlers;
    }

private:
    WorkerThreadId workerThreadId;
    std::uint64_t numberOfWorkerThreads;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::function<void(const TupleBuffer&)> requeue;
    bool isRepeatRequested{false};
    std::chrono::milliseconds requestedInterval{0};
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
};

}
