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
#include <unordered_map>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>

/// A per-worker `PipelineExecutionContext` double for `--mode=sink`.
///
/// Modeled on `nes-executable/tests/TestUtils/TestTaskQueue.hpp`'s
/// `TestPipelineExecutionContext` (copied, not linked — the harness must
/// not depend on a tests target). Each drain thread owns its own instance,
/// so there is no shared PEC state across threads; the only cross-thread
/// channel is the `repeatTask` re-enqueue callback the harness installs,
/// which writes back into the shared work queue.

namespace NES::ConnTest
{

class HarnessSinkPEC final : public PipelineExecutionContext
{
public:
    HarnessSinkPEC(
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        const WorkerThreadId workerThreadId,
        const PipelineId pipelineId,
        const std::uint64_t numberOfWorkerThreads)
        : workerThreadId(workerThreadId)
        , pipelineId(pipelineId)
        , numberOfWorkerThreads(numberOfWorkerThreads)
        , bufferProvider(std::move(bufferProvider))
    {
    }

    /// Install the re-enqueue sink for `repeatTask(validBuffer, ...)`. The
    /// harness wires this to `queue.blockingWrite(TupleBuffer(b))`.
    void setRepeatTaskCallback(std::function<void(const TupleBuffer&)> callback) { repeatTaskCallback = std::move(callback); }

    /// True once `repeatTask({}, ...)` was called with an empty buffer since
    /// the last `clearRepeatRequest()`. The stop() loop polls this to learn
    /// the sink wants another stop() attempt (delivery tokens still pending).
    [[nodiscard]] bool repeatRequested() const noexcept { return isRepeatRequested; }

    void clearRepeatRequest() noexcept { isRepeatRequested = false; }

    /// A terminal sink never emits downstream; report success unconditionally.
    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }

    void repeatTask(const TupleBuffer& buffer, std::chrono::milliseconds) override
    {
        if (!buffer)
        {
            /// Empty buffer: the sink (stop()) wants to be called again.
            isRepeatRequested = true;
            return;
        }
        /// Valid buffer: re-enqueue it for another drain attempt (execute()).
        if (repeatTaskCallback)
        {
            repeatTaskCallback(buffer);
        }
    }

    TupleBuffer allocateTupleBuffer() override { return bufferProvider->getBufferBlocking(); }

    TupleBuffer& pinBuffer(TupleBuffer&& tupleBuffer) override
    {
        /// Stable address for the lifetime of this PEC, like the test double.
        pinnedBuffers.push_back(std::make_unique<TupleBuffer>(std::move(tupleBuffer)));
        return *pinnedBuffers.back();
    }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return workerThreadId; }

    [[nodiscard]] std::uint64_t getNumberOfWorkerThreads() const override { return numberOfWorkerThreads; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferProvider; }

    [[nodiscard]] PipelineId getPipelineId() const override { return pipelineId; }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers) override
    {
        operatorHandlers = handlers;
    }

private:
    WorkerThreadId workerThreadId;
    PipelineId pipelineId;
    std::uint64_t numberOfWorkerThreads;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::function<void(const TupleBuffer&)> repeatTaskCallback;
    bool isRepeatRequested{false};
    std::vector<std::unique_ptr<TupleBuffer>> pinnedBuffers;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
};

}
