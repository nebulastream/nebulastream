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
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>

/// A *collecting* `PipelineExecutionContext` for the JSONL <-> native `Formatter`.
///
/// The production sibling of `HarnessSinkPEC` (same shape, copied not linked — the
/// harness must not depend on a tests target): the only delta is `emitBuffer`,
/// which **appends** the emitted buffer to a results vector instead of discarding
/// it. The Formatter runs its Scan->Emit pipeline single-threaded (worker slot 0)
/// and reads the appended buffers back out after each `execute()` / `stop()`, so
/// it needs to capture every emit rather than terminate it like a sink.

namespace NES::ConnTest
{

class FormatterPEC final : public PipelineExecutionContext
{
public:
    explicit FormatterPEC(std::shared_ptr<AbstractBufferProvider> bufferProvider) : bufferProvider(std::move(bufferProvider)) { }

    /// Capture every emitted buffer, in emit order, for the Formatter to drain.
    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
    {
        results.push_back(buffer);
        return true;
    }

    /// The input formatter may ask to re-run a buffer it cannot resolve in isolation
    /// (OpenReturnState::REPEAT). The Formatter only ever feeds buffers in monotonic
    /// sequence order, which never takes that path; a no-op keeps the staged bytes for
    /// a later execute() instead of tripping a missing-callback precondition.
    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { }

    TupleBuffer allocateTupleBuffer() override { return bufferProvider->getBufferBlocking(); }

    TupleBuffer& pinBuffer(TupleBuffer&& tupleBuffer) override
    {
        /// Stable address for the lifetime of this PEC, like the test double.
        pinnedBuffers.push_back(std::make_unique<TupleBuffer>(std::move(tupleBuffer)));
        return *pinnedBuffers.back();
    }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return WorkerThreadId(0); }

    [[nodiscard]] std::uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferProvider; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(0); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers) override
    {
        operatorHandlers = handlers;
    }

    /// The buffers emitted so far, in emit order. The Formatter snapshots its size
    /// before an execute()/stop() and takes the suffix appended by that call.
    [[nodiscard]] std::vector<TupleBuffer>& collected() noexcept { return results; }

private:
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::vector<TupleBuffer> results;
    std::vector<std::unique_ptr<TupleBuffer>> pinnedBuffers;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
};

}
