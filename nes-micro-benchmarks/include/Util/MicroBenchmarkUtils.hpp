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

#include <folly/MPMCQueue.h>
#include <nautilus/Engine.hpp>
#include <PipelineExecutionContext.hpp>

#include "../../../nes-execution/include/QueryCompiler/QueryCompilerOptions.hpp"
#include "../../../nes-query-engine/Task.hpp"

namespace NES
{
class MicroBenchmarkUtils
{
public:
    MicroBenchmarkUtils(
        uint64_t selectivity,
        uint64_t bufferSize,
        std::string fieldNameForSelectivity,
        std::shared_ptr<Schema> schemaInput,
        std::shared_ptr<Schema> schemaOutput,
        std::string providerName);

    /// This static function creates tasks and inserts them into the task queue. Each tasks is a simple scan --> filter --> emit pipeline that is compiled.
    /// The schema contains three values: {id, value, ts}. Id is random in the range of 0-1000, giving us the opportunity to set a selectivity easily
    /// The value is strong monotonic increasing like the timestamp
    std::shared_ptr<Runtime::RunningQueryPlanNode> createTasks(
        folly::MPMCQueue<Runtime::WorkTask>& taskQueue,
        const uint64_t numberOfTasks,
        const uint64_t numberOfTuplesPerTask,
        Runtime::WorkEmitter& emitter,
        Memory::AbstractBufferProvider& bufferProvider,
        Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
        std::chrono::nanoseconds sleepDurationPerTuple) const;

private:
    std::unique_ptr<Runtime::Execution::ExecutablePipelineStage> createFilterPipelineExecutableStage() const;
    std::unique_ptr<Runtime::Execution::ExecutablePipelineStage>
    createSleepPipelineExecutableStage(std::chrono::nanoseconds sleepDurationPerTuple) const;


    uint64_t selectivity;
    uint64_t bufferSize;
    std::string fieldNameForSelectivity;
    std::shared_ptr<Schema> schemaInput, schemaOutput;
    std::string providerName;
    uint64_t minRange = 0, maxRange = 1000;
};

class BenchmarkPEC final : public Runtime::Execution::PipelineExecutionContext
{
public:
    std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>>* operatorHandlers = nullptr;
    std::function<void(const Memory::TupleBuffer& tb, ContinuationPolicy)> handler;
    std::shared_ptr<Memory::AbstractBufferProvider> bm;
    size_t numberOfThreads;
    WorkerThreadId threadId;
    PipelineId pipelineId;

    BenchmarkPEC(
        const size_t numberOfThreads,
        const WorkerThreadId threadId,
        const PipelineId pipelineId,
        std::shared_ptr<Memory::AbstractBufferProvider> bm,
        std::function<void(const Memory::TupleBuffer& tb, ContinuationPolicy)> handler)
        : handler(std::move(handler)), bm(std::move(bm)), numberOfThreads(numberOfThreads), threadId(threadId), pipelineId(pipelineId)
    {
    }

    [[nodiscard]] WorkerThreadId getId() const override { return threadId; }
    Memory::TupleBuffer allocateTupleBuffer() override { return bm->getBufferBlocking(); }
    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return numberOfThreads; }
    void emitBuffer(const Memory::TupleBuffer& buffer, const ContinuationPolicy policy) override { handler(buffer, policy); }
    std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager() const override { return bm; }
    PipelineId getPipelineId() const override { return pipelineId; }
    std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>>& getOperatorHandlers() override
    {
        PRECONDITION(operatorHandlers, "Operator Handlers were not set");
        return *operatorHandlers;
    }
    void setOperatorHandlers(std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>>& handlers) override
    {
        operatorHandlers = std::addressof(handlers);
    }
};


class BenchmarkWorkEmitter final : public Runtime::WorkEmitter
{
public:
    ~BenchmarkWorkEmitter() override = default;
    void emitWork(QueryId, const std::shared_ptr<Runtime::RunningQueryPlanNode>&, Memory::TupleBuffer, onComplete, onFailure) override;
    void emitPipelineStart(QueryId, const std::shared_ptr<Runtime::RunningQueryPlanNode>&, onComplete, onFailure) override;
    void emitPendingPipelineStop(QueryId, std::shared_ptr<Runtime::RunningQueryPlanNode>, onComplete, onFailure) override;
    void emitPipelineStop(QueryId, std::unique_ptr<Runtime::RunningQueryPlanNode>, onComplete, onFailure) override;
};
}
