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

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Executable.hpp>
#include <PipelineExecutionContext.hpp>

// Todo: give shared resources of TestTaskQueue?
// - could share tuple buffer <-- is thread safe if using getBufferBlocking
// - could share resultBuffers <-- is thread safe, if dedicated slot (PipelineId/WorkerThreadId)
class TestPipelineExecutionContext : public NES::Runtime::Execution::PipelineExecutionContext
{
public:
    /// Setting invalid values for ids, since we set the values later.
    explicit TestPipelineExecutionContext(
        std::shared_ptr<NES::Memory::BufferManager> bufferManager,
        std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers)
        : numberOfWorkerThreads(0)
        , pipelineId(NES::PipelineId(0))
        , workerThreadId(NES::WorkerThreadId(0))
        , bufferManager(std::move(bufferManager))
        , resultBuffers(resultBuffers)
    {
    }

    void emitBuffer(const NES::Memory::TupleBuffer& resultBuffer, ContinuationPolicy) override
    {
        resultBuffers->at(pipelineId.getRawValue()).emplace_back(resultBuffer);
    }
    uint64_t getNumberOfWorkerThreads() override { return numberOfWorkerThreads; }
    [[nodiscard]] NES::WorkerThreadId getId() const override { return workerThreadId; }
    NES::Memory::TupleBuffer allocateTupleBuffer() override
    {
        if (auto buffer = bufferManager->getBufferNoBlocking())
        {
            return buffer.value();
        }
        throw NES::BufferAllocationFailure("Required more buffers in TestTaskQueue than provided.");
    }
    std::shared_ptr<NES::Memory::AbstractBufferProvider> getBufferManager() override { return bufferManager; }
    NES::PipelineId getPipelineID() override { return pipelineId; }
    std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }
    void setOperatorHandlers(std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>>& operatorHandlers) override
    {
        this->operatorHandlers = operatorHandlers;
    }

    uint64_t numberOfWorkerThreads;
    NES::PipelineId pipelineId;
    NES::WorkerThreadId workerThreadId;

private:
    std::shared_ptr<NES::Memory::BufferManager> bufferManager; //Sharing resource with TestTaskQueue
    std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>> operatorHandlers; //Todo: what to do with OperatorHandlers?
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers;
};

class TestablePipelineStage : public NES::Runtime::Execution::ExecutablePipelineStage
{
public:
    using ExecuteFunction = std::function<void(const NES::Memory::TupleBuffer&, NES::Runtime::Execution::PipelineExecutionContext&)>;
    TestablePipelineStage() = default;
    TestablePipelineStage(std::string step_name, ExecuteFunction testTask) { addStep(std::move(step_name), std::move(testTask)); }

    void addStep(std::string step_name, ExecuteFunction testTask) { taskSteps.push_back({step_name, std::move(testTask)}); }

    void execute(const NES::Memory::TupleBuffer& tupleBuffer, NES::Runtime::Execution::PipelineExecutionContext& pec) override
    {
        for (const auto& step : taskSteps)
        {
            step.second(tupleBuffer, pec);
        }
    }

private:
    std::vector<std::pair<std::string, ExecuteFunction>> taskSteps;

    /// Member functions
    std::ostream& toString(std::ostream& os) const override { return os; } //Todo
    uint32_t start(NES::Runtime::Execution::PipelineExecutionContext&) override { return 0; }
    uint32_t stop(NES::Runtime::Execution::PipelineExecutionContext&) override { return 0; }
};

class TestablePipelineTask
{
public:
    TestablePipelineTask(
        NES::SequenceNumber sequenceNumber,
        NES::Memory::TupleBuffer tupleBuffer,
        std::unique_ptr<NES::Runtime::Execution::ExecutablePipelineStage> eps,
        std::shared_ptr<NES::Memory::BufferManager> bufferManager,
        std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers)
        : sequenceNumber(sequenceNumber), tupleBuffer(std::move(tupleBuffer)), eps(std::move(eps))
    {
        this->pipelineExecutionContext = std::make_unique<TestPipelineExecutionContext>(std::move(bufferManager), resultBuffers);
    }

    void execute() { eps->execute(tupleBuffer, *this->pipelineExecutionContext); }

    void setWorkerThreadId(const uint64_t workerThreadId)
    {
        this->pipelineExecutionContext->workerThreadId = NES::WorkerThreadId(workerThreadId);
    }
    void setPipelineId(const uint64_t pipelineId) { this->pipelineExecutionContext->pipelineId = NES::PipelineId(pipelineId); }

    // Todo: allow setting operator handlers in constructor?
    void setOperatorHandlers(std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>> operatorHandlers) const
    {
        this->pipelineExecutionContext->setOperatorHandlers(operatorHandlers);
    }

    NES::SequenceNumber sequenceNumber;
    std::unique_ptr<TestPipelineExecutionContext> pipelineExecutionContext;
private:
    NES::Memory::TupleBuffer tupleBuffer;
    std::unique_ptr<NES::Runtime::Execution::ExecutablePipelineStage> eps;
};

// The test executor that controls task execution
class TestTaskQueue
{
public:
    TestTaskQueue(const size_t numThreads) : numberOfWorkerThreads(numThreads), numPipelines(0)
    {
        /// Initialize with size of worker threads, each thread gets one task
        testTasks.resize(numberOfWorkerThreads);
    }

    ~TestTaskQueue() = default;

    void enqueueTask(std::unique_ptr<TestablePipelineTask> pipelineTask)
    {
        pipelineTask->setWorkerThreadId(numPipelines);
        pipelineTask->setPipelineId(numPipelines);
        ++numPipelines;
        testTasks.at(pipelineTask->sequenceNumber.getRawValue()) = std::move(pipelineTask);
        // testTasks.emplace_back(std::move(pipelineTask));
    }

    void startProcessing()
    {
        for (size_t i = 0; i < numberOfWorkerThreads; ++i)
        {
            workerThreads.emplace_back([this, i] { workerLoop(i); });
        }
    }

    /// Wait for all tasks to complete
    void waitForCompletion()
    {
        while (numThreadsCompleted != numberOfWorkerThreads)
        {
            /// busy wait
        }
    }

private:
    void workerLoop(const size_t taskIndex)
    {
        INVARIANT(taskIndex < testTasks.size(), "Cannot execute task {} when there are only {} tasks.", taskIndex, testTasks.size());
        testTasks.at(taskIndex)->execute();
        ++numThreadsCompleted;
    }
    std::atomic<size_t> numThreadsCompleted;
    uint64_t numberOfWorkerThreads;
    uint64_t numPipelines;
    std::vector<std::unique_ptr<TestablePipelineTask>> testTasks;
    std::vector<std::jthread> workerThreads;
};