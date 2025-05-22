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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <latch>
#include <memory>
#include <ostream>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Timer.hpp>
#include <folly/MPMCQueue.h>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution
{

/// Mocks 'PipelineExecutionContext'. Allows to control how to emit and allocate buffers. Currently, when getting data in its emit function,
/// the TestPipelineExecutionContext puts it into a vector owned by an external instance that can then inspect the buffers that the
/// TestPipelineExecutionContext emitted.
class TestPipelineExecutionContext final : public PipelineExecutionContext
{
public:
    TestPipelineExecutionContext()
        : workerThreadId(WorkerThreadId(WorkerThreadId::INVALID)), pipelineId(PipelineId(PipelineId::INVALID)) { };

    /// Setting invalid values for ids, since we set the values later.
    explicit TestPipelineExecutionContext(
        std::shared_ptr<Memory::AbstractBufferProvider> bufferManager,
        const WorkerThreadId workerThreadId,
        const PipelineId pipelineId,
        std::shared_ptr<std::vector<std::vector<Memory::TupleBuffer>>> resultBuffers)
        : workerThreadId(workerThreadId)
        , pipelineId(pipelineId)
        , bufferManager(std::move(bufferManager))
        , resultBuffers(std::move(resultBuffers))
    {
        PRECONDITION(this->resultBuffers != nullptr, "Result buffer vector is a nullptr");
        PRECONDITION(
            this->workerThreadId.getRawValue() < this->resultBuffers->size(),
            "Result buffer vector covers {} threads, but tried to create a context for thread {}",
            this->resultBuffers->size(),
            this->workerThreadId.getRawValue());
    }

    /// Setting invalid values for ids, since we set the values later.
    explicit TestPipelineExecutionContext(
        std::shared_ptr<Memory::AbstractBufferProvider> bufferManager,
        std::shared_ptr<std::vector<std::vector<Memory::TupleBuffer>>> resultBufferPtr)
        : workerThreadId(WorkerThreadId(0))
        , pipelineId(PipelineId(0))
        , bufferManager(std::move(bufferManager))
        , resultBuffers(std::move(resultBufferPtr))
    {
        PRECONDITION(this->resultBuffers != nullptr, "Result buffer vector is a nullptr");
        PRECONDITION(
            this->workerThreadId.getRawValue() < this->resultBuffers->size(),
            "Result buffer vector covers {} threads, but tried to create a context for thread {}",
            this->resultBuffers->size(),
            this->workerThreadId.getRawValue());
    }

    /// if buffer contains data, writes it into the result buffer vector, otherwise, calls the 'repeatTaskCallback'
    bool emitBuffer(const Memory::TupleBuffer& resultBuffer, ContinuationPolicy continuationPolicy) override;

    Memory::TupleBuffer allocateTupleBuffer() override;

    void setRepeatTaskCallback(std::function<void()> repeatTaskCallback) { this->repeatTaskCallback = std::move(repeatTaskCallback); }

    [[nodiscard]] WorkerThreadId getId() const override { return workerThreadId; };

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 0; }; /// dummy implementation for  pure virtual function

    [[nodiscard]] std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return pipelineId; }

    std::vector<std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }

    void setOperatorHandlers(std::vector<std::shared_ptr<OperatorHandler>>& operatorHandlers) override
    {
        this->operatorHandlers = operatorHandlers;
    }

    WorkerThreadId workerThreadId;
    PipelineId pipelineId;

private:
    std::function<void()> repeatTaskCallback;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferManager;
    std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers;
    /// Different threads have different TestPipelineExecutionContexts. All threads share the same pointer to the result buffers.
    /// Each thread writes its own results in a dedicated slot. This keeps results in a single place and does not require awkward logic
    /// to get the result buffers out of the TestPipelineExecutionContexts.
    std::shared_ptr<std::vector<std::vector<Memory::TupleBuffer>>> resultBuffers;
};

/// Represents a single ExecutablePipelineStage with multiple functions ('taskSteps').
/// Executes all 'taskSteps' in its 'execute' function.
class TestablePipelineStage : public ExecutablePipelineStage
{
public:
    using ExecuteFunction = std::function<void(const Memory::TupleBuffer&, PipelineExecutionContext&)>;
    TestablePipelineStage() = default;

    TestablePipelineStage(const std::string& stepName, ExecuteFunction testTask) { addStep(stepName, std::move(testTask)); }

    void addStep(const std::string& stepName, ExecuteFunction testTask) { taskSteps.emplace_back(stepName, std::move(testTask)); }

    /// executes all task steps (ExecuteFunctions)
    void execute(const Memory::TupleBuffer& tupleBuffer, PipelineExecutionContext& pec) override;

private:
    std::vector<std::pair<std::string, ExecuteFunction>> taskSteps;

    std::ostream& toString(std::ostream& os) const override;

    void start(PipelineExecutionContext&) override { /* noop */ }

    void stop(PipelineExecutionContext&) override { /* noop */ }
};

/// Maps a pipeline task to a specific worker thread and therefore allows a test task queue to execute a specific task on a specific worker.
struct TestablePipelineTask
{
    TestablePipelineTask() : workerThreadId(WorkerThreadId(WorkerThreadId::INVALID)) { };

    TestablePipelineTask(const WorkerThreadId workerThreadId, Memory::TupleBuffer tupleBuffer, std::shared_ptr<ExecutablePipelineStage> eps)
        : workerThreadId(workerThreadId), tupleBuffer(std::move(tupleBuffer)), eps(std::move(eps))
    {
    }

    TestablePipelineTask(const WorkerThreadId workerThreadId, std::shared_ptr<ExecutablePipelineStage> eps)
        : workerThreadId(workerThreadId), eps(std::move(eps))
    {
    }

    /// Executes the TestablePipelineTask, passing the pipelineExecutionContext into its 'execute' function
    void execute(TestPipelineExecutionContext& pec) const { eps->execute(tupleBuffer, pec); }

    WorkerThreadId workerThreadId;
    Memory::TupleBuffer tupleBuffer;
    std::shared_ptr<ExecutablePipelineStage> eps;
};

struct WorkTask
{
    TestablePipelineTask task;
    std::shared_ptr<TestPipelineExecutionContext> pipelineExecutionContext;
};

/// Processes TestablePipelineTasks sequentially. May use more than one thread to process the tasks. Allows to verify
/// that a specific order of tasks leads to the correct result and different threads influence their state in an
/// expected way.
class SingleThreadedTestTaskQueue
{
public:
    SingleThreadedTestTaskQueue(
        std::shared_ptr<Memory::BufferManager> bufferProvider,
        std::shared_ptr<std::vector<std::vector<Memory::TupleBuffer>>> resultBuffers);

    ~SingleThreadedTestTaskQueue() = default;

    /// Sequentially processes pipeline tasks on respective threads. Stops all threads.
    void processTasks(std::vector<TestablePipelineTask> pipelineTasks);

private:
    std::queue<WorkTask> tasks;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::shared_ptr<std::vector<std::vector<Memory::TupleBuffer>>> resultBuffers;

    std::shared_ptr<ExecutablePipelineStage> eps;

    /// Sets up all tasks for the threads.
    void enqueueTasks(std::vector<TestablePipelineTask> pipelineTasks);
    /// Executes tasks on respective threads.
    void runTasks();
};

/// Takes TestablePipelineTask and a number of threads. Creates WorkTasks from the TestablePipelineTasks and writes the WorkTasks into an
/// MPMC queue. On calling, 'startProcessing()' the threads start to concurrently process WorkTasks from the MPMC queue, until it is empty.
class MultiThreadedTestTaskQueue
{
public:
    MultiThreadedTestTaskQueue(
        size_t numberOfThreads,
        const std::vector<TestablePipelineTask>& testTasks,
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
        std::shared_ptr<std::vector<std::vector<Memory::TupleBuffer>>> resultBuffers);

    /// Activates threads which start to concurrently process the WorkTasks in the MPMC queue.
    void startProcessing();

    /// Wait for all threads to complete all WorkTasks in the MPMC queue.
    void waitForCompletion();

private:
    folly::MPMCQueue<WorkTask> threadTasks;
    uint64_t numberOfWorkerThreads;
    std::latch completionLatch;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::shared_ptr<std::vector<std::vector<Memory::TupleBuffer>>> resultBuffers;
    std::shared_ptr<ExecutablePipelineStage> eps;
    std::vector<std::jthread> threads;
    Timer<std::chrono::microseconds> timer;


    void threadFunction(size_t threadIdx);
};

}
