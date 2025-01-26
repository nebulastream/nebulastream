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
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>

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
        // Todo: add assert
        // Fix workerThreadId handling
        resultBuffers->at(workerThreadId.getRawValue()).emplace_back(resultBuffer);
    }
    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return numberOfWorkerThreads; };
    [[nodiscard]] NES::WorkerThreadId getId() const override { return workerThreadId; }
    NES::Memory::TupleBuffer allocateTupleBuffer() override
    {
        if (auto buffer = bufferManager->getBufferNoBlocking())
        {
            return buffer.value();
        }
        throw NES::BufferAllocationFailure("Required more buffers in TestTaskQueue than provided.");
    }
    std::shared_ptr<NES::Memory::AbstractBufferProvider> getBufferManager() const override { return bufferManager; }
    [[nodiscard]] NES::PipelineId getPipelineId() const override { return  pipelineId; }
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
    void start(NES::Runtime::Execution::PipelineExecutionContext&) override { /* noop */ }
    void stop(NES::Runtime::Execution::PipelineExecutionContext&) override { /* noop */ }
};

class TestablePipelineTask
{
public:
    TestablePipelineTask(
        NES::SequenceNumber sequenceNumber,
        NES::WorkerThreadId workerThreadId,
        NES::Memory::TupleBuffer tupleBuffer,
        std::unique_ptr<NES::Runtime::Execution::ExecutablePipelineStage> eps,
        std::shared_ptr<NES::Memory::BufferManager> bufferManager,
        std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers,
        std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>> operatorHandlers
        )
        : sequenceNumber(sequenceNumber), workerThreadId(workerThreadId), tupleBuffer(std::move(tupleBuffer)), eps(std::move(eps))
    {
        this->pipelineExecutionContext = std::make_unique<TestPipelineExecutionContext>(std::move(bufferManager), resultBuffers);
        this->pipelineExecutionContext->setOperatorHandlers(operatorHandlers);
    }

    void execute() { eps->execute(tupleBuffer, *this->pipelineExecutionContext); }

    void setWorkerThreadId(const uint64_t workerThreadId)
    {
        this->pipelineExecutionContext->workerThreadId = NES::WorkerThreadId(workerThreadId);
    }
    void setPipelineId(const uint64_t pipelineId) { this->pipelineExecutionContext->pipelineId = NES::PipelineId(pipelineId); }

    NES::SequenceNumber sequenceNumber;
    NES::WorkerThreadId workerThreadId;
    std::unique_ptr<TestPipelineExecutionContext> pipelineExecutionContext;

private:
    NES::Memory::TupleBuffer tupleBuffer;
    std::unique_ptr<NES::Runtime::Execution::ExecutablePipelineStage> eps;
};

class TestWorkerThreadPool
{
public:
    TestWorkerThreadPool(const size_t num_threads) : thread_data(num_threads)
    {
        for (size_t i = 0; i < num_threads; ++i)
        {
            threads.emplace_back([this, i] { thread_function(i); });
        }
    }

    // Assign work to a specific thread
    void assign_work(size_t thread_idx, TestablePipelineTask task)
    {
        if (thread_idx >= thread_data.size())
        {
            throw std::out_of_range("Thread index out of range");
        }

        auto& data = thread_data[thread_idx];
        {
            std::lock_guard<std::mutex> lock(data.mtx);
            data.tasks.push(std::move(task));
            data.is_working = true;
        }
        data.cv.notify_one();
    }

    // Wait for all threads to complete their current work
    void wait()
    {
        bool all_done;
        do
        {
            std::unique_lock<std::mutex> wait_lock(wait_mtx);
            all_done = true;

            // Check if any thread is still working
            for (auto& data : thread_data)
            {
                std::lock_guard<std::mutex> lock(data.mtx);
                if (data.is_working || !data.tasks.empty())
                {
                    all_done = false;
                    break;
                }
            }

            if (!all_done)
            {
                // Wait for notification from any thread completing work
                wait_cv.wait(wait_lock);
            }
        } while (!all_done);
    }

    ~TestWorkerThreadPool()
    {
        // Stop all threads
        for (auto& data : thread_data)
        {
            {
                std::lock_guard<std::mutex> lock(data.mtx);
                data.stop = true;
            }
            data.cv.notify_one();
        }
    }

private:
    struct ThreadData
    {
        std::mutex mtx;
        std::condition_variable cv;
        std::queue<TestablePipelineTask> tasks;
        bool stop = false;
        bool is_working = false;
    };

    std::vector<ThreadData> thread_data;
    std::vector<std::jthread> threads;

    std::mutex wait_mtx;
    std::condition_variable wait_cv;

private:
    void thread_function(size_t thread_idx)
    {
        auto& data = thread_data[thread_idx];

        while (true)
        {
            std::unique_lock<std::mutex> lock(data.mtx);
            data.cv.wait(lock, [&data] { return !data.tasks.empty() || data.stop; });

            if (data.stop && data.tasks.empty())
            {
                break;
            }

            while (!data.tasks.empty())
            {
                TestablePipelineTask task = std::move(data.tasks.front());
                data.tasks.pop();

                // Release lock while processing
                lock.unlock();
                task.execute();
                lock.lock();
            }
            data.is_working = false;
            lock.unlock();
            wait_cv.notify_all();
        }
    }
};

// The test executor that controls task execution
class TestTaskQueue
{
public:
    struct TestTask
    {
        NES::WorkerThreadId workerThreadId;
        TestablePipelineTask task;
    };
    TestTaskQueue(const size_t numThreads)
        : numberOfWorkerThreads(numThreads), numPipelines(0), workerThreads(TestWorkerThreadPool(numberOfWorkerThreads))
    {
    }

    ~TestTaskQueue() = default;

    void enqueueTask(const NES::WorkerThreadId workerThreadId, TestablePipelineTask pipelineTask)
    {
        pipelineTask.setWorkerThreadId(numPipelines);
        pipelineTask.setPipelineId(numPipelines);
        ++numPipelines;
        testTasks.push({.workerThreadId = workerThreadId, .task = std::move(pipelineTask)});
    }
    void enqueueTasks(std::vector<TestablePipelineTask> pipelineTasks)
    {
        for (size_t i = 0; auto& pipelineTask : pipelineTasks)
        {
            pipelineTask.setWorkerThreadId(numPipelines);
            pipelineTask.setPipelineId(numPipelines);
            ++numPipelines;
            testTasks.push({.workerThreadId = pipelineTask.workerThreadId, .task = std::move(pipelineTask)});
            ++i;
        }
    }

    void startProcessing()
    {
        while (!testTasks.empty())
        {
            auto [workerThreadId, task] = std::move(testTasks.front());
            testTasks.pop();
            workerThreads.assign_work(workerThreadId.getRawValue(), std::move(task));
            workerThreads.wait();
        }
    }

    void processTasks(std::vector<TestablePipelineTask> pipelineTasks)
    {
        enqueueTasks(std::move(pipelineTasks));
        startProcessing();
    }

private:
    uint64_t numberOfWorkerThreads;
    uint64_t numPipelines;
    std::queue<TestTask> testTasks;
    TestWorkerThreadPool workerThreads;
};