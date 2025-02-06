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


class TestablePipelineTask;
class TestPipelineExecutionContext : public NES::Runtime::Execution::PipelineExecutionContext
{
public:
    /// Setting invalid values for ids, since we set the values later.
    explicit TestPipelineExecutionContext(
        std::shared_ptr<NES::Memory::BufferManager> bufferManager)
        : workerThreadId(NES::WorkerThreadId(0)), pipelineId(NES::PipelineId(0))
        , bufferManager(std::move(bufferManager))
    {
    }

    void emitBuffer(const NES::Memory::TupleBuffer& resultBuffer, ContinuationPolicy continuationPolicy) override
    {
        if (continuationPolicy == ContinuationPolicy::REPEAT)
        {
            repeatTaskCallback();
        }
        else
        {
            resultBufferPtr->at(workerThreadId.getRawValue()).emplace_back(resultBuffer);
        }
    }

    NES::Memory::TupleBuffer allocateTupleBuffer() override
    {
        if (auto buffer = bufferManager->getBufferNoBlocking())
        {
            return buffer.value();
        }
        throw NES::BufferAllocationFailure("Required more buffers in TestTaskQueue than provided.");
    }
    void setTemporaryResultBuffers(std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBufferPtr)
    {
        this->resultBufferPtr = std::move(resultBufferPtr);
    }
    void setRepeatTaskCallback(std::function<void()> repeatTaskCallback)
    {
        this->repeatTaskCallback = std::move(repeatTaskCallback);
    }

    [[nodiscard]] NES::WorkerThreadId getId() const override { return workerThreadId; };
    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1;};
    std::shared_ptr<NES::Memory::AbstractBufferProvider> getBufferManager() const override { return bufferManager; }
    [[nodiscard]] NES::PipelineId getPipelineId() const override { return  pipelineId; }
    std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }
    void setOperatorHandlers(std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>>& operatorHandlers) override
    {
        this->operatorHandlers = operatorHandlers;
    }

    NES::WorkerThreadId workerThreadId;
    NES::PipelineId pipelineId;

private:
    std::function<void()> repeatTaskCallback;
    std::shared_ptr<NES::Memory::BufferManager> bufferManager; //Sharing resource with TestTaskQueue
    std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>> operatorHandlers; //Todo: what to do with OperatorHandlers?
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBufferPtr;
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
        NES::WorkerThreadId workerThreadId,
        NES::SequenceNumber sequenceNumber,
        NES::Memory::TupleBuffer tupleBuffer,
        std::shared_ptr<NES::Runtime::Execution::ExecutablePipelineStage> eps,
        std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>>)
        : isFinalTask(false), workerThreadId(workerThreadId), sequenceNumber(sequenceNumber), tupleBuffer(std::move(tupleBuffer)), eps(std::move(eps))
    {}
    TestablePipelineTask(
        bool isFinalTask,
        NES::WorkerThreadId workerThreadId,
        std::shared_ptr<NES::Runtime::Execution::ExecutablePipelineStage> eps)
        : isFinalTask(isFinalTask), workerThreadId(workerThreadId), sequenceNumber(NES::SequenceNumber::INVALID), tupleBuffer(NES::Memory::TupleBuffer{}), eps(std::move(eps))
    {}

    void execute(TestPipelineExecutionContext& pipelineExecutionContext) { eps->execute(tupleBuffer, pipelineExecutionContext); }
    std::shared_ptr<NES::Runtime::Execution::ExecutablePipelineStage> getExecutablePipelineStage() const { return eps; }
public:
    bool isFinalTask;
    NES::WorkerThreadId workerThreadId;
    NES::SequenceNumber sequenceNumber;
    std::shared_ptr<TestPipelineExecutionContext> pipelineExecutionContext;
    NES::Memory::TupleBuffer tupleBuffer;

private:
    std::shared_ptr<NES::Runtime::Execution::ExecutablePipelineStage> eps;
};

class TestWorkerThreadPool
{
public:
    struct WorkTask
    {
        TestablePipelineTask task;
        std::shared_ptr<TestPipelineExecutionContext> pipelineExecutionContext;
    };

    TestWorkerThreadPool(const size_t num_threads) : threadData(num_threads)
    {
        for (size_t i = 0; i < num_threads; ++i)
        {
            threads.emplace_back([this, i] { thread_function(i); });
        }
    }

    // Assign work to a specific thread
    void assign_work(size_t thread_idx, WorkTask task)
    {
        if (thread_idx >= threadData.size())
        {
            throw std::out_of_range("Thread index out of range");
        }

        auto& data = threadData[thread_idx];
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
            for (auto& data : threadData)
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
        for (auto& data : threadData)
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
        std::queue<WorkTask> tasks;
        bool stop = false;
        bool is_working = false;
    };

    std::vector<ThreadData> threadData;
    std::vector<std::jthread> threads;

    std::mutex wait_mtx;
    std::condition_variable wait_cv;

private:
    void thread_function(size_t thread_idx)
    {
        auto& data = threadData[thread_idx];

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
                WorkTask task = std::move(data.tasks.front());
                data.tasks.pop();

                // Release lock while processing
                lock.unlock();
                if (not(task.task.isFinalTask))
                {
                    task.task.execute(*task.pipelineExecutionContext);
                }
                else
                {
                    task.task.getExecutablePipelineStage()->stop(*task.pipelineExecutionContext);
                }
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
    enum class ProcessingMode : uint8_t
    {
        SEQUENTIAL,
        ASYNCHRONOUS
    };

    TestTaskQueue(const size_t numThreads, std::shared_ptr<NES::Memory::BufferManager> bufferProvider, std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers)
        : numberOfWorkerThreads(numThreads), numPipelines(0), workerThreads(TestWorkerThreadPool(numberOfWorkerThreads)), bufferProvider(std::move(bufferProvider)), resultBuffers(std::move(resultBuffers))
    {
    }

    ~TestTaskQueue() = default;


    void processTasks(std::vector<TestablePipelineTask> pipelineTasks, const ProcessingMode processingMode)
    {
        enqueueTasks(std::move(pipelineTasks));
        startProcessing(processingMode);
    }

    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> takeResultBuffers() const
    {
        return std::move(resultBuffers);
    }

private:
    void enqueueTasks(std::vector<TestablePipelineTask> pipelineTasks)
    {
        for (auto& pipelineTask : pipelineTasks)
        {
            testTasks.push(std::move(pipelineTask));
        }
    }

    void startProcessing(const ProcessingMode processingMode)
    {
        auto lastWorkerThread = NES::WorkerThreadId(NES::WorkerThreadId::INITIAL);
        auto pointerToSharedExecutablePipelineStage = testTasks.front().getExecutablePipelineStage();
        while (not(testTasks.empty()))
        {
            auto currentTestTask = std::move(testTasks.front());
            /// Create a callback that allows to thread-safely add a task back to the task queue
            auto repeatTaskCallback = [this, currentTestTask]()
            {
                std::scoped_lock lock(addNewTaskMutex);
                testTasks.emplace(std::move(currentTestTask));
            };

            lastWorkerThread = currentTestTask.workerThreadId;
            const auto responsibleWorkerThread = currentTestTask.workerThreadId;
            testTasks.pop();
            auto pipelineExecutionContext = std::make_shared<TestPipelineExecutionContext>(bufferProvider);
            pipelineExecutionContext->workerThreadId = NES::WorkerThreadId(responsibleWorkerThread);
            pipelineExecutionContext->pipelineId = NES::PipelineId(numPipelines++);
            pipelineExecutionContext->setTemporaryResultBuffers(resultBuffers);
            pipelineExecutionContext->setRepeatTaskCallback(std::move(repeatTaskCallback));

            const auto workTask = TestWorkerThreadPool::WorkTask{.task = std::move(currentTestTask), .pipelineExecutionContext = pipelineExecutionContext};
            workerThreads.assign_work(responsibleWorkerThread.getRawValue(), std::move(workTask));
            if (processingMode == ProcessingMode::SEQUENTIAL)
            {
                workerThreads.wait();
            }
        }
        /// Process final 'dummy' task that flushes pipeline <--- Todo: overly specific for formatter? make optional
        const auto idxOfWorkerThreadForFlushTask = lastWorkerThread.getRawValue();
        auto pipelineExecutionContext = std::make_shared<TestPipelineExecutionContext>(bufferProvider);
        pipelineExecutionContext->workerThreadId = NES::WorkerThreadId(idxOfWorkerThreadForFlushTask);
        pipelineExecutionContext->pipelineId = NES::PipelineId(numPipelines++);
        pipelineExecutionContext->setTemporaryResultBuffers(resultBuffers);

        const auto flushTask = TestablePipelineTask{true, NES::WorkerThreadId(idxOfWorkerThreadForFlushTask), std::move(pointerToSharedExecutablePipelineStage)};
        const auto workTask = TestWorkerThreadPool::WorkTask{.task = std::move(flushTask), .pipelineExecutionContext = pipelineExecutionContext};
        workerThreads.assign_work(idxOfWorkerThreadForFlushTask, std::move(workTask));
        workerThreads.wait();

    }

private:
    uint64_t numberOfWorkerThreads;
    uint64_t numPipelines;
    std::mutex addNewTaskMutex;
    std::queue<TestablePipelineTask> testTasks;
    TestWorkerThreadPool workerThreads;
    std::shared_ptr<NES::Memory::BufferManager> bufferProvider; //Todo: change to AbstractBufferProvider?
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers;
};