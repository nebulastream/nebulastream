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

#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <vector>
#include <memory>
#include <Executable.hpp>

// Forward declarations
class TestableTask;
class TestTaskExecutor;

// Represents a barrier that tasks can wait on
class Barrier {
public:
    explicit Barrier(size_t count) : threshold(count), count(count), generation(0) {}

    void wait() {
        std::unique_lock<std::mutex> lock(mtx);
        auto gen = generation;
        if (--count == 0) {
            generation++;
            count = threshold;
            cv.notify_all();
        } else {
            cv.wait(lock, [this, gen] { return gen != generation; });
        }
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    size_t threshold;
    size_t count;
    size_t generation;
};

class TestPipelineExecutionContext : public NES::Runtime::Execution::PipelineExecutionContext
{
public:
    void emitBuffer(const NES::Memory::TupleBuffer&, ContinuationPolicy) override
    {
        // Todo: implement mocked emitBuffer
    }
    [[nodiscard]] NES::WorkerThreadId getId() const override
    {
        return NES::WorkerThreadId(0); // Todo: invalid
    }
    NES::Memory::TupleBuffer allocateTupleBuffer() override
    {
        // Todo: implement allocateBuffer function
        // -> should have BufferManager/Pool as member that provides buffers
    }
    uint64_t getNumberOfWorkerThreads() override
    {
        return 1; // Todo <-- needs to come from TestTaskExecutor!
    }
    std::shared_ptr<NES::Memory::AbstractBufferProvider> getBufferManager() override
    {
        // Todo: need to set BufferManager in constructor
    }
    NES::PipelineId getPipelineID() override
    {
        return NES::PipelineId(0); // Todo: invalid
    }
    std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>>& getOperatorHandlers() override
    {
        // Todo: mock operator handler
    }
    void setOperatorHandlers(std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>>&) override
    {
        // todo allow to set mocked operator handlers
    }
};

// A task that can be controlled for testing
class TestableTask {
public:
    // using TaskFunction = std::function<void()>;
    using TaskPipeline = std::unique_ptr<NES::Runtime::Execution::ExecutablePipelineStage>;

    TestableTask(std::string name) : name_(name), completed_(false) {}

    void addStep(std::string step_name, TaskPipeline taskPipeline) {
        steps_.push_back({step_name, std::move(taskPipeline)});
    }

    // Todo: mock PipelineExecutionContext -> see above!
    // Todo: how to handle TupleBuffers?
    void executeStep(size_t step_index) {
        if (step_index < steps_.size()) {
            steps_[step_index].second->execute( /* Todo: TupleBuffer, PipelineExecutionContext */);
        }
    }

    void executeAllSteps() {
        for (auto& step : steps_) {
            step.second();
        }
        completed_ = true;
    }

    bool isCompleted() const { return completed_; }
    const std::string& getName() const { return name_; }
    size_t getStepCount() const { return steps_.size(); }

private:
    std::string name_;
    std::vector<std::pair<std::string, TaskPipeline>> steps_;
    bool completed_;
};

// The test executor that controls task execution
class TestTaskExecutor {
public:
    TestTaskExecutor(size_t thread_count = 1) : running_(true) {
        for (size_t i = 0; i < thread_count; ++i) {
            workers_.emplace_back([this] { workerLoop(); });
        }
    }

    ~TestTaskExecutor() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            running_ = false;
        }
        cv_.notify_all();
        for (auto& worker : workers_) {
            worker.join();
        }
    }

    // Add a task to the queue
    void enqueueTask(std::unique_ptr<TestableTask> task) {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.push(std::move(task));
        cv_.notify_one();
    }

    // Execute specific step for all tasks
    void executeStepForAllTasks(size_t step_index) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::unique_ptr<TestableTask>> temp_tasks;

        while (!tasks_.empty()) {
            auto task = std::move(tasks_.front());
            tasks_.pop();
            task->executeStep(step_index);
            temp_tasks.emplace_back(std::move(task));
        }

        for (auto& task : temp_tasks) {
            tasks_.emplace(std::move(task));
        }
    }

    // Wait for all tasks to complete
    void waitForCompletion() {
        while (true) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (tasks_.empty()) {
                break;
            }
            std::this_thread::yield();
        }
    }

private:
    void workerLoop() {
        while (true) {
            std::unique_ptr<TestableTask> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] {
                    return !running_ || !tasks_.empty();
                });

                if (!running_ && tasks_.empty()) {
                    return;
                }

                if (!tasks_.empty()) {
                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
            }

            if (task) {
                task->executeAllSteps();
            }
        }
    }

    std::queue<std::unique_ptr<TestableTask>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::thread> workers_;
    bool running_;
};