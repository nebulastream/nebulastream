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

#include <TestTaskQueue.hpp>

#include <chrono>
#include <cstddef>
#include <memory>
#include <ostream>
#include <ranges>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
bool TestPipelineExecutionContext::emitBuffer(const TupleBuffer& resultBuffer, const ContinuationPolicy continuationPolicy)
{
    if (resultBuffer.getNumberOfTuples() == 0)
    {
        return true;
    }
    switch (continuationPolicy)
    {
        case ContinuationPolicy::NEVER: {
            resultBuffers->at(workerThreadId.getRawValue()).emplace_back(resultBuffer);
            break;
        }
        case ContinuationPolicy::POSSIBLE: {
            resultBuffers->at(workerThreadId.getRawValue()).emplace_back(resultBuffer);
            break;
        }
    }
    return true;
}

TupleBuffer TestPipelineExecutionContext::allocateTupleBuffer()
{
    if (auto buffer = bufferManager->getBufferNoBlocking())
    {
        return buffer.value();
    }
    throw BufferAllocationFailure("Required more buffers in TestTaskQueue than provided.");
}

void TestPipelineExecutionContext::repeatTask(const TupleBuffer&, std::chrono::milliseconds)
{
    PRECONDITION(repeatTaskCallback != nullptr, "Cannot repeat a task without a valid repeatTaskCallback function");
    repeatTaskCallback();
}

void TestPipelineStage::execute(const TupleBuffer& tupleBuffer, PipelineExecutionContext& pec)
{
    for (const auto& [_, taskFunction] : taskSteps)
    {
        taskFunction(tupleBuffer, pec);
    }
}

std::ostream& TestPipelineStage::toString(std::ostream& os) const
{
    if (taskSteps.empty())
    {
        return os << "TestablePipelineStage with steps: []\n";
    }
    os << "TestablePipelineStage with steps: [" << taskSteps.front().first;
    for (const auto& [stepName, _] : taskSteps | std::views::drop(1))
    {
        os << ", " << stepName;
    }
    os << "]\n";
    return os;
}

SingleThreadedTestTaskQueue::SingleThreadedTestTaskQueue(
    std::shared_ptr<BufferManager> bufferProvider, std::shared_ptr<std::vector<std::vector<TupleBuffer>>> resultBuffers)
    : bufferProvider(std::move(bufferProvider)), resultBuffers(std::move(resultBuffers))
{
}

void SingleThreadedTestTaskQueue::processTasks(std::vector<TestPipelineTask> pipelineTasks)
{
    enqueueTasks(std::move(pipelineTasks));
    runTasks();
}

void SingleThreadedTestTaskQueue::enqueueTasks(std::vector<TestPipelineTask> pipelineTasks)
{
    PRECONDITION(not pipelineTasks.empty(), "Test tasks must not be empty.");
    this->eps = pipelineTasks.front().eps;

    for (const auto& testTask : pipelineTasks)
    {
        auto pipelineExecutionContext = std::make_shared<TestPipelineExecutionContext>(
            this->bufferProvider, WorkerThreadId(testTask.workerThreadId.getRawValue()), PipelineId(0), this->resultBuffers);
        /// There is a circular dependency, because the repeatTaskCallback needs to know about the pec and the pec needs to know about the
        /// repeatTaskCallback. The Tasks own the pec. When a tasks goes out of scope, so should the pec and the repeatTaskCallback.
        /// Thus, we give a weak_ptr of the pec to the repeatTaskCallback, which is guaranteed to be alive during the lifetime of the repeatTaskCallback.
        const std::weak_ptr weakPipelineExecutionContext = pipelineExecutionContext;
        auto repeatTaskCallback = [this, testTask, weakPipelineExecutionContext]()
        {
            const auto pecFromWeakCapturedPtr = weakPipelineExecutionContext.lock();
            PRECONDITION(pecFromWeakCapturedPtr != nullptr, "The pipelineExecutionContext must be valid in the repeat callback function");
            tasks.emplace(WorkTask{.task = testTask, .pipelineExecutionContext = pecFromWeakCapturedPtr});
        };
        pipelineExecutionContext->setRepeatTaskCallback(std::move(repeatTaskCallback));
        tasks.emplace(WorkTask{.task = testTask, .pipelineExecutionContext = pipelineExecutionContext});
    }
}

void SingleThreadedTestTaskQueue::runTasks()
{
    tasks.front().task.eps->start(*tasks.front().pipelineExecutionContext);
    while (not tasks.empty())
    {
        const auto [task, pipelineExecutionContext] = std::move(tasks.front());
        tasks.pop();
        task.execute(*pipelineExecutionContext);
    }

    /// Process final tuple
    const auto pipelineExecutionContext = std::make_shared<TestPipelineExecutionContext>(this->bufferProvider, this->resultBuffers);
    eps->stop(*pipelineExecutionContext);
}

MultiThreadedTestTaskQueue::MultiThreadedTestTaskQueue(
    const size_t numberOfThreads,
    const std::vector<TestPipelineTask>& testTasks,
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    std::shared_ptr<std::vector<std::vector<TupleBuffer>>> resultBuffers)
    : threadTasks(testTasks.size())
    , numberOfWorkerThreads(numberOfThreads)
    , completionLatch(numberOfThreads)
    , bufferProvider(std::move(bufferProvider))
    , resultBuffers(std::move(resultBuffers))
    , timer("ConcurrentTestTaskQueue")
{
    PRECONDITION(not testTasks.empty(), "Test tasks must not be empty.");
    /// Store a pointer to the executable pipeline stage to call 'stop()' after completion
    this->eps = testTasks.front().eps;
    /// Start/Compile the executable pipeline stage
    auto pec = TestPipelineExecutionContext(this->bufferProvider, WorkerThreadId(0), PipelineId(0), this->resultBuffers);
    this->eps->start(pec);

    /// Fill the task queue with test tasks
    for (const auto& testTask : testTasks)
    {
        threadTasks.blockingWrite(testTask);
    }
}

void MultiThreadedTestTaskQueue::startProcessing()
{
    timer.start();
    for (size_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        threads.emplace_back([this, i] { threadFunction(i); });
    }
}

void MultiThreadedTestTaskQueue::waitForCompletion()
{
    completionLatch.wait();
    const auto pipelineExecutionContext = std::make_shared<TestPipelineExecutionContext>(this->bufferProvider, this->resultBuffers);
    eps->stop(*pipelineExecutionContext);
    timer.pause();
    NES_DEBUG("Final time to process all tasks: {}ms", timer.getPrintTime());
}

void MultiThreadedTestTaskQueue::threadFunction(const size_t threadIdx)
{
    TestPipelineTask testTask{};
    while (threadTasks.readIfNotEmpty(testTask))
    {
        /// Create the pipeline execution context and set the repeat task callback (in case the thread can't execute the task immediately)
        auto pec = std::make_shared<TestPipelineExecutionContext>(
            this->bufferProvider, WorkerThreadId(WorkerThreadId(threadIdx)), PipelineId(0), this->resultBuffers);
        pec->setRepeatTaskCallback([this, testTask]() { threadTasks.blockingWrite(testTask); });

        testTask.execute(*pec);
    }
    completionLatch.count_down();
}
}
