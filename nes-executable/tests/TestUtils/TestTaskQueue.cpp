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

#include <cstddef>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <TestTaskQueue.hpp>

namespace NES::Runtime::Execution
{
bool TestPipelineExecutionContext::emitBuffer(const NES::Memory::TupleBuffer& resultBuffer, const ContinuationPolicy continuationPolicy)
{
    switch (continuationPolicy)
    {
        case ContinuationPolicy::REPEAT: {
            PRECONDITION(repeatTaskCallback != nullptr, "Cannot repeat a task without a valid repeatTaskCallback function");
            repeatTaskCallback();
            break;
        }
        case ContinuationPolicy::NEVER: {
            resultBuffers->at(workerThreadId.getRawValue()).emplace_back(resultBuffer);
            break;
        }
        case ContinuationPolicy::POSSIBLE: {
            resultBuffers->at(workerThreadId.getRawValue()).emplace_back(resultBuffer);
            break;
        }
        default:
            throw NES::NotImplemented(
                "Not supporting ContinuationPolicy {} in TestPipelineExecutionContext.", magic_enum::enum_name(continuationPolicy));
    }
    return true;
}

NES::Memory::TupleBuffer TestPipelineExecutionContext::allocateTupleBuffer()
{
    if (auto buffer = bufferManager->getBufferNoBlocking())
    {
        return buffer.value();
    }
    throw NES::BufferAllocationFailure("Required more buffers in TestTaskQueue than provided.");
}

void TestablePipelineStage::execute(const NES::Memory::TupleBuffer& tupleBuffer, NES::Runtime::Execution::PipelineExecutionContext& pec)
{
    for (const auto& [_, taskFunction] : taskSteps)
    {
        taskFunction(tupleBuffer, pec);
    }
}

std::ostream& TestablePipelineStage::toString(std::ostream& os) const
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
    std::shared_ptr<NES::Memory::BufferManager> bufferProvider,
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers)
    : bufferProvider(std::move(bufferProvider)), resultBuffers(std::move(resultBuffers))
{
}

void SingleThreadedTestTaskQueue::processTasks(std::vector<TestablePipelineTask> pipelineTasks)
{
    enqueueTasks(std::move(pipelineTasks));
    runTasks();
}

void SingleThreadedTestTaskQueue::enqueueTasks(std::vector<TestablePipelineTask> pipelineTasks)
{
    PRECONDITION(not pipelineTasks.empty(), "Test tasks must not be empty.");
    this->eps = pipelineTasks.front().eps;

    for (const auto& testTask : pipelineTasks)
    {
        auto pipelineExecutionContext = std::make_shared<TestPipelineExecutionContext>(
            this->bufferProvider, NES::WorkerThreadId(testTask.workerThreadId.getRawValue()), PipelineId(0), this->resultBuffers);
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
    const std::vector<TestablePipelineTask>& testTasks,
    std::shared_ptr<NES::Memory::AbstractBufferProvider> bufferProvider,
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers)
    : threadTasks(testTasks.size())
    , numberOfWorkerThreads(numberOfThreads)
    , completionLatch(numberOfThreads)
    , bufferProvider(std::move(bufferProvider))
    , resultBuffers(std::move(resultBuffers))
    , timer("ConcurrentTestTaskQueue")
{
    PRECONDITION(not testTasks.empty(), "Test tasks must not be empty.");
    this->eps = testTasks.front().eps;

    for (const auto& testTask : testTasks)
    {
        auto pipelineExecutionContext = std::make_shared<TestPipelineExecutionContext>(
            this->bufferProvider, WorkerThreadId(WorkerThreadId(0)), PipelineId(0), this->resultBuffers);
        /// There is a circular dependency, because the repeatTaskCallback needs to know about the pec and the pec needs to know about the
        /// repeatTaskCallback. The Tasks own the pec. When a tasks goes out of scope, so should the pec and the repeatTaskCallback.
        /// Thus, we give a weak_ptr of the pec to the repeatTaskCallback, which is guaranteed to be alive during the lifetime of the repeatTaskCallback.
        const std::weak_ptr weakPipelineExecutionContext = pipelineExecutionContext;
        auto repeatTaskCallback = [this, testTask, weakPipelineExecutionContext]()
        {
            const auto pecFromWeakCapturedPtr = weakPipelineExecutionContext.lock();
            PRECONDITION(pecFromWeakCapturedPtr != nullptr, "The pipelineExecutionContext must be valid in the repeat callback function");
            threadTasks.blockingWrite(WorkTask{.task = testTask, .pipelineExecutionContext = pecFromWeakCapturedPtr});
        };
        pipelineExecutionContext->setRepeatTaskCallback(std::move(repeatTaskCallback));
        threadTasks.blockingWrite(WorkTask{.task = testTask, .pipelineExecutionContext = pipelineExecutionContext});
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
    WorkTask workTask{};
    while (threadTasks.readIfNotEmpty(workTask))
    {
        workTask.pipelineExecutionContext->workerThreadId = NES::WorkerThreadId(threadIdx);
        workTask.task.execute(*workTask.pipelineExecutionContext);
    }
    completionLatch.count_down();
}
}
