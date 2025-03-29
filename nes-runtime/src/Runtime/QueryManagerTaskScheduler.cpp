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
#include <iostream>
#include <memory>
#include <stack>
#include <utility>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES
{

namespace detail
{

class ReconfigurationPipelineExecutionContext : public PipelineExecutionContext
{
public:
    explicit ReconfigurationPipelineExecutionContext(QueryId queryId, QueryManagerPtr queryManager)
        : PipelineExecutionContext(
              INVALID_PIPELINE_ID, /// this is a dummy pipelineID
              queryId,
              queryManager->getBufferManager(),
              queryManager->getNumberOfWorkerThreads(),
              [](Memory::TupleBuffer&, WorkerContext&) {},
              [](Memory::TupleBuffer&) {},
              std::vector<std::shared_ptr<OperatorHandler>>())
    {
        /// nop
    }
};

class ReconfigurationEntryPointPipelineStage : public ExecutablePipelineStage
{
    using base = ExecutablePipelineStage;

public:
    explicit ReconfigurationEntryPointPipelineStage() : base(PipelineStageArity::Unary)
    {
        /// nop
    }

    ExecutionResult execute(Memory::TupleBuffer& buffer, PipelineExecutionContext&, WorkerContextRef workerContext)
    {
        NES_TRACE(
            "QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint begin on thread {}",
            workerContext.getId());
        auto* task = buffer.getBuffer<ReconfigurationMessage>();
        NES_TRACE(
            "QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint going to wait "
            "on thread {}",
            workerContext.getId());
        task->wait();
        NES_TRACE(
            "QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint going to "
            "reconfigure on thread {}",
            workerContext.getId());
        task->getInstance()->reconfigure(*task, workerContext);
        NES_TRACE(
            "QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint post callback "
            "on thread {}",
            workerContext.getId());
        task->postReconfiguration();
        NES_TRACE(
            "QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint completed on "
            "thread {}",
            workerContext.getId());
        task->postWait();
        return ExecutionResult::Ok;
    }
};
}

ExecutionResult QueryManager::processNextTask(bool running, WorkerContext& workerContext)
{
    NES_TRACE("QueryManager: QueryManager::getWork wait get lock");
    Task task;
    if (running)
    {
        taskQueue.blockingRead(task);

#ifdef ENABLE_PAPI_PROFILER
        auto profiler = cpuProfilers[NesThread::getId() % cpuProfilers.size()];
        auto numOfInputTuples = task.getNumberOfInputTuples();
        profiler->startSampling();
#endif

        NES_TRACE("QueryManager: provide task {} to thread (getWork())", task.toString());
        ExecutionResult result = task(workerContext);
#ifdef ENABLE_PAPI_PROFILER
        profiler->stopSampling(numOfInputTuples);
#endif

        switch (result)
        {
            ///OK comes from sinks and intermediate operators
            case ExecutionResult::Ok: {
                completedWork(task, workerContext);
                return ExecutionResult::Ok;
            }
            ///Finished indicate that the processing is done
            case ExecutionResult::Finished: {
                completedWork(task, workerContext);
                return ExecutionResult::Finished;
            }
            case ExecutionResult::Error: {
                NES_ERROR("Task execution failed");
                notifyTaskFailure(task.getExecutable(), "Task execution failed");
                return ExecutionResult::Error;
            }
            default: {
                return result;
            }
        }
    }
    else
    {
        return terminateLoop(workerContext);
    }
}

ExecutionResult QueryManager::terminateLoop(WorkerContext& workerContext)
{
    bool hitReconfiguration = false;
    Task task;
    while (taskQueue.read(task))
    {
        if (!hitReconfiguration)
        { /// execute all pending tasks until first reconfiguration
            task(workerContext);
            if (task.isReconfiguration())
            {
                hitReconfiguration = true;
            }
        }
        else
        {
            if (task.isReconfiguration())
            { /// execute only pending reconfigurations
                task(workerContext);
            }
        }
    }
    return ExecutionResult::Finished;
}

void QueryManager::addWorkForNextPipeline(Memory::TupleBuffer& buffer, SuccessorExecutablePipeline executable)
{
    if (const auto nextPipeline = std::get_if<std::shared_ptr<ExecutablePipeline>>(&executable))
    {
        if (!(*nextPipeline)->isRunning())
        {
            /// we ignore task if the pipeline is not running anymore.
            NES_WARNING("Pushed task for non running executable pipeline id={}", (*nextPipeline)->getPipelineId());
            return;
        }

        taskQueue.blockingWrite(Task(executable, buffer, getNextTaskId()));
    }
    else if (const auto sink = std::get_if<std::shared_ptr<Sinks::Sink>>(&executable))
    {
        NES_DEBUG("QueryManager: added Task for Sink {} inputBuffer {}", **sink, fmt::streamed(buffer));
        taskQueue.blockingWrite(Task(executable, buffer, getNextTaskId()));
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("This should not happen");
    }
}

void QueryManager::updateStatistics(const Task& task, QueryId queryId, PipelineId pipelineId, WorkerContext& workerContext)
{
    tempCounterTasksCompleted[workerContext.getId() % tempCounterTasksCompleted.size()].fetch_add(1);
#ifndef LIGHT_WEIGHT_STATISTICS
    if (queryToStatisticsMap.contains(queryId))
    {
        auto statistics = queryToStatisticsMap.find(queryId);
        uint64_t now
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();

        statistics->setTimestampFirstProcessedTask(now, true);
        statistics->setTimestampLastProcessedTask(now);
        statistics->incProcessedTasks();
        statistics->incProcessedBuffers();
        const auto creation = task.getBufferRef().getCreationTimestampInMS().getRawValue();
        if (now < creation)
        {
            NES_WARNING(
                "Creation timestamp is in the future; skipping statistics update: creation timestamp = {}, buffer origin "
                "ID = {}, buffer chunk number = {}, now = {}",
                creation,
                task.getBufferRef().getOriginId(),
                task.getBufferRef().getChunkNumber(),
                static_cast<unsigned long>(now));
        }
        else
        {
            statistics->incLatencySum(now - creation);
        }

        for (auto& bufferManager : bufferManagers)
        {
            statistics->incAvailableGlobalBufferSum(bufferManager->getAvailableBuffers());
            statistics->incAvailableFixedBufferSum(bufferManager->getAvailableBuffersInFixedSizePools());
        }

        statistics->incTasksPerPipelineId(pipelineId, workerContext.getId());

#    ifdef NES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT
        statistics->addTimestampToLatencyValue(now, diff);
#    endif
        statistics->incProcessedTuple(task.getNumberOfInputTuples());

        /// with multiple queryIdAndCatalogEntryMapping this won't be correct
        auto qSize = taskQueue.size();
        statistics->incQueueSizeSum(qSize > 0 ? qSize : 0);
    }
    else
    {
        using namespace std::string_literals;

        NES_ERROR("queryToStatisticsMap not set for {} this should only happen for testing", queryId);
    }
#endif
}

void QueryManager::completedWork(Task& task, WorkerContext& wtx)
{
    NES_TRACE("QueryManager::completedWork: Work for task={} worker ctx id={}", task.toString(), wtx.getId());
    if (task.isReconfiguration())
    {
        return;
    }

    QueryId queryId = INVALID_QUERY_ID;
    PipelineId pipelineId = INVALID_PIPELINE_ID;
    auto executable = task.getExecutable();
    if (const auto sink = std::get_if<std::shared_ptr<NES::Sinks::Sink>>(&executable))
    {
        queryId = (*sink)->queryId;
        NES_TRACE("QueryManager::completedWork: task for sink querySubPlanId={}", queryId);
    }
    else if (auto* executablePipeline = std::get_if<std::shared_ptr<ExecutablePipeline>>(&executable))
    {
        queryId = (*executablePipeline)->getQueryId();
        pipelineId = (*executablePipeline)->getPipelineId();
        NES_TRACE("QueryManager::completedWork: task for exec pipeline isreconfig={}", (*executablePipeline)->isReconfiguration());
    }
    updateStatistics(task, queryId, pipelineId, wtx);
}

bool QueryManager::addReconfigurationMessage(QueryId queryId, const ReconfigurationMessage& message, bool blocking)
{
    NES_DEBUG(
        "QueryManager: QueryManager::addReconfigurationMessage begins on plan {} blocking={} type {}",
        queryId,
        blocking,
        magic_enum::enum_name(message.getType()));
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
    auto optBuffer = bufferManagers[0]->getUnpooledBuffer(sizeof(ReconfigurationMessage));
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();
    new (buffer.getBuffer()) ReconfigurationMessage(message, threadPool->getNumberOfThreads(), blocking); /// memcpy using copy ctor
    return addReconfigurationMessage(queryId, std::move(buffer), blocking);
}

bool QueryManager::addReconfigurationMessage(QueryId queryId, Memory::TupleBuffer&& buffer, bool blocking)
{
    auto* task = buffer.getBuffer<ReconfigurationMessage>();
    {
        std::unique_lock reconfLock(reconfigurationMutex);
        NES_DEBUG(
            "QueryManager: QueryManager::addReconfigurationMessage begins on plan {} blocking={} type {}",
            queryId,
            blocking,
            magic_enum::enum_name(task->getType()));
        NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
        auto pipelineContext = std::make_shared<detail::ReconfigurationPipelineExecutionContext>(queryId, inherited0::shared_from_this());
        auto reconfigurationExecutable = std::make_shared<detail::ReconfigurationEntryPointPipelineStage>();
        auto pipeline = ExecutablePipeline::create(
            INVALID_PIPELINE_ID,
            queryId,
            inherited0::shared_from_this(),
            pipelineContext,
            reconfigurationExecutable,
            1,
            std::vector<SuccessorExecutablePipeline>(),
            true);

        for (uint64_t threadId = 0; threadId < threadPool->getNumberOfThreads(); threadId++)
        {
            taskQueue.blockingWrite(Task(pipeline, buffer, getNextTaskId()));
        }
    }
    if (blocking)
    {
        task->postWait();
        task->postReconfiguration();
    }
    return true;
}

namespace detail
{
class PoisonPillEntryPointPipelineStage : public ExecutablePipelineStage
{
    using base = ExecutablePipelineStage;

public:
    explicit PoisonPillEntryPointPipelineStage() : base(PipelineStageArity::Unary)
    {
        /// nop
    }

    virtual ~PoisonPillEntryPointPipelineStage() = default;

    ExecutionResult execute(Memory::TupleBuffer&, PipelineExecutionContext&, WorkerContextRef) { return ExecutionResult::AllFinished; }
};
}

void QueryManager::poisonWorkers()
{
    auto optBuffer = bufferManagers[0]->getUnpooledBuffer(1); /// there is always one buffer manager
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();

    auto pipelineContext
        = std::make_shared<detail::ReconfigurationPipelineExecutionContext>(INVALID_QUERY_ID, inherited0::shared_from_this());
    auto pipeline = ExecutablePipeline::create(
        INVALID_PIPELINE_ID,
        INVALID_QUERY_ID, /// any query plan
        inherited0::shared_from_this(),
        pipelineContext,
        std::make_shared<detail::PoisonPillEntryPointPipelineStage>(),
        1,
        std::vector<SuccessorExecutablePipeline>(),
        true);
    for (auto u{0ul}; u < threadPool->getNumberOfThreads(); ++u)
    {
        NES_DEBUG("Add poison for queue= {}", u);
        taskQueue.blockingWrite(Task(pipeline, buffer, getNextTaskId()));
    }
}

}
