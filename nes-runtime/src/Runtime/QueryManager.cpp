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
#include <utility>
#include <variant>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime
{

static constexpr auto DEFAULT_QUEUE_INITIAL_CAPACITY = 64 * 1024;

QueryManager::QueryManager(
    std::shared_ptr<AbstractQueryStatusListener> queryStatusListener,
    std::vector<Memory::BufferManagerPtr> bufferManagers,
    WorkerId nodeEngineId,
    uint16_t numThreads,
    uint64_t numberOfBuffersPerEpoch,
    std::vector<uint64_t> workerToCoreMapping)
    : taskQueue(folly::MPMCQueue<Task>(DEFAULT_QUEUE_INITIAL_CAPACITY))
    , nodeEngineId(nodeEngineId)
    , bufferManagers(std::move(bufferManagers))
    , numThreads(numThreads)
    , workerToCoreMapping(std::move(workerToCoreMapping))
    , queryStatusListener(std::move(queryStatusListener))
    , numberOfBuffersPerEpoch(numberOfBuffersPerEpoch)
{
    tempCounterTasksCompleted.resize(numThreads);

    asyncTaskExecutor = std::make_shared<AsyncTaskExecutor>(1);
    NES_DEBUG("QueryManger: use dynamic mode with numThreads= {}", numThreads);
}

QueryManager::~QueryManager() NES_NOEXCEPT(false)
{
    destroy();
}

bool QueryManager::startThreadPool(uint64_t numberOfBuffersPerWorker)
{
    NES_DEBUG("startThreadPool: setup thread pool for nodeEngineId= {}  with numThreads= {}", nodeEngineId, numThreads);
    ///Note: the shared_from_this prevents from starting this in the ctor because it expects one shared ptr from this
    auto expected = QueryManagerStatus::Created;
    if (queryManagerStatus.compare_exchange_strong(expected, QueryManagerStatus::Running))
    {
#ifdef ENABLE_PAPI_PROFILER
        cpuProfilers.resize(numThreads);
#endif

        threadPool = std::make_shared<ThreadPool>(
            nodeEngineId, inherited0::shared_from_this(), numThreads, bufferManagers, numberOfBuffersPerWorker, workerToCoreMapping);
        return threadPool->start();
    }

    NES_ASSERT2_FMT(false, "Cannot start query manager workers");
    return false;
}

void QueryManager::destroy()
{
    /// 0. if already destroyed
    if (queryManagerStatus.load() == QueryManagerStatus::Destroyed)
    {
        return;
    }
    /// 1. attempt transition from Running -> Stopped
    auto expected = QueryManagerStatus::Running;

    bool successful = true;
    if (queryManagerStatus.compare_exchange_strong(expected, QueryManagerStatus::Stopped))
    {
        std::unique_lock lock(queryMutex);
        auto copyOfRunningQeps = runningQEPs;
        lock.unlock();
        for (auto& [_, qep] : copyOfRunningQeps)
        {
            successful &= stopQuery(qep, Runtime::QueryTerminationType::HardStop);
        }
    }
    NES_ASSERT2_FMT(successful, "Cannot stop running queryIdAndCatalogEntryMapping upon query manager destruction");
    /// 2. attempt transition from Stopped -> Destroyed
    expected = QueryManagerStatus::Stopped;
    if (queryManagerStatus.compare_exchange_strong(expected, QueryManagerStatus::Destroyed))
    {
        {
            std::scoped_lock const locks(queryMutex, statisticsMutex);

            queryToStatisticsMap.clear();
            runningQEPs.clear();
        }
        if (threadPool)
        {
            threadPool->stop();
            threadPool.reset();
        }
        NES_DEBUG("QueryManager::resetQueryManager finished");
    }

    if (queryManagerStatus.load() == QueryManagerStatus::Destroyed)
    {
        taskQueue = decltype(taskQueue)();
    }
}

Execution::QueryStatus QueryManager::getQueryStatus(QueryId queryId) const
{
    std::unique_lock const lock(queryMutex);
    auto it = runningQEPs.find(queryId);
    if (it != runningQEPs.end())
    {
        return it->second->getStatus();
    }
    return Execution::QueryStatus::Invalid;
}

template <class... Ts>
struct overloaded : Ts...
{
    using Ts::operator()...;
};
Sources::SourceReturnType::EmitFunction
QueryManager::createSourceEmitFunction(std::vector<Execution::SuccessorExecutablePipeline>&& executableSuccessorPipelines)
{
    /// Todo #241: Pass the new unique source identifier to the emit function for correct source 'callback-event' handling.
    return [this, successors = std::move(executableSuccessorPipelines)](
               const OriginId originId, Sources::SourceReturnType::SourceReturnType returntype)
    {
        std::visit(
            overloaded{
                [&](Sources::SourceReturnType::Data& data)
                {
                    for (const auto& successorPipeline : successors)
                    {
                        this->addWorkForNextPipeline(data.buffer, successorPipeline);
                    }
                },
                [&](const Sources::SourceReturnType::EoS&)
                {
                    NES_DEBUG("DataSource {} : End of stream.", originId);
                    this->addEndOfStream(originId, successors, Runtime::QueryTerminationType::Graceful);
                    this->notifySourceCompletion(originId, Runtime::QueryTerminationType::Graceful);
                },
                [&](const Sources::SourceReturnType::Stopped&)
                {
                    NES_DEBUG("DataSource {} : Stopping.", originId);
                    this->addEndOfStream(originId, successors, Runtime::QueryTerminationType::HardStop);
                    this->notifySourceCompletion(originId, Runtime::QueryTerminationType::HardStop);
                },
                [&](const Sources::SourceReturnType::Error& error)
                {
                    NES_DEBUG("DataSource {} : Failure.", originId);
                    this->notifySourceFailure(originId, error.ex);
                }},
            returntype);
    };
}

void QueryManager::reconfigure(ReconfigurationMessage& task, WorkerContext& context)
{
    Reconfigurable::reconfigure(task, context);
    switch (task.getType())
    {
        case ReconfigurationType::Destroy: {
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("QueryManager: task type not supported");
        }
    }
}

void QueryManager::postReconfigurationCallback(ReconfigurationMessage& task)
{
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType())
    {
        case ReconfigurationType::Destroy: {
            auto queryId = task.getQueryId();
            auto status = getQueryStatus(queryId);
            if (status == Execution::QueryStatus::Invalid)
            {
                NES_WARNING("Query {} was already removed or never deployed", queryId);
                return;
            }
            NES_ASSERT(
                status == Execution::QueryStatus::Stopped || status == Execution::QueryStatus::Finished
                    || status == Execution::QueryStatus::Failed,
                "query plan " << queryId << " is not in valid state " << int(status));
            std::unique_lock const lock(queryMutex);
            if (auto it = runningQEPs.find(queryId); it != runningQEPs.end())
            { /// note that this will release all shared pointers stored in a QEP object
                it->second->destroy();
                runningQEPs.erase(it);
            }
            /// we need to think if we want to remove this after a soft stop
            ///            queryToStatisticsMap.erase(qepId);
            NES_DEBUG("QueryManager: removed running QEP  {}", queryId);
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("QueryManager: task type not supported");
        }
    }
}

uint64_t QueryManager::getNextTaskId()
{
    return ++taskIdCounter;
}

uint64_t QueryManager::getNumberOfWorkerThreads() const
{
    return numThreads;
}

} /// namespace NES::Runtime
