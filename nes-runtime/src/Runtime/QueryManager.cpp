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

/* TODO
 * add to Reconfig... constructors:
 -1, // any query ID
 */

#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <memory>
#include <stack>
#include <utility>
namespace NES::Runtime {

static constexpr auto DEFAULT_QUEUE_INITIAL_CAPACITY = 64 * 1024;

AbstractQueryManager::AbstractQueryManager(std::shared_ptr<AbstractQueryStatusListener> queryStatusListener,
                                           std::vector<BufferManagerPtr> bufferManagers,
                                           WorkerId nodeEngineId,
                                           uint16_t numThreads,
                                           HardwareManagerPtr hardwareManager,
                                           uint64_t numberOfBuffersPerEpoch,
                                           std::vector<uint64_t> workerToCoreMapping)
    : nodeEngineId(nodeEngineId), bufferManagers(std::move(bufferManagers)), numThreads(numThreads),
      hardwareManager(std::move(hardwareManager)), workerToCoreMapping(std::move(workerToCoreMapping)),
      queryStatusListener(std::move(queryStatusListener)), numberOfBuffersPerEpoch(numberOfBuffersPerEpoch) {

    tempCounterTasksCompleted.resize(numThreads);

    asyncTaskExecutor = std::make_shared<AsyncTaskExecutor>(this->hardwareManager, 1);
}

DynamicQueryManager::DynamicQueryManager(std::shared_ptr<AbstractQueryStatusListener> queryStatusListener,
                                         std::vector<BufferManagerPtr> bufferManagers,
                                         WorkerId nodeEngineId,
                                         uint16_t numThreads,
                                         HardwareManagerPtr hardwareManager,
                                         uint64_t numberOfBuffersPerEpoch,
                                         std::vector<uint64_t> workerToCoreMapping)
    : AbstractQueryManager(std::move(queryStatusListener),
                           std::move(bufferManagers),
                           nodeEngineId,
                           numThreads,
                           std::move(hardwareManager),
                           numberOfBuffersPerEpoch,
                           std::move(workerToCoreMapping)),
      taskQueue(folly::MPMCQueue<Task>(DEFAULT_QUEUE_INITIAL_CAPACITY)) {
    NES_DEBUG("QueryManger: use dynamic mode with numThreads= {}", numThreads);
}

MultiQueueQueryManager::MultiQueueQueryManager(std::shared_ptr<AbstractQueryStatusListener> queryStatusListener,
                                               std::vector<BufferManagerPtr> bufferManagers,
                                               WorkerId nodeEngineId,
                                               uint16_t numThreads,
                                               HardwareManagerPtr hardwareManager,
                                               uint64_t numberOfBuffersPerEpoch,
                                               std::vector<uint64_t> workerToCoreMapping,
                                               uint64_t numberOfQueues,
                                               uint64_t numberOfThreadsPerQueue)
    : AbstractQueryManager(std::move(queryStatusListener),
                           std::move(bufferManagers),
                           nodeEngineId,
                           numThreads,
                           std::move(hardwareManager),
                           numberOfBuffersPerEpoch,
                           std::move(workerToCoreMapping)),
      numberOfQueues(numberOfQueues), numberOfThreadsPerQueue(numberOfThreadsPerQueue) {

    NES_DEBUG("QueryManger: use static mode for numberOfQueues={} numThreads={} numberOfThreadsPerQueue=",
              numberOfQueues,
              numThreads,
              numberOfThreadsPerQueue);
    if (numberOfQueues * numberOfThreadsPerQueue != numThreads) {
        NES_THROW_RUNTIME_ERROR("number of queues and threads have to match");
    }

    //create the actual task queues
    for (uint64_t i = 0; i < numberOfQueues; i++) {
        taskQueues.emplace_back(DEFAULT_QUEUE_INITIAL_CAPACITY);
    }
}

uint64_t DynamicQueryManager::getNumberOfBuffersPerEpoch() const { return numberOfBuffersPerEpoch; }

uint64_t DynamicQueryManager::getNumberOfTasksInWorkerQueues() const { return taskQueue.size(); }

uint64_t MultiQueueQueryManager::getNumberOfTasksInWorkerQueues() const {
    uint64_t sum = 0;
    for (uint64_t i = 0; i < numberOfQueues; i++) {
        sum += taskQueues[i].size();
    }
    return sum;
}

uint64_t AbstractQueryManager::getCurrentTaskSum() {
    size_t sum = 0;
    for (auto& val : tempCounterTasksCompleted) {
        sum += val.counter.load(std::memory_order_relaxed);
    }
    return sum;
}

uint64_t AbstractQueryManager::getNumberOfBuffersPerEpoch() const { return numberOfBuffersPerEpoch; }

AbstractQueryManager::~AbstractQueryManager() NES_NOEXCEPT(false) { destroy(); }

bool DynamicQueryManager::startThreadPool(uint64_t numberOfBuffersPerWorker) {
    NES_DEBUG("startThreadPool: setup thread pool for nodeEngineId= {}  with numThreads= {}", nodeEngineId, numThreads);
    //Note: the shared_from_this prevents from starting this in the ctor because it expects one shared ptr from this
    auto expected = QueryManagerStatus::Created;
    if (queryManagerStatus.compare_exchange_strong(expected, QueryManagerStatus::Running)) {
#ifdef ENABLE_PAPI_PROFILER
        cpuProfilers.resize(numThreads);
#endif

        threadPool = std::make_shared<ThreadPool>(nodeEngineId,
                                                  inherited0::shared_from_this(),
                                                  numThreads,
                                                  bufferManagers,
                                                  numberOfBuffersPerWorker,
                                                  hardwareManager,
                                                  workerToCoreMapping);
        return threadPool->start();
    }

    NES_ASSERT2_FMT(false, "Cannot start query manager workers");
    return false;
}

uint64_t MultiQueueQueryManager::getNumberOfBuffersPerEpoch() const { return numberOfBuffersPerEpoch; }

bool MultiQueueQueryManager::startThreadPool(uint64_t numberOfBuffersPerWorker) {
    NES_DEBUG("startThreadPool: setup thread pool for nodeId= {}  with numThreads= {}", nodeEngineId, numThreads);
    //Note: the shared_from_this prevents from starting this in the ctor because it expects one shared ptr from this
    auto expected = QueryManagerStatus::Created;
    if (queryManagerStatus.compare_exchange_strong(expected, QueryManagerStatus::Running)) {
#ifdef ENABLE_PAPI_PROFILER
        cpuProfilers.resize(numThreads);
#endif

        std::vector<uint64_t> threadToQueueMapping;

        for (uint64_t queueId = 0; queueId < taskQueues.size(); queueId++) {
            for (uint64_t threadId = 0; threadId < numberOfThreadsPerQueue; threadId++) {
                threadToQueueMapping.push_back(queueId);
            }
        }

        threadPool = std::make_shared<ThreadPool>(nodeEngineId,
                                                  inherited0::shared_from_this(),
                                                  numThreads,
                                                  bufferManagers,
                                                  numberOfBuffersPerWorker,
                                                  hardwareManager,
                                                  workerToCoreMapping);
        return threadPool->start(threadToQueueMapping);
    }

    NES_ASSERT2_FMT(false, "Cannot start query manager workers");
    return false;
}

void DynamicQueryManager::destroy() {
    AbstractQueryManager::destroy();
    if (queryManagerStatus.load() == QueryManagerStatus::Destroyed) {
        taskQueue = decltype(taskQueue)();
    }
}

void MultiQueueQueryManager::destroy() {
    AbstractQueryManager::destroy();
    if (queryManagerStatus.load() == QueryManagerStatus::Destroyed) {
        taskQueues.clear();
    }
}

void AbstractQueryManager::destroy() {
    // 0. if already destroyed
    if (queryManagerStatus.load() == QueryManagerStatus::Destroyed) {
        return;
    }
    // 1. attempt transition from Running -> Stopped
    auto expected = QueryManagerStatus::Running;

    bool successful = true;
    if (queryManagerStatus.compare_exchange_strong(expected, QueryManagerStatus::Stopped)) {
        std::unique_lock lock(queryMutex);
        auto copyOfRunningQeps = runningQEPs;
        lock.unlock();
        for (auto& [_, qep] : copyOfRunningQeps) {
            successful &= stopExecutableQueryPlan(qep, Runtime::QueryTerminationType::HardStop);
        }
    }
    NES_ASSERT2_FMT(successful, "Cannot stop running queryIdAndCatalogEntryMapping upon query manager destruction");
    // 2. attempt transition from Stopped -> Destroyed
    expected = QueryManagerStatus::Stopped;
    if (queryManagerStatus.compare_exchange_strong(expected, QueryManagerStatus::Destroyed)) {
        {
            std::scoped_lock locks(queryMutex, statisticsMutex);

            queryToStatisticsMap.clear();
            runningQEPs.clear();
        }
        if (threadPool) {
            threadPool->stop();
            threadPool.reset();
        }
        NES_DEBUG("AbstractQueryManager::resetQueryManager finished");
    }
}

SharedQueryId AbstractQueryManager::getSharedQueryId(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion) const {
    std::unique_lock lock(statisticsMutex);
    auto iterator = runningQEPs.find(decomposedQueryIdWithVersion);
    if (iterator != runningQEPs.end()) {
        return iterator->second->getSharedQueryId();
    }
    return INVALID_SHARED_QUERY_ID;
}

Execution::ExecutableQueryPlanStatus
AbstractQueryManager::getQepStatus(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion) {
    std::unique_lock lock(queryMutex);
    auto it = runningQEPs.find(decomposedQueryIdWithVersion);
    if (it != runningQEPs.end()) {
        return it->second->getStatus();
    }
    return Execution::ExecutableQueryPlanStatus::Invalid;
}

Execution::ExecutableQueryPlanPtr
AbstractQueryManager::getQueryExecutionPlan(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion) const {
    std::unique_lock lock(queryMutex);
    auto it = runningQEPs.find(decomposedQueryIdWithVersion);
    if (it != runningQEPs.end()) {
        return it->second;
    }
    return nullptr;
}

QueryStatisticsPtr AbstractQueryManager::getQueryStatistics(DecomposedQueryId decomposedQueryId) {
    QueryStatisticsPtr statistic = nullptr;
    queryToStatisticsMap.find(decomposedQueryId, statistic);
    return statistic;//either value or null
}

void AbstractQueryManager::resetQueryStatistics(NES::DecomposedQueryId decomposedQueryId) {
    QueryStatisticsPtr statistic = nullptr;
    queryToStatisticsMap.find(decomposedQueryId, statistic);
    if (statistic) {
        statistic->clear();
    }
}

void AbstractQueryManager::reconfigure(ReconfigurationMessage& task, WorkerContext& context) {
    Reconfigurable::reconfigure(task, context);
    switch (task.getType()) {
        case ReconfigurationType::Destroy: {
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("AbstractQueryManager: task type not supported");
        }
    }
}

void AbstractQueryManager::postReconfigurationCallback(ReconfigurationMessage& task) {
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case ReconfigurationType::Destroy: {
            auto decomposedQueryIdWithVersion = DecomposedQueryIdWithVersion(task.getParentPlanId(), task.getParentPlanVersion());
            auto status = getQepStatus(decomposedQueryIdWithVersion);
            if (status == Execution::ExecutableQueryPlanStatus::Invalid) {
                NES_WARNING("Query {}.{} was already removed or never deployed",
                            decomposedQueryIdWithVersion.id,
                            decomposedQueryIdWithVersion.version);
                return;
            }
            NES_ASSERT(status == Execution::ExecutableQueryPlanStatus::Stopped
                           || status == Execution::ExecutableQueryPlanStatus::Finished
                           || status == Execution::ExecutableQueryPlanStatus::ErrorState,
                       "query plan " << decomposedQueryIdWithVersion.id << "." << decomposedQueryIdWithVersion.version
                                     << " is not in valid state " << int(status));
            std::unique_lock lock(queryMutex);
            if (auto it = runningQEPs.find(decomposedQueryIdWithVersion); it != runningQEPs.end()) {
                it->second->destroy();
                runningQEPs.erase(it);
            }
            // we need to think if we want to remove this after a soft stop
            //            queryToStatisticsMap.erase(decomposedQueryIdWithVersion);
            NES_DEBUG("AbstractQueryManager: removed running QEP  {}.{}",
                      decomposedQueryIdWithVersion.id,
                      decomposedQueryIdWithVersion.version);
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("AbstractQueryManager: task type not supported");
        }
    }
}

WorkerId AbstractQueryManager::getNodeId() const { return nodeEngineId; }

bool AbstractQueryManager::isThreadPoolRunning() const { return threadPool != nullptr; }

uint64_t AbstractQueryManager::getNextTaskId() { return ++taskIdCounter; }

uint64_t AbstractQueryManager::getNumberOfWorkerThreads() { return numThreads; }

std::unordered_set<DecomposedQueryIdWithVersion> AbstractQueryManager::getExecutablePlanIdsForSource(DataSourcePtr source) const {
    std::unique_lock lock(queryMutex);
    auto executablePlans = sourceToQEPMapping.at(source->getOperatorId());
    std::unordered_set<DecomposedQueryIdWithVersion> ids;
    for (auto& plan : executablePlans) {
        ids.insert(DecomposedQueryIdWithVersion(plan->getDecomposedQueryId(), plan->getDecomposedQueryVersion()));
    }
    return ids;
}

bool AbstractQueryManager::injectEpochBarrier(uint64_t epochBarrier, OperatorId sourceOperatorId) {
    std::unique_lock lock(queryMutex);
    auto qeps = sourceToQEPMapping.find(sourceOperatorId);
    if (qeps != sourceToQEPMapping.end()) {
        //post reconfiguration message to the executable query plan with an epoch barrier to trim buffer storages
        for (const auto& qep : qeps->second) {
            for (auto sink : qep->getSinks()) {
                if (sink->getSinkMediumType() == SinkMediumTypes::NETWORK_SINK) {
                    NES_DEBUG("AbstractQueryManager::injectEpochBarrier queryId={} punctuation={} ", epochBarrier);
                    auto newReconf = ReconfigurationMessage(qep->getSharedQueryId(),
                                                            qep->getDecomposedQueryId(),
                                                            Runtime::ReconfigurationType::PropagateEpoch,
                                                            sink,
                                                            std::make_any<uint64_t>(epochBarrier));
                    addReconfigurationMessage(qep->getSharedQueryId(), qep->getDecomposedQueryId(), newReconf);
                }
            }
        }
        return true;
    }
    NES_THROW_RUNTIME_ERROR("AbstractQueryManager: no source was found");
    return false;
}

}// namespace NES::Runtime
