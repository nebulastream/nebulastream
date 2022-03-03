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
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
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
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <memory>
#include <stack>
#include <utility>

namespace NES::Runtime {

namespace detail {

class ReconfigurationPipelineExecutionContext : public Execution::PipelineExecutionContext {
  public:
    explicit ReconfigurationPipelineExecutionContext(QuerySubPlanId queryExecutionPlanId, QueryManagerPtr queryManager)
        : Execution::PipelineExecutionContext(
            queryExecutionPlanId,
            std::move(queryManager),
            [](TupleBuffer&, NES::Runtime::WorkerContext&) {
            },
            [](TupleBuffer&) {
            },
            std::vector<Execution::OperatorHandlerPtr>()) {
        // nop
    }
};

class ReconfigurationEntryPointPipelineStage : public Execution::ExecutablePipelineStage {
    using base = Execution::ExecutablePipelineStage;

  public:
    explicit ReconfigurationEntryPointPipelineStage() : base(Unary) {
        // nop
    }

    ExecutionResult
    execute(TupleBuffer& buffer, Execution::PipelineExecutionContext& pipelineContext, WorkerContextRef workerContext) override {
        NES_TRACE("QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint begin on thread "
                  << workerContext.getId());
        auto queryManager = pipelineContext.getQueryManager();
        auto* task = buffer.getBuffer<ReconfigurationMessage>();
        NES_TRACE(
            "QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint going to wait on thread "
            << workerContext.getId());
        task->wait();
        NES_TRACE("QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint going to reconfigure "
                  "on thread "
                  << workerContext.getId());
        task->getInstance()->reconfigure(*task, workerContext);
        NES_TRACE(
            "QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint post callback on thread "
            << workerContext.getId());
        task->postReconfiguration();
        NES_TRACE("QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint completed on thread "
                  << workerContext.getId());
        task->postWait();
        return ExecutionResult::Ok;
    }
};

}// namespace detail

static constexpr auto DEFAULT_QUEUE_INITIAL_CAPACITY = 64 * 1024;

void QueryManager::initQueryManagerMode() {
    if (queryMangerMode == NumaAware) {
        NES_NOT_IMPLEMENTED();
        //        //the goal of the code below is to make sure that we have a even distribution of task among numa nodes
        //        // because we use this assumptions in the remainder for simplify the shutdown/reconfiguration
        //
        //        auto usedThreadsForMapping = 0;
        //
        //        //loop over the config provided by the user, e.g., 0,1,2 as cores to use
        //        for (auto& val : workerToCoreMapping) {
        //            //the user can specify more locations in the config than actually used for processing so we have to
        //            //check this end only check for the used thread count
        //            if (usedThreadsForMapping < numThreads) {
        //                auto tmpNumaNode = hardwareManager->getNumaNodeForCore(val);
        //                //count the number of threads per numa node
        //                hardwareManager[tmpNumaNode] += 1;
        //                usedThreadsForMapping++;
        //            } else {
        //                break;
        //            }
        //        }
        //
        //        std::stringstream ss;
        //        for (auto& val : numaRegionToThreadMap) {
        //            ss << "region=" << val.first << " threadCnt=" << val.second << std::endl;
        //        }
        //        ss << "numa placement=" << ss.str() << std::endl;
        //        NES_DEBUG("with numa placement =" << ss.str());
        //
        //        //make sure that all numa nodes have the same number of threads (for simplicity)
        //        bool firstValue = true;
        //        size_t prevValue = 0;
        //        for (auto region : numaRegionToThreadMap) {
        //            if (firstValue) {
        //                prevValue = region.second;//gather the first value as reference
        //            } else {
        //                //check each value if it is equal to the previous value
        //                NES_ASSERT(region.second == prevValue, "the worker map contains different number of threads per node");
        //            }
        //        }
        //
        //        //make sure that we only have consecutive numa regions in use
        //        firstValue = true;
        //        size_t prevKey = 0;
        //        for (auto region : numaRegionToThreadMap) {
        //            if (firstValue) {
        //                prevKey = region.first;//gather the first value as reference
        //            } else {
        //                //check each value if it is the next numa region e.g., 1,2,3, etc.
        //                NES_ASSERT(region.first == prevKey + 1, "the worker map contains non consecutive numa regions");
        //                prevKey = region.first;
        //            }
        //        }
        //
        //        if (numaRegionToThreadMap.size() != 0) {
        //            numberOfQueues = numaRegionToThreadMap.size();
        //        }
        //        NES_DEBUG("Number of queues used for running is =" << numberOfQueues);
        //
        //        //create the actual task queues
        //        for (uint64_t i = 0; i < numberOfQueues; i++) {
        //            taskQueues.push_back(folly::MPMCQueue<Task>(DEFAULT_QUEUE_INITIAL_CAPACITY));
        //        }
    } else if (queryMangerMode == Static) {
        NES_DEBUG("QueryManger: use static mode for numberOfQueues=" << numberOfQueues << " numThreads=" << numThreads
                                                                     << " numberOfThreadsPerQueue=" << numberOfThreadsPerQueue);
        if (numberOfQueues * numberOfThreadsPerQueue != numThreads) {
            NES_THROW_RUNTIME_ERROR("number of queues and threads have to match");
        }

        //create the actual task queues
        for (uint64_t i = 0; i < numberOfQueues; i++) {
            taskQueues.push_back(folly::MPMCQueue<Task>(DEFAULT_QUEUE_INITIAL_CAPACITY));
        }
    } else if (queryMangerMode == Dynamic) {
        NES_DEBUG("QueryManger: use dynmic mode for numberOfQueues=" << numberOfQueues << " numThreads=" << numThreads
                                                                     << " numberOfThreadsPerQueue=" << numberOfThreadsPerQueue);
        taskQueues.push_back(folly::MPMCQueue<Task>(DEFAULT_QUEUE_INITIAL_CAPACITY));
    }
}

QueryManager::QueryManager(std::vector<BufferManagerPtr> bufferManagers,
                           uint64_t nodeEngineId,
                           uint16_t numThreads,
                           HardwareManagerPtr hardwareManager,
                           std::vector<uint64_t> workerToCoreMapping,
                           uint64_t numberOfQueues,
                           uint64_t numberOfThreadsPerQueue,
                           QueryMangerMode queryManagerMode)
    : nodeEngineId(nodeEngineId), bufferManagers(std::move(bufferManagers)), numThreads(numThreads),
      queryMangerMode(queryManagerMode), numberOfQueues(numberOfQueues), numberOfThreadsPerQueue(numberOfThreadsPerQueue),
      hardwareManager(hardwareManager), workerToCoreMapping(workerToCoreMapping) {

    initQueryManagerMode();
    tempCounterTasksCompleted.resize(numThreads);
    reconfigurationExecutable = std::make_shared<detail::ReconfigurationEntryPointPipelineStage>();
}

uint64_t QueryManager::getCurrentTaskSum() {
    size_t sum = 0;
    for (auto& val : tempCounterTasksCompleted) {
        sum += val.counter.load(std::memory_order_relaxed);
    }
    return sum;
}

QueryManager::~QueryManager() NES_NOEXCEPT(false) { destroy(); }

bool QueryManager::startThreadPool(uint64_t numberOfBuffersPerWorker) {
    NES_DEBUG("startThreadPool: setup thread pool for nodeId=" << nodeEngineId << " with numThreads=" << numThreads);
    //Note: the shared_from_this prevents from starting this in the ctor because it expects one shared ptr from this
    auto expected = Created;
    if (queryManagerStatus.compare_exchange_strong(expected, Running)) {
#ifdef ENABLE_PAPI_PROFILER
        cpuProfilers.resize(numThreads);
#endif

        std::vector<uint64_t> threadToQueueMapping;

        if (queryMangerMode == NumaAware) {
            NES_NOT_IMPLEMENTED();
        } else if (queryMangerMode == Static) {
            for (uint64_t queueId = 0; queueId < taskQueues.size(); queueId++) {
                for (uint64_t threadId = 0; threadId < numberOfThreadsPerQueue; threadId++) {
                    threadToQueueMapping.push_back(queueId);
                }
            }
        } else if (queryMangerMode == Dynamic) {
            numberOfThreadsPerQueue = numThreads;
            for (uint32_t i = 0; i < numberOfThreadsPerQueue; i++) {
                //all threads map to the same queue
                threadToQueueMapping.push_back(0);
            }
        } else {
            NES_NOT_IMPLEMENTED();
        }

        threadPool = std::make_shared<ThreadPool>(nodeEngineId,
                                                  inherited0::shared_from_this(),
                                                  numThreads,
                                                  bufferManagers,
                                                  numberOfBuffersPerWorker,
                                                  hardwareManager,
                                                  workerToCoreMapping,
                                                  threadToQueueMapping);
        return threadPool->start();
    }

    NES_ASSERT2_FMT(false, "Cannot start query manager workers");
    return false;
}

void QueryManager::destroy() {
    // 0. if already destroyed
    if (queryManagerStatus.load() == Destroyed) {
        return;
    }
    // 1. attempt transition from Running -> Stopped
    auto expected = Running;

    bool successful = true;
    if (queryManagerStatus.compare_exchange_strong(expected, Stopped)) {
        std::unique_lock lock(queryMutex);
        auto copyOfRunningQeps = runningQEPs;
        lock.unlock();
        for (auto& [_, qep] : copyOfRunningQeps) {
            successful &= stopQuery(qep, false);
        }
    }
    NES_ASSERT2_FMT(successful, "Cannot stop running queries upon query manager destruction");
    // 2. attempt transition from Stopped -> Destroyed
    expected = Stopped;
    if (queryManagerStatus.compare_exchange_strong(expected, Destroyed)) {
        {
            std::scoped_lock locks(queryMutex, statisticsMutex);
            for (uint32_t i = 0; i < taskQueues.size(); i++) {
                NES_DEBUG("QueryManager: Destroy queryId_to_query_map " << sourceIdToExecutableQueryPlanMap.size() << " id=" << i
                                                                        << " task queue size=" << taskQueues[i].size());
            }

            queryToStatisticsMap.clear();
            runningQEPs.clear();
        }
        if (threadPool) {
            threadPool->stop();
            threadPool.reset();
            taskQueues.clear();
        }
        NES_DEBUG("QueryManager::resetQueryManager finished");
    } else {
        NES_ASSERT2_FMT(false, "invalid status of query manager, need to implement the transition");
    }
}

bool QueryManager::registerQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_DEBUG("QueryManager::registerQueryInNodeEngine: query" << qep->getQueryId() << " subquery=" << qep->getQuerySubPlanId());

    NES_ASSERT2_FMT(queryManagerStatus.load() == Running,
                    "QueryManager::registerQuery: cannot accept new query id " << qep->getQuerySubPlanId() << " "
                                                                               << qep->getQueryId());
    std::scoped_lock lock(queryMutex, statisticsMutex);
    // test if elements already exist
    NES_DEBUG("QueryManager: resolving sources for query " << qep);
    for (const auto& source : qep->getSources()) {
        // source already exists, add qep to source set if not there
        OperatorId sourceOperatorId = source->getOperatorId();
        sourceIdToSuccessorMap[sourceOperatorId] = source->getExecutableSuccessors();

        NES_DEBUG("QueryManager: Source " << sourceOperatorId << " not found. Creating new element with with qep " << qep);
        sourceIdToExecutableQueryPlanMap[sourceOperatorId] = qep;
        queryMapToOperatorId[qep->getQueryId()].push_back(sourceOperatorId);
    }

    NES_DEBUG("queryToStatisticsMap add for=" << qep->getQuerySubPlanId() << " pair queryId=" << qep->getQueryId()
                                              << " subplanId=" << qep->getQuerySubPlanId());

    //TODO: Tiis assumes 1) that there is only one pipeline per query and 2) that the subqueryplan id is unique => both can become a problem
    queryToStatisticsMap.insert(qep->getQuerySubPlanId(),
                                std::make_shared<QueryStatistics>(qep->getQueryId(), qep->getQuerySubPlanId()));

    if (queryMangerMode == Static) {
        NES_ASSERT2_FMT(queryToStatisticsMap.size() <= numberOfQueues,
                        "QueryManager::registerQuery: not enough queues are free for numberOfQueues="
                            << numberOfQueues << " query cnt=" << queryToStatisticsMap.size());
    }

    //currently we asume all queues have same number of threads so we can do this.
    queryToTaskQueueIdMap[qep->getQueryId()] = currentTaskQueueId++;
    NES_DEBUG("queryToTaskQueueIdMap add for=" << qep->getQueryId() << " queue=" << currentTaskQueueId - 1);
    return true;
}

bool QueryManager::startQuery(const Execution::ExecutableQueryPlanPtr& qep, StateManagerPtr stateManager) {
    NES_DEBUG("QueryManager::startQuery: query id " << qep->getQuerySubPlanId() << " " << qep->getQueryId());
    NES_ASSERT2_FMT(queryManagerStatus.load() == Running,
                    "QueryManager::startQuery: cannot accept new query id " << qep->getQuerySubPlanId() << " "
                                                                            << qep->getQueryId());
    NES_ASSERT(qep->getStatus() == Execution::ExecutableQueryPlanStatus::Created,
               "Invalid status for starting the QEP " << qep->getQuerySubPlanId());

    // TODO do not change the start sequence plz
    // 1. start the qep and handlers, if any
    if (!qep->setup() || !qep->start(std::move(stateManager))) {
        NES_FATAL_ERROR("QueryManager: query execution plan could not started");
        return false;
    }

    // 2. start net sinks
    for (const auto& sink : qep->getSinks()) {
        if (std::dynamic_pointer_cast<Network::NetworkSink>(sink)) {
            NES_DEBUG("QueryManager: start network sink " << sink);
            sink->setup();
        }
    }

    // 3. start net sources
    for (const auto& source : qep->getSources()) {
        if (std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
            NES_DEBUG("QueryManager: start network source " << source << " str=" << source->toString());
            if (!source->start()) {
                NES_WARNING("QueryManager: network source " << source << " could not started as it is already running");
            } else {
                NES_DEBUG("QueryManager: network source " << source << " started successfully");
            }
        }
    }

    // 4. start data sinks
    for (const auto& sink : qep->getSinks()) {
        if (std::dynamic_pointer_cast<Network::NetworkSink>(sink)) {
            continue;
        }
        NES_DEBUG("QueryManager: start sink " << sink);
        sink->setup();
    }

    // 5. start data sources
    for (const auto& source : qep->getSources()) {
        if (std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
            continue;
            ;
        }
        NES_DEBUG("QueryManager: start source " << source << " str=" << source->toString());
        if (!source->start()) {
            NES_WARNING("QueryManager: source " << source << " could not started as it is already running");
        } else {
            NES_DEBUG("QueryManager: source " << source << " started successfully");
        }
    }

    {
        std::unique_lock lock(queryMutex);
        runningQEPs.emplace(qep->getQuerySubPlanId(), qep);
    }
    return true;
}

bool QueryManager::deregisterQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_DEBUG("QueryManager::deregisterAndUndeployQuery: query" << qep->getQueryId());
    std::unique_lock lock(queryMutex);
    for (auto const& source : qep->getSources()) {
        sourceToQEPMapping.erase(source->getOperatorId());
    }

    return true;
}

namespace detail {
class PoisonPillEntryPointPipelineStage : public Execution::ExecutablePipelineStage {
    using base = Execution::ExecutablePipelineStage;

  public:
    explicit PoisonPillEntryPointPipelineStage() : base(Unary) {
        // nop
    }

    virtual ~PoisonPillEntryPointPipelineStage() = default;

    ExecutionResult execute(TupleBuffer&, Execution::PipelineExecutionContext&, WorkerContextRef) override {
        return ExecutionResult::AllFinished;
    }
};
}// namespace detail

void QueryManager::poisonWorkers() {
    auto optBuffer = bufferManagers[0]->getUnpooledBuffer(1);// there is always one buffer manager
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();

    auto pipelineContext = std::make_shared<detail::ReconfigurationPipelineExecutionContext>(-1, inherited0::shared_from_this());
    auto pipeline = Execution::ExecutablePipeline::create(-1,// any query plan
                                                          -1,// any sub query plan
                                                          -1,
                                                          pipelineContext,
                                                          std::make_shared<detail::PoisonPillEntryPointPipelineStage>(),
                                                          1,
                                                          std::vector<Execution::SuccessorExecutablePipeline>(),
                                                          true);

    if (queryMangerMode == Static) {
        for (auto u{0ul}; u < taskQueues.size(); ++u) {
            for (auto i{0ul}; i < numberOfThreadsPerQueue; ++i) {
                NES_DEBUG("Add poision for queue=" << u << " and thread=" << i);
                taskQueues[u].blockingWrite(Task(pipeline, buffer, getNextTaskId()));
            }
        }
    } else {
        for (auto u{0ul}; u < threadPool->getNumberOfThreads(); ++u) {
            NES_DEBUG("Add poision for queue=" << u);
            taskQueues[0].blockingWrite(Task(pipeline, buffer, getNextTaskId()));
        }
    }
}

bool QueryManager::failQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    bool ret = true;
    NES_DEBUG("QueryManager::failQuery: query" << qep->getQueryId());
    switch (qep->getStatus()) {
        case Execution::ExecutableQueryPlanStatus::ErrorState:
        case Execution::ExecutableQueryPlanStatus::Finished:
        case Execution::ExecutableQueryPlanStatus::Stopped: {
            return true;
        }
        case Execution::ExecutableQueryPlanStatus::Invalid: {
            NES_ASSERT2_FMT(false, "not supported yet " << qep->getQuerySubPlanId());
            break;
        }
        default: {
            break;
        }
    }
    for (const auto& source : qep->getSources()) {
        NES_ASSERT2_FMT(source->fail(),
                        "Cannot fail source " << source->getOperatorId() << " beloning to " << qep->getQuerySubPlanId());
    }

    auto terminationFuture = qep->getTerminationFuture();
    auto terminationStatus = terminationFuture.wait_for(std::chrono::minutes(10));
    switch (terminationStatus) {
        case std::future_status::ready: {
            if (terminationFuture.get() != Execution::ExecutableQueryPlanResult::Fail) {
                NES_FATAL_ERROR("QueryManager: QEP " << qep->getQuerySubPlanId() << " could not be failed");
                ret = false;
            }
            break;
        }
        case std::future_status::timeout:
        case std::future_status::deferred: {
            // TODO we need to fail the query now as it could not be stopped?
            NES_ASSERT2_FMT(false, "Cannot stop query within deadline " << qep->getQuerySubPlanId());
            break;
        }
    }
    if (ret) {
        addReconfigurationMessage(qep->getQuerySubPlanId(),
                                  ReconfigurationMessage(qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
                                  true);
    }
    NES_DEBUG("QueryManager::failQuery: query " << qep->getQuerySubPlanId() << " was "
                                                << (ret ? "successful" : " not successful"));
    return true;
}

bool QueryManager::stopQuery(const Execution::ExecutableQueryPlanPtr& qep, bool graceful) {
    NES_DEBUG("QueryManager::stopQuery: query sub-plan id " << qep->getQuerySubPlanId() << " graceful=" << graceful);
    bool ret = true;
    switch (qep->getStatus()) {
        case Execution::ExecutableQueryPlanStatus::ErrorState:
        case Execution::ExecutableQueryPlanStatus::Finished:
        case Execution::ExecutableQueryPlanStatus::Stopped: {
            NES_DEBUG("QueryManager::stopQuery: query sub-plan id " << qep->getQuerySubPlanId() << " is already stopped");
            return true;
        }
        case Execution::ExecutableQueryPlanStatus::Invalid: {
            NES_ASSERT2_FMT(false, "not supported yet " << qep->getQuerySubPlanId());
            break;
        }
        default: {
            break;
        }
    }

    for (const auto& source : qep->getSources()) {
        if (graceful) {
            // graceful shutdown :: only leaf sources
            if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
                NES_ASSERT2_FMT(source->stop(graceful), "Cannot terminate source " << source->getOperatorId());
            }
        } else {
            NES_ASSERT2_FMT(source->stop(graceful), "Cannot terminate source " << source->getOperatorId());
        }
    }
    for (uint32_t i = 0; i < taskQueues.size(); i++) {
        NES_DEBUG("QueryManager: stopQuery queue sizeses are "
                  << " id=" << i << " task queue size=" << taskQueues[i].size());
    }
    // TODO evaluate if we need to have this a wait instead of a get
    // TODO for instance we could wait N seconds and if the stopped is not succesful by then
    // TODO we need to trigger a hard local kill of a QEP
    auto terminationFuture = qep->getTerminationFuture();
    auto terminationStatus = terminationFuture.wait_for(std::chrono::minutes(10));
    switch (terminationStatus) {
        case std::future_status::ready: {
            if (terminationFuture.get() != Execution::ExecutableQueryPlanResult::Ok) {
                NES_FATAL_ERROR("QueryManager: QEP " << qep->getQuerySubPlanId() << " could not be stopped");
                ret = false;
            }
            break;
        }
        case std::future_status::timeout:
        case std::future_status::deferred: {
            // TODO we need to fail the query now as it could not be stopped?
            NES_ASSERT2_FMT(false, "Cannot stop query within deadline " << qep->getQuerySubPlanId());
            break;
        }
    }
    if (ret) {
        addReconfigurationMessage(
            qep->getQueryId(),
            qep->getQuerySubPlanId(),
            ReconfigurationMessage(qep->getQueryId(), qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
            true);
    }
    NES_DEBUG("QueryManager::stopQuery: query " << qep->getQuerySubPlanId() << " was "
                                                << (ret ? "successful" : " not successful"));
    return ret;
}

uint64_t QueryManager::getNumberOfTasksInWorkerQueue() const {
    auto sum = 0;
    for (uint32_t i = 0; i < taskQueues.size(); i++) {
        auto qSize = taskQueues[i].size();
        if (qSize > 0) {
            sum += qSize;
        }
    }
    return sum;
}

bool QueryManager::addReconfigurationMessage(QueryId queryId,
                                             QuerySubPlanId queryExecutionPlanId,
                                             const ReconfigurationMessage& message,
                                             bool blocking) {
    NES_DEBUG("QueryManager: QueryManager::addReconfigurationMessage begins on plan "
              << queryExecutionPlanId << " blocking=" << blocking << " type " << message.getType());
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
    auto optBuffer = bufferManagers[0]->getUnpooledBuffer(sizeof(ReconfigurationMessage));
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();
    if (queryMangerMode == Dynamic) {
        new (buffer.getBuffer())
            ReconfigurationMessage(message, threadPool->getNumberOfThreads(), blocking);// memcpy using copy ctor
    } else {
        new (buffer.getBuffer()) ReconfigurationMessage(message, numberOfThreadsPerQueue, blocking);// memcpy using copy ctor
    }
    return addReconfigurationMessage(queryId, queryExecutionPlanId, std::move(buffer), blocking);
}

bool QueryManager::addReconfigurationMessage(QueryId queryId,
                                             QuerySubPlanId queryExecutionPlanId,
                                             TupleBuffer&& buffer,
                                             bool blocking) {
    std::unique_lock reconfLock(addReconfigurationMessageMutex);
    auto* task = buffer.getBuffer<ReconfigurationMessage>();
    NES_DEBUG("QueryManager: QueryManager::addReconfigurationMessage begins on plan "
              << queryExecutionPlanId << " blocking=" << blocking << " type " << task->getType()
              << " to queue=" << queryToTaskQueueIdMap[queryId]);
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
    auto pipelineContext =
        std::make_shared<detail::ReconfigurationPipelineExecutionContext>(queryExecutionPlanId, inherited0::shared_from_this());
    auto pipeline = Execution::ExecutablePipeline::create(-1,// any query plan
                                                          queryId,
                                                          queryExecutionPlanId,
                                                          pipelineContext,
                                                          reconfigurationExecutable,
                                                          1,
                                                          std::vector<Execution::SuccessorExecutablePipeline>(),
                                                          true);

    if (queryMangerMode == Dynamic) {
        for (uint64_t threadId = 0; threadId < threadPool->getNumberOfThreads(); threadId++) {
            taskQueues[0].blockingWrite(Task(pipeline, buffer, getNextTaskId()));
        }
    } else if (queryMangerMode == Static) {
        for (uint64_t threadId = 0; threadId < numberOfThreadsPerQueue; threadId++) {
            taskQueues[queryToTaskQueueIdMap[queryId]].blockingWrite(Task(pipeline, buffer, getNextTaskId()));
        }
    }

    reconfLock.unlock();
    if (blocking) {
        task->postWait();
        task->postReconfiguration();
    }
    //    }
    return true;
}

bool QueryManager::addSoftEndOfStream(DataSourcePtr source) {
    auto sourceId = source->getOperatorId();
    auto executableQueryPlan = sourceToQEPMapping[sourceId];
    auto pipelineSuccessors = source->getExecutableSuccessors();
    // send EOS to NetworkSources
    for (auto& source : executableQueryPlan->getSources()) {// TODO change source vector to source map to avoid for loops..
        if (source->getOperatorId() == sourceId) {
            if (auto netSource = std::dynamic_pointer_cast<Network::NetworkSource>(source); netSource != nullptr) {
                auto reconfMessage = ReconfigurationMessage(executableQueryPlan->getQueryId(),
                                                            executableQueryPlan->getQuerySubPlanId(),
                                                            SoftEndOfStream,
                                                            netSource);
                addReconfigurationMessage(executableQueryPlan->getQueryId(),
                                          executableQueryPlan->getQuerySubPlanId(),
                                          reconfMessage,
                                          false);
            }
        }
    }
    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is a executable pipeline we send a reconfiguration message to the pipeline.
        // If successor is a data sink we send the reconfiguration message to the query plan.
        auto weakQep = std::make_any<std::weak_ptr<Execution::ExecutableQueryPlan>>(executableQueryPlan);
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executableQueryPlan->getQueryId(),
                                                        executableQueryPlan->getQuerySubPlanId(),
                                                        SoftEndOfStream,
                                                        (*executablePipeline),
                                                        std::move(weakQep));
            addReconfigurationMessage(executableQueryPlan->getQueryId(),
                                      executableQueryPlan->getQuerySubPlanId(),
                                      reconfMessage,
                                      false);
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            auto reconfMessageSink = ReconfigurationMessage(executableQueryPlan->getQueryId(),
                                                            executableQueryPlan->getQuerySubPlanId(),
                                                            SoftEndOfStream,
                                                            (*sink));
            addReconfigurationMessage(executableQueryPlan->getQueryId(),
                                      executableQueryPlan->getQuerySubPlanId(),
                                      reconfMessageSink,
                                      false);
            auto reconfMessageQEP = ReconfigurationMessage(executableQueryPlan->getQueryId(),
                                                           executableQueryPlan->getQuerySubPlanId(),
                                                           SoftEndOfStream,
                                                           (executableQueryPlan),
                                                           std::move(weakQep));
            addReconfigurationMessage(executableQueryPlan->getQueryId(),
                                      executableQueryPlan->getQuerySubPlanId(),
                                      reconfMessageQEP,
                                      false);
        }
        NES_DEBUG("soft end-of-stream opId=" << sourceId << " reconfType=" << int(HardEndOfStream)
                                             << " queryID=" << executableQueryPlan->getQueryId()
                                             << " subPlanId=" << executableQueryPlan->getQuerySubPlanId()
                                             << " queryExecutionPlanId=" << executableQueryPlan->getQuerySubPlanId()
                                             << " threadPool->getNumberOfThreads()=" << threadPool->getNumberOfThreads() << " qep"
                                             << executableQueryPlan->getQueryId());
    }
    return true;
}

bool QueryManager::addHardEndOfStream(DataSourcePtr source) {
    auto sourceId = source->getOperatorId();
    auto executableQueryPlan = sourceToQEPMapping[sourceId];
    auto pipelineSuccessors = source->getExecutableSuccessors();

    // send EOS to NetworkSources
    for (auto& source : executableQueryPlan->getSources()) {// TODO change source vector to source map to avoid for loops..
        if (source->getOperatorId() == sourceId) {
            if (auto netSource = std::dynamic_pointer_cast<Network::NetworkSource>(source); netSource != nullptr) {
                auto reconfMessage = ReconfigurationMessage(executableQueryPlan->getQueryId(),
                                                            executableQueryPlan->getQuerySubPlanId(),
                                                            HardEndOfStream,
                                                            netSource);
                addReconfigurationMessage(executableQueryPlan->getQueryId(),
                                          executableQueryPlan->getQuerySubPlanId(),
                                          reconfMessage,
                                          false);
            }
        }
    }
    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is a executable pipeline we send a reconfiguration message to the pipeline.
        // If successor is a data sink we send the reconfiguration message to the query plan.
        auto weakQep = std::make_any<std::weak_ptr<Execution::ExecutableQueryPlan>>(executableQueryPlan);
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executableQueryPlan->getQueryId(),
                                                        executableQueryPlan->getQuerySubPlanId(),
                                                        HardEndOfStream,
                                                        (*executablePipeline),
                                                        std::move(weakQep));
            addReconfigurationMessage(executableQueryPlan->getQueryId(),
                                      executableQueryPlan->getQuerySubPlanId(),
                                      reconfMessage,
                                      false);
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            auto reconfMessageSink = ReconfigurationMessage(executableQueryPlan->getQueryId(),
                                                            executableQueryPlan->getQuerySubPlanId(),
                                                            HardEndOfStream,
                                                            (*sink));
            addReconfigurationMessage(executableQueryPlan->getQueryId(),
                                      executableQueryPlan->getQuerySubPlanId(),
                                      reconfMessageSink,
                                      false);
            auto reconfMessageQEP = ReconfigurationMessage(executableQueryPlan->getQueryId(),
                                                           executableQueryPlan->getQuerySubPlanId(),
                                                           HardEndOfStream,
                                                           (executableQueryPlan),
                                                           std::move(weakQep));
            addReconfigurationMessage(executableQueryPlan->getQueryId(),
                                      executableQueryPlan->getQuerySubPlanId(),
                                      reconfMessageQEP,
                                      false);
        }
        NES_DEBUG("hard end-of-stream opId=" << sourceId << " reconfType=" << HardEndOfStream
                                             << " queryId=" << executableQueryPlan->getQueryId()
                                             << " querySubPlanId=" << executableQueryPlan->getQuerySubPlanId()
                                             << " queryExecutionPlanId=" << executableQueryPlan->getQuerySubPlanId()
                                             << " threadPool->getNumberOfThreads()=" << threadPool->getNumberOfThreads() << " qep"
                                             << executableQueryPlan->getQueryId());
    }
    executableQueryPlan->stop();
    return true;
}

bool QueryManager::addEndOfStream(DataSourcePtr source, bool graceful) {
    std::unique_lock queryLock(queryMutex);
    bool isSourcePipeline = sourceIdToSuccessorMap.find(sourceId) != sourceIdToSuccessorMap.end();
    NES_DEBUG("QueryManager: QueryManager::addEndOfStream for source operator " << sourceId << " graceful=" << graceful << " end "
                                                                                << isSourcePipeline);
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool no longer running");
    NES_ASSERT2_FMT(sourceToQEPMapping.find(source->getOperatorId()) != sourceToQEPMapping.end(),
                    "invalid source " << source->getOperatorId());

    bool success = false;
    if (graceful) {
        success = addSoftEndOfStream(source);
    } else {
        success = addHardEndOfStream(source);
    }
    return true;
}

ExecutionResult QueryManager::processNextTask(bool running, WorkerContext& workerContext) {
    NES_TRACE("QueryManager: QueryManager::getWork wait get lock");
    Task task;
    if (running) {
        taskQueues[workerContext.getQueueId()].blockingRead(task);

#ifdef ENABLE_PAPI_PROFILER
        auto profiler = cpuProfilers[NesThread::getId() % cpuProfilers.size()];
        auto numOfInputTuples = task.getNumberOfInputTuples();
        profiler->startSampling();
#endif

        NES_TRACE("QueryManager: provide task" << task.toString() << " to thread (getWork())");
        auto result = task(workerContext);
#ifdef ENABLE_PAPI_PROFILER
        profiler->stopSampling(numOfInputTuples);
#endif

        switch (result) {
            case ExecutionResult::Ok: {
                completedWork(task, workerContext);
                return ExecutionResult::Ok;
            }
            default: {
                return result;
            }
        }
    } else {
        return terminateLoop(workerContext);
    }
}// namespace NES::Runtime

ExecutionResult QueryManager::terminateLoop(WorkerContext& workerContext) {
    bool hitReconfiguration = false;
    Task task;
    while (taskQueues[workerContext.getQueueId()].read(task)) {
        if (!hitReconfiguration) {// execute all pending tasks until first reconfiguration
            task(workerContext);
            if (task.isReconfiguration()) {
                hitReconfiguration = true;
            }
        } else {
            if (task.isReconfiguration()) {// execute only pending reconfigurations
                task(workerContext);
            }
        }
    }
    return ExecutionResult::Finished;
}

void QueryManager::addWorkForNextPipeline(TupleBuffer& buffer,
                                          Execution::SuccessorExecutablePipeline executable,
                                          uint32_t queueId) {
    NES_DEBUG("Add Work for executable for queue=" << queueId);
    if (auto nextPipeline = std::get_if<Execution::ExecutablePipelinePtr>(&executable)) {
        if (!(*nextPipeline)->isRunning()) {
            // we ignore task if the pipeline is not running anymore.
            NES_WARNING("Pushed task for non running executable pipeline id=" << (*nextPipeline)->getPipelineId());
            return;
        }
        //NES_ASSERT2_FMT((*nextPipeline)->isRunning(),
        //                "Pushed task for non running pipeline id=" << (*nextPipeline)->getPipelineId());
        NES_DEBUG("QueryManager: added Task this pipelineID="
                  << (*nextPipeline)->getPipelineId() << "  for Number of next pipelines "
                  << (*nextPipeline)->getSuccessors().size() << " inputBuffer " << buffer << " queueId=" << queueId);

        taskQueues[queueId].write(Task(executable, buffer, getNextTaskId()));
    } else if (auto sink = std::get_if<DataSinkPtr>(&executable)) {
        NES_DEBUG("QueryManager: added Task for Sink " << sink->get()->toString() << " inputBuffer " << buffer
                                                       << " queueId=" << queueId);

        taskQueues[queueId].write(Task(executable, buffer, getNextTaskId()));
    } else {
        NES_THROW_RUNTIME_ERROR("This should not happen");
    }
}
//#define LIGHT_WEIGHT_STATISTICS
void QueryManager::completedWork(Task& task, WorkerContext& wtx) {
    NES_DEBUG("QueryManager::completedWork: Work for task=" << task.toString() << "worker ctx id=" << wtx.getId());
    if (task.isReconfiguration()) {
        return;
    }
    tempCounterTasksCompleted[wtx.getId() % tempCounterTasksCompleted.size()].fetch_add(1);

#ifndef LIGHT_WEIGHT_STATISTICS
    // todo also support data sinks
    uint64_t querySubPlanId = -1;
    uint64_t queryId = -1;
    auto executable = task.getExecutable();
    if (auto* sink = std::get_if<DataSinkPtr>(&executable)) {
        querySubPlanId = (*sink)->getParentPlanId();
        NES_DEBUG("QueryManager::completedWork: task for sink querySubPlanId=" << querySubPlanId);
        //        return;
    } else if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&executable)) {
        if ((*executablePipeline)->isReconfiguration()) {
            NES_DEBUG("QueryManager::completedWork: task reconfig task");
            return;
        }
        querySubPlanId = (*executablePipeline)->getQuerySubPlanId();
        queryId = (*executablePipeline)->getQueryId();
        NES_DEBUG(
            "QueryManager::completedWork: task for exec pipeline isreconfig=" << (*executablePipeline)->isReconfiguration());
    }

    if (queryToStatisticsMap.contains(querySubPlanId)) {
        auto statistics = queryToStatisticsMap.find(querySubPlanId);

        statistics->incProcessedTasks();
        statistics->incProcessedBuffers();
        auto creation = task.getBufferRef().getCreationTimestamp();
        auto now =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        auto diff = now - creation;
        //        std::cout << "now in queryMan=" << now << " creation=" << creation << std::endl;
        NES_ASSERT(creation <= (unsigned long) now, "timestamp is in the past");
        statistics->incLatencySum(diff);

        auto qSize = taskQueues[wtx.getQueueId()].size();
        statistics->incQueueSizeSum(qSize > 0 ? qSize : 0);

        for (auto& bufferManager : bufferManagers) {
            statistics->incAvailableGlobalBufferSum(bufferManager->getAvailableBuffers());
            statistics->incAvailableFixedBufferSum(bufferManager->getAvailableBuffersInFixedSizePools());
        }

#ifdef NES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT
        statistics->addTimestampToLatencyValue(now, diff);
#endif
        statistics->incProcessedTuple(task.getNumberOfInputTuples());
    } else {
        //        for (auto& elem : queryToStatisticsMap) {
        //            NES_DEBUG("first elem=" << elem.first << " queyId=" << elem.second->getQueryId()
        //                                    << " subquery=" << elem.second->getSubQueryId());
        //        }
        NES_FATAL_ERROR("queryToStatisticsMap not set, this should only happen for testing queryId=" << queryId << " subPlanId="
                                                                                                     << querySubPlanId);
        std::cout << "queryToStatisticsMap not set, this should only happen for testing queryId=" << queryId
                  << " subPlanId=" << querySubPlanId << std::endl;
        NES_THROW_RUNTIME_ERROR("got buffer for not registered qep");
    }
#endif
}

uint64_t QueryManager::getQueryId(uint64_t querySubPlanId) const {
    std::unique_lock lock(statisticsMutex);
    auto iterator = runningQEPs.find(querySubPlanId);
    if (iterator != runningQEPs.end()) {
        return iterator->second->getQueryId();
    }
    return -1;
}

Execution::ExecutableQueryPlanStatus QueryManager::getQepStatus(QuerySubPlanId id) {
    std::unique_lock lock(queryMutex);
    auto it = runningQEPs.find(id);
    if (it != runningQEPs.end()) {
        return it->second->getStatus();
    }
    return Execution::ExecutableQueryPlanStatus::Invalid;
}

Execution::ExecutableQueryPlanPtr QueryManager::getQueryExecutionPlan(QuerySubPlanId id) const {
    std::unique_lock lock(queryMutex);
    auto it = runningQEPs.find(id);
    if (it != runningQEPs.end()) {
        return it->second;
    }
    return nullptr;
}

QueryStatisticsPtr QueryManager::getQueryStatistics(QuerySubPlanId qepId) {
    std::unique_lock lock(statisticsMutex);
    NES_DEBUG("QueryManager::getQueryStatistics: for qep=" << qepId);
    return queryToStatisticsMap.find(qepId);
}

void QueryManager::reconfigure(ReconfigurationMessage& task, WorkerContext& context) {
    Reconfigurable::reconfigure(task, context);
    switch (task.getType()) {
        case Destroy: {
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("QueryManager: task type not supported");
        }
    }
}

void QueryManager::postReconfigurationCallback(ReconfigurationMessage& task) {
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case Destroy: {
            auto qepId = task.getParentPlanId();
            auto status = getQepStatus(qepId);
            if (status == Execution::ExecutableQueryPlanStatus::Invalid) {
                NES_WARNING("Query" << qepId << "was already removed or never deployed");
                return;
            }
            NES_ASSERT(status == Execution::ExecutableQueryPlanStatus::Stopped
                           || status == Execution::ExecutableQueryPlanStatus::ErrorState,
                       "query plan " << qepId << " is not in valid state " << int(status));
            std::unique_lock lock(queryMutex);
            if (auto it = runningQEPs.find(qepId); it != runningQEPs.end()) { // note that this will release all shared pointers stored in a QEP object
                it->second->destroy();
                runningQEPs.erase(it);
            }

            NES_DEBUG("QueryManager: removed running QEP " << qepId);
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("QueryManager: task type not supported");
        }
    }
}

uint64_t QueryManager::getNodeId() const { return nodeEngineId; }

bool QueryManager::isThreadPoolRunning() const { return threadPool != nullptr; }

uint64_t QueryManager::getNextTaskId() { return ++taskIdCounter; }

uint64_t QueryManager::getNumberOfWorkerThreads() { return numThreads; }

void QueryManager::notifyQueryStatusChange(const Execution::ExecutableQueryPlanPtr& qep,
                                           Execution::ExecutableQueryPlanStatus status) {
    NES_ASSERT(qep, "Invalid query plan object");
    if (status == Execution::ExecutableQueryPlanStatus::Stopped) {
        // if we got here it means the query gracefully stopped
        // we need to go to each source and terminate their threads
        // alternatively, we need to detach source threads
        for (const auto& source : qep->getSources()) {
            NES_ASSERT(source->stop(true), "Cannot cleanup source"); // just a clean-up op
        }
        addReconfigurationMessage(qep->getQuerySubPlanId(),
                                  ReconfigurationMessage(qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
                                  false);
    }
}
void QueryManager::notifyOperatorFailure(DataSourcePtr, const std::string errorMessage) {
    NES_ASSERT(false, errorMessage);
}
void QueryManager::notifyTaskFailure(Execution::SuccessorExecutablePipeline, const std::string& errorMessage) {
    NES_ASSERT(false, errorMessage);
}

}// namespace NES::Runtime