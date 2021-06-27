/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
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
            LocalBufferPoolPtr(),
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

#ifdef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
static constexpr auto DEFAULT_QUEUE_INITIAL_CAPACITY = 16 * 1024;
#endif

QueryManager::QueryManager(BufferManagerPtr bufferManager, uint64_t nodeEngineId, uint16_t numThreads)
    : nodeEngineId(nodeEngineId), bufferManager(std::move(bufferManager)), numThreads(numThreads)
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE

#else
      ,
      taskQueue(
          DEFAULT_QUEUE_INITIAL_CAPACITY)// TODO consider if we could use something num of buffers in buffer manager but maybe it could be too much
#endif
{
    NES_DEBUG("Init QueryManager::QueryManager");
    reconfigurationExecutable = std::make_shared<detail::ReconfigurationEntryPointPipelineStage>();
}

QueryManager::~QueryManager() noexcept(false) {
    NES_DEBUG("~QueryManager()");
    destroy();
}

bool QueryManager::startThreadPool() {
    NES_DEBUG("startThreadPool: setup thread pool for nodeId=" << nodeEngineId << " with numThreads=" << numThreads);
    //Note: the shared_from_this prevents from starting this in the ctor because it expects one shared ptr from this
    auto expected = Created;
    if (queryManagerStatus.compare_exchange_strong(expected, Running)) {
        threadPool = std::make_shared<ThreadPool>(nodeEngineId, inherited0::shared_from_this(), numThreads);
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

    if (queryManagerStatus.compare_exchange_strong(expected, Stopped)) {
        std::unique_lock lock(queryMutex);
        auto copyOfRunningQeps = runningQEPs;
        lock.unlock();
        for (auto& [_, qep] : copyOfRunningQeps) {
            stopQuery(qep, false);
        }
    }

    // 2. attempt transition from Stopped -> Destroyed
    expected = Stopped;
    if (queryManagerStatus.compare_exchange_strong(expected, Destroyed)) {
        {
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
            std::scoped_lock locks(queryMutex, workMutex, statisticsMutex);
#else
            std::scoped_lock locks(queryMutex, statisticsMutex);
#endif
            NES_DEBUG("QueryManager: Destroy queryId_to_query_map " << sourceIdToExecutableQueryPlanMap.size()
                                                                    << " task queue size=" << taskQueue.size());

            sourceIdToExecutableQueryPlanMap.clear();
            queryToStatisticsMap.clear();
            runningQEPs.clear();
        }
        if (threadPool) {
            threadPool->stop();
            threadPool.reset();
        }
        NES_DEBUG("QueryManager::resetQueryManager finished");
    } else {
        NES_ASSERT2_FMT(false, "invalid status of query manager, need to implement the transition");
    }
}

bool QueryManager::registerQuery(const Execution::ExecutableQueryPlanPtr& qep, const std::vector<DataSourcePtr>& sources) {
    NES_DEBUG("QueryManager::registerQuery: query" << qep->getQueryId() << " subquery=" << qep->getQuerySubPlanId());
    NES_ASSERT2_FMT(queryManagerStatus.load() == Running,
                    "QueryManager::registerQuery: cannot accept new query id " << qep->getQuerySubPlanId() << " "
                                                                               << qep->getQueryId());
    std::scoped_lock lock(queryMutex, statisticsMutex);
    // test if elements already exist
    NES_DEBUG("QueryManager: resolving sources for query " << qep->getQuerySubPlanId());
    for (const auto& source : sources) {
        OperatorId sourceOperatorId = source->getOperatorId();
        sourceIdToSuccessorMap[sourceOperatorId] = source->getExecutableSuccessors();
        sourceIdToExecutableQueryPlanMap[sourceOperatorId] = qep;
        queryToStatisticsMap.insert(qep->getQuerySubPlanId(),
                                    std::make_shared<QueryStatistics>(qep->getQueryId(), qep->getQuerySubPlanId()));
    }

#if EXTENDEDDEBUGGING//the mapping is a common sources of errors so please leave it in
    NES_DEBUG("operatorIdToPipelineStage mapping:");
    for (auto& a : operatorIdToPipelineStage) {
        NES_DEBUG("first=" << a.first << " second=" << a.second);
    }

    NES_DEBUG("operatorIdToQueryMap mapping:");
    for (auto& a : operatorIdToQueryMap) {
        NES_DEBUG("first=" << a.first << " second=" << a.second.size());
    }

    NES_DEBUG("queryMapToOperatorId mapping:");
    for (auto& a : queryMapToOperatorId) {
        NES_DEBUG("first=" << a.first << " second=" << a.second.size());
    }
#endif
    return true;
}

bool QueryManager::startQuery(const Execution::ExecutableQueryPlanPtr& qep, StateManagerPtr stateManager) {
    return startQueryForSources(qep, stateManager, qep->getSources());
}

bool QueryManager::startQueryForSources(Execution::ExecutableQueryPlanPtr& qep,
                                        StateManagerPtr stateManager,
                                        const std::vector<DataSourcePtr>& sources) {
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
    for (const auto& source : sources) {
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
    for (const auto& source : sources) {
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

bool QueryManager::isSourceAssociatedWithQep(OperatorId sourceOperatorId, QuerySubPlanId querySubPlanId) {
    if (sourceIdToExecutableQueryPlanMap.find(sourceOperatorId) == sourceIdToExecutableQueryPlanMap.end()) {
        return false;
    }
    return sourceIdToExecutableQueryPlanMap[sourceOperatorId]->getQuerySubPlanId() == querySubPlanId;
}

bool QueryManager::allSourcesMappedToQep(Execution::ExecutableQueryPlanPtr qep) {
    for (auto source : qep->getSources()) {
        OperatorId sourceOperatorId = source->getOperatorId();
        if (sourceIdToExecutableQueryPlanMap.find(sourceOperatorId) == sourceIdToExecutableQueryPlanMap.end()) {
            return false;
        }
        if (sourceIdToExecutableQueryPlanMap[sourceOperatorId]->getQuerySubPlanId() != qep->getQuerySubPlanId()) {
            return false;
        }
    }
    return true;
}

bool QueryManager::deregisterQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_DEBUG("QueryManager::deregisterAndUndeployQuery: query" << qep);

    std::unique_lock lock(queryMutex);
    bool succeed = true;
    auto sources = qep->getSources();

    for (const auto& source : sources) {
        NES_DEBUG("QueryManager: stop source " << source->toString());
        // remove source from map
        sourceIdToExecutableQueryPlanMap.erase(source->getOperatorId());
        /**
        if (sourceIdToExecutableQueryPlanMap.find(source->getOperatorId()) != sourceIdToExecutableQueryPlanMap.end()) {
            // source exists, remove qep from source if there
            if (sourceIdToExecutableQueryPlanMap[source->getOperatorId()].find(qep)
                != sourceIdToExecutableQueryPlanMap[source->getOperatorId()].end()) {
                // qep found, remove it
                NES_DEBUG("QueryManager: Removing QEP " << qep << " from source" << source->getOperatorId());
                if (sourceIdToExecutableQueryPlanMap[source->getOperatorId()].erase(qep) == 0) {
                    NES_FATAL_ERROR("QueryManager: Removing QEP " << qep << " for source " << source->getOperatorId()
                                                                  << " failed!");
                    succeed = false;
                }

                // if source has no qeps remove the source from map
                if (sourceIdToExecutableQueryPlanMap[source->getOperatorId()].empty()) {
                    if (sourceIdToExecutableQueryPlanMap.erase(source->getOperatorId()) == 0) {
                        NES_FATAL_ERROR("QueryManager: Removing source " << source->getOperatorId() << " failed!");
                        succeed = false;
                    }
                }
            }
        }
         */
    }
    return succeed;
}

bool QueryManager::failQuery(const Execution::ExecutableQueryPlanPtr&) {
    NES_NOT_IMPLEMENTED();
#if 0
    NES_DEBUG("QueryManager::failQuery: query" << qep);
    bool ret = true;
    {
        std::unique_lock lock(queryMutex);

        auto sources = qep->getSources();
        for (const auto& source : sources) {
            NES_DEBUG("QueryManager: stop source " << source->toString());
            // TODO what if two qeps use the same source

            if (operatorIdToQueryMap[source->getOperatorId()].size() != 1) {
                NES_WARNING("QueryManager: could not stop source " << source->toString() << " because other qeps are using it n="
                                                                   << operatorIdToQueryMap[source->getOperatorId()].size());
            } else {
                NES_DEBUG("QueryManager: failQuery source " << source->toString() << " because only " << qep << " is using it");
                bool success = source->stop();
                if (!success) {
                    NES_ERROR("QueryManager: failQuery: could not stop source " << source->toString());
                    ret = false;
                } else {
                    NES_DEBUG("QueryManager: failQuery: source " << source->toString() << " successfully stopped");
                }
            }
        }
        NES_DEBUG("QueryManager::stopQuery: query finished " << qep);
    }
    if (!qep->fail()) {
        NES_FATAL_ERROR("QueryManager: QEP could not be failed");
        ret = false;
    }

    auto sinks = qep->getSinks();
    for (const auto& sink : sinks) {
        NES_DEBUG("QueryManager: stop sink " << sink->toString());
        // TODO: do we also have to prevent to shutdown sink that is still used by another qep
        sink->shutdown();
    }
    if (ret) {
        addReconfigurationMessage(qep->getQuerySubPlanId(), ReconfigurationMessage(qep->getQuerySubPlanId(), Destroy, this), true);
    }
    return ret;
#endif
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
    auto optBuffer = bufferManager->getUnpooledBuffer(1);
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();

    auto pipelineContext = std::make_shared<detail::ReconfigurationPipelineExecutionContext>(-1, inherited0::shared_from_this());
    auto pipeline = Execution::ExecutablePipeline::create(-1,// any query plan
                                                          -1,// any sub query plan
                                                          pipelineContext,
                                                          std::make_shared<detail::PoisonPillEntryPointPipelineStage>(),
                                                          1,
                                                          std::vector<Execution::SuccessorExecutablePipeline>(),
                                                          true);
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
    std::unique_lock lock(workMutex);
    for (auto i{0ul}; i < threadPool->getNumberOfThreads(); ++i) {
        taskQueue.emplace_back(pipeline, buffer);
    }
    NES_WARNING("QueryManager: Poisoning Task Queue " << taskQueue.size());
    cv.notify_all();
#else
    for (auto i{0ul}; i < threadPool->getNumberOfThreads(); ++i) {
        taskQueue.write(Task(pipeline, buffer));
    }
#endif
}

bool QueryManager::stopQuery(const Execution::ExecutableQueryPlanPtr& qep, bool graceful) {
    NES_DEBUG("QueryManager::stopQuery: query sub-plan id " << qep->getQuerySubPlanId() << " graceful=" << graceful);
    bool ret = true;
    //    std::unique_lock lock(queryMutex);
    // here im using COW to avoid keeping the lock for long
    // however, this is not a long-term fix
    // because it wont lead to correct behaviour
    // under heavy query deployment ops
    auto sources = qep->getSources();
    auto copiedSources = std::vector(sources.begin(), sources.end());
    //    lock.unlock();

    switch (qep->getStatus()) {
        case Execution::Finished:
        case Execution::Stopped: {
            return true;
        }
        case Execution::ErrorState:
        case Execution::Invalid: {
            NES_ASSERT2_FMT(false, "not supported yet " << qep->getQuerySubPlanId());
            break;
        }
        default: {
            break;
        }
    }

    for (const auto& source : copiedSources) {
        if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
            source->stop(graceful);
        }
    }
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
    {
        std::unique_lock taskLock(workMutex);
        NES_WARNING("Number of tasks in queue when stopped=" << taskQueue.size());
    }
#endif
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
            NES_ASSERT2_FMT(false, "Cannot stop query within deadline " << qep->getQuerySubPlanId());
            break;
        }
    }
    if (ret) {
        addReconfigurationMessage(qep->getQuerySubPlanId(),
                                  ReconfigurationMessage(qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
                                  true);
    }
    NES_DEBUG("QueryManager::stopQuery: query " << qep->getQuerySubPlanId() << " was "
                                                << (ret ? "successful" : " not successful"));
    return ret;
}

uint64_t QueryManager::getNumberOfTasksInWorkerQueue() const {
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
    std::unique_lock workLock(workMutex);
    return taskQueue.size();
#else
    auto qSize = taskQueue.size();
    return qSize > 0 ? qSize : 0;
#endif
}

void QueryManager::addWork(const OperatorId sourceId, TupleBuffer& buf) {
    std::shared_lock queryLock(queryMutex);// we need this lock because operatorIdToQueryMap can be concurrently modified

    // check if source is registered
    if (sourceIdToSuccessorMap.find(sourceId) != sourceIdToSuccessorMap.end()) {
        auto executablePipelines = sourceIdToSuccessorMap[sourceId];
        // iterate over all executable pipelines
        for (const auto& executablePipeline : executablePipelines) {
            NES_DEBUG("Add Work for executable from network source " << sourceId << " origin id: " << buf.getOriginId());
            // create task
            auto task = Task(executablePipeline, buf);
            // dispatch task to task queue.
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
            //TODO: this is a problem now as it can become the bottleneck
            std::unique_lock workQueueLock(workMutex);
            taskQueue.emplace_back(task);
            cv.notify_all();
#else
            taskQueue.write(std::move(task));
#endif
        }
    } else {
        NES_ERROR("Source with id: " << sourceId << " is not registered.");
    }
}

bool QueryManager::addReconfigurationMessage(QuerySubPlanId queryExecutionPlanId,
                                             const ReconfigurationMessage& message,
                                             bool blocking) {
    NES_DEBUG("QueryManager: QueryManager::addReconfigurationMessage begins on plan "
              << queryExecutionPlanId << " blocking=" << blocking << " type " << message.getType());
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
    auto optBuffer = bufferManager->getUnpooledBuffer(sizeof(ReconfigurationMessage));
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();
    auto* task = new (buffer.getBuffer())
        ReconfigurationMessage(message, threadPool->getNumberOfThreads(), blocking);// memcpy using copy ctor
    auto pipelineContext =
        std::make_shared<detail::ReconfigurationPipelineExecutionContext>(queryExecutionPlanId, inherited0::shared_from_this());
    auto pipeline = Execution::ExecutablePipeline::create(-1,// any query plan
                                                          queryExecutionPlanId,
                                                          pipelineContext,
                                                          reconfigurationExecutable,
                                                          1,
                                                          std::vector<Execution::SuccessorExecutablePipeline>(),
                                                          true);
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
    {
        std::unique_lock lock(workMutex);
        for (auto i{0ull}; i < threadPool->getNumberOfThreads(); ++i) {
            taskQueue.emplace_back(pipeline, buffer);
        }
        cv.notify_all();
    }
#else
    if (threadPool->getNumberOfThreads() > 1) {
        std::vector<Task> batch;
        for (size_t i = 0; i < threadPool->getNumberOfThreads(); ++i) {
            //            batch.emplace_back(pipeline, buffer);
            taskQueue.write(Task(pipeline, buffer));
        }
    } else {
        taskQueue.write(Task(pipeline, buffer));
    }
#endif
    if (blocking) {
        task->postWait();
        task->postReconfiguration();
    }
    return true;
}

void QueryManager::propagateViaSuccessorPipelines(
    const ReconfigurationType type,
    const std::function<std::any(Execution::ExecutableQueryPlanPtr)>& userdataSupplier,
    const Execution::ExecutableQueryPlanPtr executableQueryPlan,
    std::vector<Execution::SuccessorExecutablePipeline>& pipelineSuccessors) {
    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is a executable pipeline we send a reconfiguration message to the pipeline.
        // If successor is a data sink we send the reconfiguration message to the query plan.
        auto userData = userdataSupplier(executableQueryPlan);
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            addReconfigurationMessage(executableQueryPlan->getQuerySubPlanId(),
                                      ReconfigurationMessage(executableQueryPlan->getQuerySubPlanId(),
                                                             type,
                                                             (*executablePipeline),
                                                             std::move(userData)));
        } else {
            addReconfigurationMessage(executableQueryPlan->getQuerySubPlanId(),
                                      ReconfigurationMessage(executableQueryPlan->getQuerySubPlanId(),
                                                             type,
                                                             (executableQueryPlan),
                                                             std::move(userData)));
        }
    }
}

bool QueryManager::addHardEndOfStream(OperatorId sourceId) {
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
    std::unique_lock lock(workMutex);
#endif
    auto executableQueryPlan = sourceIdToExecutableQueryPlanMap[sourceId];

#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
    auto optBuffer = bufferManager->getUnpooledBuffer(sizeof(ReconfigurationMessage));
    NES_ASSERT(!!optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();

    //use in-place construction to create the reconfig task within a buffer
    new (buffer.getBuffer()) ReconfigurationMessage(executableQueryPlan->getQuerySubPlanId(),
                                                    HardEndOfStream,
                                                    threadPool->getNumberOfThreads(),
                                                    executableQueryPlan);
    NES_DEBUG("hard end-of-stream opId=" << sourceId << " reconfType=" << HardEndOfStream
                                         << " queryExecutionPlanId=" << executableQueryPlan->getQuerySubPlanId()
                                         << " threadPool->getNumberOfThreads()=" << threadPool->getNumberOfThreads() << " qep"
                                         << executableQueryPlan->getQueryId() << " tasks in queue=" << taskQueue.size());

    auto pipelineContext =
        std::make_shared<detail::ReconfigurationPipelineExecutionContext>(executableQueryPlan->getQuerySubPlanId(),
                                                                          inherited0::shared_from_this());
    auto pipeline = Execution::ExecutablePipeline::create(-1,// any query plan
                                                          executableQueryPlan->getQuerySubPlanId(),
                                                          pipelineContext,
                                                          reconfigurationExecutable,
                                                          1,
                                                          std::vector<Execution::SuccessorExecutablePipeline>(),
                                                          true);
    std::stack<Task> temp;
    while (!taskQueue.empty()) {
        Task task = taskQueue.front();
        auto executable = task.getExecutable();
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&executable)) {
            if ((*executablePipeline)->isReconfiguration()) {
                temp.push(task);
                taskQueue.pop_front();
            } else {
                break;// reached a data task
            }
        } else {
            break;// reached a data task
        }
    }
    for (auto i{0ul}; i < threadPool->getNumberOfThreads(); ++i) {
        taskQueue.emplace_front(pipeline, buffer);
    }
    while (!temp.empty()) {
        taskQueue.emplace_front(temp.top());
        temp.pop();
    }
#else
    auto reconfigurationToken =
        ReconfigurationMessage(executableQueryPlan->getQuerySubPlanId(), HardEndOfStream, 1, executableQueryPlan);
    reconfigurationToken.wait();
    executableQueryPlan->postReconfigurationCallback(reconfigurationToken);
#endif
    return true;
}

bool QueryManager::addEndOfStream(OperatorId sourceId, bool graceful) {
    std::shared_lock queryLock(queryMutex);
    bool isSourcePipeline = sourceIdToSuccessorMap.find(sourceId) != sourceIdToSuccessorMap.end();
    NES_DEBUG("QueryManager: QueryManager::addEndOfStream for source operator " << sourceId << " graceful=" << graceful << " end "
                                                                                << isSourcePipeline);
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool no longer running");
    NES_ASSERT2_FMT(isSourcePipeline, "invalid source");
    NES_ASSERT2_FMT(sourceIdToExecutableQueryPlanMap.find(sourceId) != sourceIdToExecutableQueryPlanMap.end(),
                    "Operator id to query map for operator is empty");
    if (graceful) {
        auto qep = sourceIdToExecutableQueryPlanMap[sourceId];
        // todo adopt this code for multiple source pipelines
        auto pipelineSuccessors = sourceIdToSuccessorMap[sourceId];
        propagateViaSuccessorPipelines(
            SoftEndOfStream,
            [](Execution::ExecutableQueryPlanPtr executableQueryPlan) {
                return std::make_any<std::weak_ptr<Execution::ExecutableQueryPlan>>(executableQueryPlan);
            },
            qep,
            pipelineSuccessors);
    } else {
        addHardEndOfStream(sourceId);
    }
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
    cv.notify_all();
#endif
    return true;
}

#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
ExecutionResult QueryManager::processNextTask(std::atomic<bool>& running, WorkerContext& workerContext) {
#else
ExecutionResult QueryManager::processNextTask(bool running, WorkerContext& workerContext) {
#endif
    NES_TRACE("QueryManager: QueryManager::getWork wait get lock");
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
    std::unique_lock lock(workMutex);
    // wait while queue is empty but thread pool is running
    while (taskQueue.empty() && running) {
        cv.wait(lock);
        if (!running) {
            // return empty task if thread pool was shut down
            NES_DEBUG("QueryManager: Thread pool was shut down while waiting");
            lock.unlock();
            return terminateLoop(workerContext);
        }
    }
    NES_TRACE("QueryManager::getWork queue is not empty");
    // there is a potential task in the queue and the thread pool is running
    if (running && !taskQueue.empty()) {
        auto task = taskQueue.front();
        NES_DEBUG("QueryManager: provide task" << task.toString() << " to thread (getWork())");
        taskQueue.pop_front();
        lock.unlock();
        auto ret = task(workerContext);
        if (ret == ExecutionResult::Ok || ret == ExecutionResult::Finished) {
            completedWork(task, workerContext);
            return ret;
        }
        if (ret == ExecutionResult::AllFinished) {
            return ret;
        }
        return ExecutionResult::Error;
    }
    NES_DEBUG("QueryManager: Thread pool was shut down but has still tasks");
    lock.unlock();
    return terminateLoop(workerContext);

#else
    Task task;
    if (running) {
        taskQueue.blockingRead(task);
        NES_TRACE("QueryManager: provide task" << task.toString() << " to thread (getWork())");
        auto result = task(workerContext);
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
#endif
}

#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
ExecutionResult QueryManager::terminateLoop(WorkerContext& workerContext) {
    //    this->threadBarrier->wait();
    // must run this to execute all pending reconfiguration task (Destroy)
    //    bool hitReconfiguration = false;
    std::unique_lock lock(workMutex);
    while (!taskQueue.empty()) {
        NES_INFO("size before dequeue=" << taskQueue.size());

        auto task = taskQueue.front();
        taskQueue.pop_front();
        lock.unlock();
        auto executable = task.getExecutable();
#if 1
        // execute only reconfiguration tasks
        if (auto* taskExecutable = std::get_if<Execution::ExecutablePipelinePtr>(&(executable))) {
            if ((*taskExecutable)->isReconfiguration()) {
                task(workerContext);
            }
        }
        lock.lock();// relocking to access empty()
#else
        if (!hitReconfiguration) {
            // execute all pending tasks until first reconfiguration
            task(workerContext);
            if (auto taskExecutable = std::get_if<Execution::NewExecutablePipelinePtr>(&(executable))) {
                if ((*taskExecutable)->isReconfiguration()) {
                    hitReconfiguration = true;
                }
            }
            lock.lock();
            continue;
        } else {
            if (auto taskExecutable = std::get_if<Execution::NewExecutablePipelinePtr>(&(executable))) {
                if ((*taskExecutable)->isReconfiguration()) {
                    task(workerContext);
                }
            }
            lock.lock();
        }
#endif
    }
    lock.unlock();
    return ExecutionResult::Finished;
}
#else
ExecutionResult QueryManager::terminateLoop(WorkerContext& workerContext) {
    bool hitReconfiguration = false;
    Task task;
    while (taskQueue.read(task)) {
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
#endif

void QueryManager::addWorkForNextPipeline(TupleBuffer& buffer, Execution::SuccessorExecutablePipeline executable) {
    //std::unique_lock lock(queryMutex);// we need this mutex because runningQEPs can be concurrently modified

    NES_DEBUG("Add Work for executable");
#ifndef NES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE
    std::unique_lock lock2(workMutex);
    // dispatch buffer as task
    if (auto* nextPipeline = std::get_if<Execution::ExecutablePipelinePtr>(&executable)) {
        if ((*nextPipeline)->isRunning()) {
            NES_DEBUG("QueryManager: added Task for next pipeline " << (*nextPipeline)->getPipelineId() << " inputBuffer "
                                                                    << buffer.getOriginId()
                                                                    << " sequence:" << buffer.getSequenceNumber());
            taskQueue.emplace_back(executable, buffer);
        } else {
            NES_ASSERT2_FMT(false, "Pushed task for non running pipeline " << (*nextPipeline)->getPipelineId());
        }
    } else if (auto dataSink = std::get_if<DataSinkPtr>(&executable)) {
        NES_DEBUG("QueryManager: added Task for next a data sink " << (*dataSink)->toString() << " inputBuffer "
                                                                   << buffer.getOriginId()
                                                                   << " sequence:" << buffer.getSequenceNumber());
        taskQueue.emplace_back(executable, buffer);
    }
    cv.notify_all();
#else
    if (auto nextPipeline = std::get_if<Execution::ExecutablePipelinePtr>(&executable)) {
        if ((*nextPipeline)->isRunning()) {
            NES_TRACE("QueryManager: added Task for next pipeline " << (*nextPipeline)->getPipelineId() << " inputBuffer "
                                                                    << buffer);
            taskQueue.write(Task(executable, buffer));
        } else {
            NES_ASSERT2_FMT(false, "Pushed task for non running pipeline " << (*nextPipeline)->getPipelineId());
        }
    } else {
        taskQueue.write(Task(executable, buffer));
    }
#endif
}

void QueryManager::completedWork(Task& task, WorkerContext&) {
    NES_DEBUG("QueryManager::completedWork: Work for task=" << task.toString());
    if (task.isReconfiguration()) {
        return;
    }
#ifdef NES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT
    std::unique_lock lock(workMutex);
#endif
    // todo also support data sinks
    uint64_t qepId = 0;
    auto executable = task.getExecutable();
    if (auto* sink = std::get_if<DataSinkPtr>(&executable)) {
        qepId = (*sink)->getParentPlanId();
    } else if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&executable)) {
        qepId = (*executablePipeline)->getQuerySubPlanId();
    }

    if (queryToStatisticsMap.contains(qepId)) {
        auto statistics = queryToStatisticsMap.find(qepId);

        statistics->incProcessedTasks();
        statistics->incProcessedBuffers();
        auto creation = task.getBufferRef().getCreationTimestamp();
        auto now =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        auto diff = now - creation;
        statistics->incLatencySum(diff);

        auto qSize = taskQueue.size();
        statistics->incQueueSizeSum(qSize > 0 ? qSize : 0);

        statistics->incAvailableGlobalBufferSum(bufferManager->getAvailableBuffers());

        statistics->incAvailableFixedBufferSum(bufferManager->getAvailableBuffersInFixedSizePools());

#ifdef NES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT
        statistics->addTimestampToLatencyValue(now, diff);
#endif
        statistics->incProcessedTuple(task.getNumberOfTuples());
    } else {
        NES_FATAL_ERROR("queryToStatisticsMap not set, this should only happen for testing");
        NES_THROW_RUNTIME_ERROR("got buffer for not registered qep");
    }
}

Execution::ExecutableQueryPlanStatus QueryManager::getQepStatus(QuerySubPlanId id) {
    std::unique_lock lock(queryMutex);
    auto it = runningQEPs.find(id);
    if (it != runningQEPs.end()) {
        return it->second->getStatus();
    }
    return Execution::ExecutableQueryPlanStatus::Invalid;
}

std::string QueryManager::getQueryManagerStatistics() {
    std::unique_lock lock(statisticsMutex);
    std::stringstream ss;
    ss << "QueryManager Statistics:";
    for (auto& [qepId, qep] : runningQEPs) {
        auto stats = queryToStatisticsMap.find(qep->getQuerySubPlanId());
        ss << "Query=" << qepId;
        ss << "\t processedTasks =" << stats->getProcessedTasks();
        ss << "\t processedTuple =" << stats->getProcessedTuple();
        ss << "\t processedBuffers =" << stats->getProcessedBuffers();
        ss << "\t processedWatermarks =" << stats->getProcessedWatermarks();

        ss << "Source Statistics:";
        for (const auto& source : qep->getSources()) {
            ss << "Source:" << source;
            ss << "\t Generated Buffers=" << source->getNumberOfGeneratedBuffers();
            ss << "\t Generated Tuples=" << source->getNumberOfGeneratedTuples();
        }
        for (const auto& sink : qep->getSinks()) {
            ss << "Sink:" << sink;
            ss << "\t Written Buffers=" << sink->getNumberOfWrittenOutBuffers();
            ss << "\t Written Tuples=" << sink->getNumberOfWrittenOutTuples();
        }
    }
    return ss.str();
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
            if (status == Execution::Invalid) {
                NES_WARNING("Query" << qepId << "was already removed or never deployed");
                return;
            }
            NES_ASSERT(status == Execution::ExecutableQueryPlanStatus::Stopped
                           || status == Execution::ExecutableQueryPlanStatus::ErrorState,
                       "query plan " << qepId << " is not in valid state " << status);
            std::unique_lock lock(queryMutex);
            runningQEPs.erase(qepId);// note that this will release all shared pointers stored in a QEP object
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

}// namespace NES::Runtime
