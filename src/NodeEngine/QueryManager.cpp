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
#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <memory>
#include <utility>

//#define QUERY_PROCESSING_WITH_SLOWDOWN

namespace NES::NodeEngine {
using std::string;
namespace detail {

class ReconfigurationPipelineExecutionContext : public Execution::PipelineExecutionContext {
  public:
    explicit ReconfigurationPipelineExecutionContext(QuerySubPlanId queryExecutionPlanId, QueryManagerPtr queryManager)
        : Execution::PipelineExecutionContext(
            queryExecutionPlanId, queryManager, LocalBufferPoolPtr(),
            [](TupleBuffer&, NES::NodeEngine::WorkerContext&) {
            },
            [](TupleBuffer&) {
            },
            std::vector<Execution::OperatorHandlerPtr>()) {
        // nop
    }
};

class ReconfigurationEntryPointPipelineStage : public Execution::ExecutablePipelineStage {
    typedef Execution::ExecutablePipelineStage base;

  public:
    explicit ReconfigurationEntryPointPipelineStage() : base(Unary) {
        // nop
    }

    uint32_t execute(TupleBuffer& buffer, Execution::PipelineExecutionContext& pipelineContext, WorkerContextRef workerContext) {
        NES_TRACE("QueryManager: QueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint begin on thread "
                  << workerContext.getId());
        auto queryManager = pipelineContext.getQueryManager();
        auto* task = buffer.getBufferAs<ReconfigurationMessage>();
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
        return 0;
    }
};

}// namespace detail

QueryManager::QueryManager(BufferManagerPtr bufferManager, uint64_t nodeEngineId, uint16_t numThreads)
    : taskQueue(), operatorIdToQueryMap(), queryMutex(), workMutex(), bufferManager(std::move(bufferManager)),
      nodeEngineId(nodeEngineId), numThreads(numThreads), waitCounter(0) {
    NES_DEBUG("Init QueryManager::QueryManager");
    reconfigurationExecutable = std::make_shared<detail::ReconfigurationEntryPointPipelineStage>();
}

QueryManager::~QueryManager() {
    NES_DEBUG("~QueryManager()");
    destroy();
}

bool QueryManager::startThreadPool() {
    NES_DEBUG("startThreadPool: setup thread pool for nodeId=" << nodeEngineId << " with numThreads=" << numThreads);
    //Note: the shared_from_this prevents from starting this in the ctor because it expects one shared ptr from this
    NES_ASSERT(threadPool == nullptr, "thread pool already running");
    threadPool = std::make_shared<ThreadPool>(nodeEngineId, inherited0::shared_from_this(), numThreads);
    return threadPool->start();
}

void QueryManager::destroy() {
    if (waitCounter != 0) {
        NES_ERROR("QueryManager waitCounter="
                  << waitCounter
                  << " which means the source was blocked and could produce in full-speed this is maybe a problem");
    }

    if (threadPool) {
        threadPool->stop();
        threadPool.reset();
    }
    std::scoped_lock locks(queryMutex, workMutex, statisticsMutex);
    NES_DEBUG("QueryManager: Destroy Task Queue " << taskQueue.size());
    taskQueue.clear();
    NES_DEBUG("QueryManager: Destroy queryId_to_query_map " << operatorIdToQueryMap.size());

    operatorIdToQueryMap.clear();
    queryToStatisticsMap.clear();
    runningQEPs.clear();
    NES_DEBUG("QueryManager::resetQueryManager finished");
}

bool QueryManager::registerQuery(Execution::ExecutableQueryPlanPtr qep) {
    NES_DEBUG("QueryManager::registerQueryInNodeEngine: query" << qep->getQueryId() << " subquery=" << qep->getQuerySubPlanId());
    std::scoped_lock lock(queryMutex, statisticsMutex);

    bool isBinaryOperator = false;
    auto executablePipelines = qep->getPipelines();
    for (auto& pipeline : executablePipelines) {
        std::string ar = pipeline->getArity() == Unary ? "unary" : "binary";
        NES_TRACE("PIPELINE ID=" << pipeline->getPipeStageId() << " input schema=" << pipeline->getInputSchema()->toString()
                                 << " outSche=" << pipeline->getOutputSchema()->toString() << " arity=" << ar
                                 << " source code=" << pipeline->getCodeAsString());

        switch (pipeline->getArity()) {
            case Unary: {
                break;
            }
            case BinaryLeft:
            case BinaryRight: {
                isBinaryOperator |= true;
                break;
            }
        }
    }

    // test if elements already exist
    NES_DEBUG("QueryManager: resolving sources for query " << qep);
    for (const auto& source : qep->getSources()) {
        // source already exists, add qep to source set if not there
        OperatorId sourceOperatorId = source->getOperatorId();
        if (operatorIdToQueryMap.find(sourceOperatorId) != operatorIdToQueryMap.end()) {
            if (operatorIdToQueryMap[sourceOperatorId].find(qep) == operatorIdToQueryMap[sourceOperatorId].end()) {
                // qep not found in list, add it
                NES_DEBUG("QueryManager: Inserting QEP " << qep << " to Source" << sourceOperatorId);
                operatorIdToQueryMap[sourceOperatorId].insert(qep);
                queryToStatisticsMap.insert(qep->getQuerySubPlanId(),
                                            std::make_shared<QueryStatistics>(qep->getQueryId(), qep->getQuerySubPlanId()));
            } else {
                NES_DEBUG("QueryManager: Source " << sourceOperatorId << " and QEP already exist.");
                return false;
            }
            // source does not exist, add source and unordered_set containing the qep
        } else {
            NES_DEBUG("QueryManager: Source " << sourceOperatorId << " not found. Creating new element with with qep " << qep);
            std::unordered_set<Execution::ExecutableQueryPlanPtr> qepSet = {qep};
            operatorIdToQueryMap[sourceOperatorId] = qepSet;
            queryToStatisticsMap.insert(qep->getQuerySubPlanId(),
                                        std::make_shared<QueryStatistics>(qep->getQueryId(), qep->getQuerySubPlanId()));
            queryMapToOperatorId[qep->getQueryId()].push_back(sourceOperatorId);
            // by far the most obscure piece of code that we have so far :D
            // TODO refactor this using the pipeline concept 2.0
            if (isBinaryOperator) {
                NES_ASSERT(executablePipelines.size() >= 2, "Binary operator must have at least two pipelines");
                SchemaPtr sourceSchema = source->getSchema();
                for (uint64_t i = 0; i < qep->getNumberOfPipelines(); i++) {
                    auto pipelineInputSchema = executablePipelines[i]->getInputSchema();
                    NES_DEBUG("check pipeline i=" << i << " id=" << executablePipelines[i]->getPipeStageId() << " input schema="
                                                  << pipelineInputSchema->toString() << " source=" << sourceSchema->toString());
                    if (pipelineInputSchema->equals(sourceSchema)) {
                        NES_DEBUG("schema equal add entry sourceOperatorId=" << sourceOperatorId << " pipeI=" << i);
                        NES_ASSERT(operatorIdToPipelineStage.count(sourceOperatorId) == 0,
                                   "Found existing entry for the source operator " << sourceOperatorId);
                        operatorIdToPipelineStage[sourceOperatorId] = i;
                    } else {
                        NES_TRACE("source not equal");
                    }
                }
            } else {
                //default fall back, if there is no join, then we always execute the pipeline at id 0
                operatorIdToPipelineStage[sourceOperatorId] = 0;
            }
        }
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

bool QueryManager::startQuery(Execution::ExecutableQueryPlanPtr qep) {
    NES_DEBUG("QueryManager::startQuery: query id " << qep->getQuerySubPlanId() << " " << qep->getQueryId());
    NES_ASSERT(qep->getStatus() == Execution::ExecutableQueryPlanStatus::Created,
               "Invalid status for starting the QEP " << qep->getQuerySubPlanId());

    // TODO do not change the start sequence plz
    // 1. start the qep and handlers, if any
    if (!qep->setup() || !qep->start()) {
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

bool QueryManager::deregisterQuery(Execution::ExecutableQueryPlanPtr qep) {
    NES_DEBUG("QueryManager::deregisterAndUndeployQuery: query" << qep);

    std::unique_lock lock(queryMutex);
    bool succeed = true;
    auto sources = qep->getSources();

    for (const auto& source : sources) {
        NES_DEBUG("QueryManager: stop source " << source->toString());
        if (operatorIdToQueryMap.find(source->getOperatorId()) != operatorIdToQueryMap.end()) {
            // source exists, remove qep from source if there
            if (operatorIdToQueryMap[source->getOperatorId()].find(qep) != operatorIdToQueryMap[source->getOperatorId()].end()) {
                // qep found, remove it
                NES_DEBUG("QueryManager: Removing QEP " << qep << " from source" << source->getOperatorId());
                if (operatorIdToQueryMap[source->getOperatorId()].erase(qep) == 0) {
                    NES_FATAL_ERROR("QueryManager: Removing QEP " << qep << " for source " << source->getOperatorId()
                                                                  << " failed!");
                    succeed = false;
                }

                // if source has no qeps remove the source from map
                if (operatorIdToQueryMap[source->getOperatorId()].empty()) {
                    if (operatorIdToQueryMap.erase(source->getOperatorId()) == 0) {
                        NES_FATAL_ERROR("QueryManager: Removing source " << source->getOperatorId() << " failed!");
                        succeed = false;
                    }
                }
            }
        }
    }
    return succeed;
}

bool QueryManager::failQuery(Execution::ExecutableQueryPlanPtr) {
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

bool QueryManager::stopQuery(Execution::ExecutableQueryPlanPtr qep, bool graceful) {
    NES_DEBUG("QueryManager::stopQuery: query sub-plan id " << qep->getQuerySubPlanId() << " graceful=" << graceful);
    bool ret = true;
    std::unique_lock lock(queryMutex);
    // here im using COW to avoid keeping the lock for long
    // however, this is not a long-term fix
    // because it wont lead to correct behaviour
    // under heavy query deployment ops
    auto sources = qep->getSources();
    auto copiedSources = std::vector(sources.begin(), sources.end());
    lock.unlock();

    if (qep->getStatus() != Execution::Running) {
        return true;
    }

    for (const auto& source : copiedSources) {
        if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
            source->stop(true);
        }
    }

    {
        std::unique_lock taskLock(workMutex);
        NES_WARNING("Number of tasks in queue when stopped=" << taskQueue.size());
    }
    // TODO evaluate if we need to have this a wait instead of a get
    // TODO for instance we could wait N seconds and if the stopped is not succesful by then
    // TODO we need to trigger a hard local kill of a QEP
    if (qep->getTerminationFuture().get() != Execution::ExecutableQueryPlanResult::Ok) {
        NES_FATAL_ERROR("QueryManager: QEP " << qep->getQuerySubPlanId() << " could not be stopped");
        ret = false;
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
    std::unique_lock workLock(workMutex);
    return taskQueue.size();
}

void QueryManager::addWork(const OperatorId operatorId, TupleBuffer& buf) {
    std::shared_lock queryLock(queryMutex);// we need this lock because operatorIdToQueryMap can be concurrently modified
#ifdef EXTENDEDDEBUGGING
    std::stringstream ss;
    ss << " sourceid=" << operatorId << "map at operatorIdToQueryMap ";
    for (std::map<uint64_t, std::unordered_set<QueryExecutionPlanPtr>>::const_iterator it = operatorIdToQueryMap.begin();
         it != operatorIdToQueryMap.end(); ++it) {
        ss << " operatorId=" << it->first;
        for (auto& a : it->second) {
            ss << " \t qepID=" << a->getQueryId() << " subqep=" << a->getQuerySubPlanId() << " pipelines=" << a->getStageSize();
        }
    }

    std::stringstream ss2;
    ss2 << " sourceid=" << operatorId << "map at operatorIdToPipelineStage ";
    for (std::map<uint64_t, uint64_t>::const_iterator it = operatorIdToPipelineStage.begin();
         it != operatorIdToPipelineStage.end(); ++it) {
        ss2 << " operatorId=" << it->first << " pipeStage=" << it->second;
    }

    std::stringstream ss3;
    ss3 << " sourceid=" << operatorId << "map at queryMapToOperatorId ";
    for (std::map<uint64_t, std::vector<uint64_t>>::const_iterator it = queryMapToOperatorId.begin();
         it != queryMapToOperatorId.end(); ++it) {
        ss3 << " queryID=" << it->first;
        for (auto& a : it->second) {
            ss3 << "operatorId=" << a;
        }
    }

    NES_TRACE(ss.str());
    NES_TRACE(ss2.str());
    NES_TRACE(ss3.str());
#endif
    NES_VERIFY(operatorIdToQueryMap[operatorId].size() > 0, "Operator id to query map for operator is empty");
    for (const auto& qep : operatorIdToQueryMap[operatorId]) {
        // for each respective source, create new task and put it into queue
        // TODO: change that in the future that stageId is used properly with #1354
        if (operatorIdToPipelineStage.find(operatorId) == operatorIdToPipelineStage.end()) {
            NES_THROW_RUNTIME_ERROR("Operator ID=" << operatorId << " not found in mapping table");
        }
        uint64_t stageId = operatorIdToPipelineStage[operatorId];
        NES_DEBUG("run task for operatorID=" << operatorId << " with pipeline=" << operatorIdToPipelineStage[operatorId]);
#ifdef QUERY_PROCESSING_WITH_SLOWDOWN//the following code is the old break that we had, we leave it in to maybe activate it again later
        auto tryCnt = 0;
        //TODO: this very simple rule ensures that sources can only get buffer if more than 10% of the overall buffer exists
        uint64_t upperBound = threadPool->getNumberOfThreads() * 10000;
        while (bufferManager->getAvailableBuffers() < bufferManager->getNumOfPooledBuffers() * 0.1
               || taskQueue.size() > upperBound) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            waitCounter++;
            NES_DEBUG("Waiting");
            //TODO: we have to do this because it could be that a source is stuck here and then the shutdown crashes, so basically we test 100x for 100ms
            // and then release the break
            if (tryCnt++ == 100) {
                break;
            }
        }
#endif

        //TODO: this is a problem now as it can become the bottleneck
        std::unique_lock workQueueLock(workMutex);
        taskQueue.emplace_back(qep->getPipeline(operatorIdToPipelineStage[operatorId]), buf);

        NES_TRACE("QueryManager: added Task for addWork" << taskQueue.back().toString() << " for query " << operatorId
                                                         << " for QEP " << qep << " inputBuffer " << buf
                                                         << " orgID=" << buf.getOriginId() << " stageID=" << stageId);
        cv.notify_all();
    }
}

bool QueryManager::addReconfigurationMessage(QuerySubPlanId queryExecutionPlanId, ReconfigurationMessage message, bool blocking) {
    NES_DEBUG("QueryManager: QueryManager::addReconfigurationMessage begins on plan "
              << queryExecutionPlanId << " blocking=" << blocking << " type " << message.getType());
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
    auto optBuffer = bufferManager->getUnpooledBuffer(sizeof(ReconfigurationMessage));
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();
    auto task = new (buffer.getBuffer())
        ReconfigurationMessage(message, threadPool->getNumberOfThreads(), blocking);// memcpy using copy ctor
    auto pipelineContext =
        std::make_shared<detail::ReconfigurationPipelineExecutionContext>(queryExecutionPlanId, inherited0::shared_from_this());
    auto pipeline = Execution::ExecutablePipeline::create(-1, queryExecutionPlanId, reconfigurationExecutable, pipelineContext, 1,
                                                          nullptr, nullptr, nullptr, true);
    {
        std::unique_lock lock(workMutex);
        for (auto i = 0; i < threadPool->getNumberOfThreads(); ++i) {
            taskQueue.emplace_back(pipeline, buffer);
        }
        cv.notify_all();
    }

    if (blocking) {
        task->postWait();
        task->postReconfiguration();
    }
    return true;
}

bool QueryManager::addEndOfStream(OperatorId sourceId, bool graceful) {
    std::shared_lock queryLock(queryMutex);
    //@Ventrua we have to do this because otherwise we can run into the situation to get threads from two barriers waiting
    std::unique_lock lock(workMutex);

    NES_DEBUG("QueryManager: QueryManager::addEndOfStream for source operator " << sourceId << " graceful=" << graceful);
    NES_VERIFY(operatorIdToQueryMap[sourceId].size() > 0, "Operator id to query map for operator is empty");
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool no longer running");
    auto reconfigType = graceful ? SoftEndOfStream : HardEndOfStream;
    for (const auto& qep : operatorIdToQueryMap[sourceId]) {
        TupleBuffer buffer;
        auto queryExecutionPlanId = qep->getQuerySubPlanId();
        if (graceful) {
            // reconfigure at stage level first
            auto targetStage = operatorIdToPipelineStage[sourceId];
            auto weakQep = std::make_any<std::weak_ptr<Execution::ExecutableQueryPlan>>(qep);
            auto optBuffer = bufferManager->getUnpooledBuffer(sizeof(ReconfigurationMessage));
            NES_ASSERT(!!optBuffer, "invalid buffer");
            buffer = optBuffer.value();
            //use in-place construction to create the reconfig task within a buffer
            new (buffer.getBuffer()) ReconfigurationMessage(queryExecutionPlanId, reconfigType, threadPool->getNumberOfThreads(),
                                                            qep->getPipeline(targetStage), std::move(weakQep));
            NES_DEBUG("QueryManager: QueryManager::addEndOfStream for source operator " << sourceId << " graceful=" << graceful
                                                                                        << " to stage " << targetStage);
        } else {
            // reconfigure at qep level
            auto optBuffer = bufferManager->getUnpooledBuffer(sizeof(ReconfigurationMessage));
            NES_ASSERT(!!optBuffer, "invalid buffer");
            buffer = optBuffer.value();

            //use in-place construction to create the reconfig task within a buffer
            new (buffer.getBuffer())
                ReconfigurationMessage(queryExecutionPlanId, reconfigType, threadPool->getNumberOfThreads(), qep);
            NES_WARNING("EOS opId=" << sourceId << " reconfType=" << reconfigType << " queryExecutionPlanId=" << queryExecutionPlanId
                        <<  " threadPool->getNumberOfThreads()=" << threadPool->getNumberOfThreads() << " qep" << qep->getQueryId()
                        );

        }

        auto pipelineContext = std::make_shared<detail::ReconfigurationPipelineExecutionContext>(queryExecutionPlanId,
                                                                                                 inherited0::shared_from_this());
        auto pipeline = Execution::ExecutablePipeline::create(
            /** default pipeline Id*/ -1, queryExecutionPlanId, reconfigurationExecutable, pipelineContext,
            /** numberOfProducingPipelines**/ 1, nullptr, nullptr, nullptr, true);
        {
//            std::unique_lock lock(workMutex);
            for (auto i = 0; i < threadPool->getNumberOfThreads(); ++i) {
                if (graceful) {
                    taskQueue.emplace_back(pipeline, buffer);
                } else {
//                    taskQueue.emplace_front(pipeline, buffer);
                    //todo redo this
                    taskQueue.emplace_back(pipeline, buffer);
                }
            }
            cv.notify_all();
        }
    }
    return true;
}

QueryManager::ExecutionResult QueryManager::processNextTask(std::atomic<bool>& running, WorkerContext& workerContext) {
    NES_TRACE("QueryManager: QueryManager::getWork wait get lock");
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
        NES_TRACE("QueryManager: provide task" << task.toString() << " to thread (getWork())");
        taskQueue.pop_front();
        lock.unlock();
        auto ret = task(workerContext);
        if (ret) {
            completedWork(task, workerContext);
            return Ok;
        }
        return Error;
    } else {
        NES_DEBUG("QueryManager: Thread pool was shut down but has still tasks");
        lock.unlock();
        return terminateLoop(workerContext);
    }
}

QueryManager::ExecutionResult QueryManager::terminateLoop(WorkerContext& workerContext) {
    std::unique_lock lock(workMutex);
    //    this->threadBarrier->wait();
    // must run this to execute all pending reconfiguration task (Destroy)
    bool hitReconfiguration = false;
    while (!taskQueue.empty()) {
        auto task = taskQueue.front();
        taskQueue.pop_front();
        lock.unlock();
        if (!hitReconfiguration) {// execute all pending tasks until first reconfiguration
            task(workerContext);
            if (task.getPipeline()->isReconfiguration()) {
                hitReconfiguration = true;
            }
            lock.lock();
            continue;
        } else {
            if (task.getPipeline()->isReconfiguration()) {// execute only pending reconfigurations
                task(workerContext);
            }
            lock.lock();
        }
    }
    lock.unlock();
    return Finished;
}

void QueryManager::addWorkForNextPipeline(TupleBuffer& buffer, Execution::ExecutablePipelinePtr nextPipeline) {
    std::unique_lock lock(workMutex);
    // dispatch buffer as task
    auto it = runningQEPs.find(nextPipeline->getQepParentId());
    if (it != runningQEPs.end() && it->second->getStatus() == Execution::Running) {
        NES_TRACE("QueryManager: added Task for next pipeline  " << taskQueue.back().toString() << " for nextPipeline "
                                                                 << nextPipeline->getPipeStageId() << " inputBuffer " << buffer);
        taskQueue.emplace_back(std::move(nextPipeline), buffer);
        cv.notify_all();
    } else {
        NES_ERROR("Pushed task for non running pipeline " << it->second->getQuerySubPlanId());
    }
}

void QueryManager::completedWork(Task& task, WorkerContext&) {
    NES_TRACE("QueryManager::completedWork: Work for task=" << task.toString());
    if (queryToStatisticsMap.contains(task.getPipeline()->getQepParentId())) {
        auto statistics = queryToStatisticsMap.find(task.getPipeline()->getQepParentId());

        statistics->incProcessedTasks();
        if (task.isWatermarkOnly()) {
            statistics->incProcessedWatermarks();
        } else if (!task.getPipeline()->isReconfiguration()) {
            statistics->incProcessedBuffers();
            auto* latency = task.getBufferRef().getBufferAs<uint64_t>();
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::
                                                                             now().time_since_epoch()).count();
            auto diff = now - latency[2];
//            NES_ERROR("read lat2=" << latency[2] << " now=" << now << " diff=" << diff);
            statistics->incLatencySum(diff);
        }
        statistics->incProcessedTuple(task.getNumberOfTuples());


    } else {
        NES_WARNING("queryToStatisticsMap not set, this should only happen for testing");
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
            NES_ASSERT(status == Execution::ExecutableQueryPlanStatus::Stopped
                           || status == Execution::ExecutableQueryPlanStatus::ErrorState,
                       "query plan is not in valid state " << status);
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

}// namespace NES::NodeEngine
