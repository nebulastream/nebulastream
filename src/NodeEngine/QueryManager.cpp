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

#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <memory>
#include <utility>

namespace NES::NodeEngine {
using std::string;
namespace detail {

class ReconfigurationTaskEntryPointPipelineStage : public Execution::ExecutablePipelineStage {
  public:
    uint32_t execute(TupleBuffer& buffer, Execution::PipelineExecutionContext&, WorkerContextRef workerContext) {
        NES_TRACE("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint begin on thread "
                  << workerContext.getId());
        auto* task = buffer.getBufferAs<ReconfigurationTask>();
        NES_TRACE("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint going to wait on thread "
                  << workerContext.getId());
        task->wait();
        NES_TRACE(
            "QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint going to reconfigure on thread "
            << workerContext.getId());
        task->getInstance()->reconfigure(*task, workerContext);
        NES_TRACE("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint post callback on thread "
                  << workerContext.getId());
        task->postReconfiguration();
        NES_TRACE("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint completed on thread "
                  << workerContext.getId());
        task->postWait();
        return 0;
    }
};

}// namespace detail

QueryManager::QueryManager(BufferManagerPtr bufferManager, uint64_t nodeEngineId, uint16_t numThreads)
    : taskQueue(), operatorIdToQueryMap(), queryMutex(), workMutex(), bufferManager(std::move(bufferManager)),
      nodeEngineId(nodeEngineId), numThreads(numThreads) {
    NES_DEBUG("Init QueryManager::QueryManager");
    reconfigurationExecutable = std::make_shared<detail::ReconfigurationTaskEntryPointPipelineStage>();
}

QueryManager::~QueryManager() {
    NES_DEBUG("~QueryManager()");
    destroy();
}

bool QueryManager::startThreadPool() {
    NES_DEBUG("startThreadPool: setup thread pool for nodeId=" << nodeEngineId << " with numThreads=" << numThreads);
    //Note: the shared_from_this prevents from starting this in the ctor because it expects one shared ptr from this
    NES_ASSERT(threadPool == nullptr, "thread pool already running");
    threadPool = std::make_shared<ThreadPool>(nodeEngineId, shared_from_this(), numThreads);
    return threadPool->start();
}

void QueryManager::destroy() {
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

    NES_DEBUG("QueryManager::resetQueryManager finished");
}

bool QueryManager::registerQuery(Execution::ExecutableQueryPlanPtr qep) {
    NES_DEBUG("QueryManager::registerQueryInNodeEngine: query" << qep);
    std::scoped_lock lock(queryMutex, statisticsMutex);

    bool isBinaryOperator = false;

    // test if elements already exist
    NES_DEBUG("QueryManager: resolving sources for query " << qep);
    for (const auto& source : qep->getSources()) {
        // source already exists, add qep to source set if not there
        if (operatorIdToQueryMap.find(source->getOperatorId()) != operatorIdToQueryMap.end()) {
            if (operatorIdToQueryMap[source->getOperatorId()].find(qep) == operatorIdToQueryMap[source->getOperatorId()].end()) {
                // qep not found in list, add it
                NES_DEBUG("QueryManager: Inserting QEP " << qep << " to Source" << source->getOperatorId());
                operatorIdToQueryMap[source->getOperatorId()].insert(qep);
                queryToStatisticsMap.insert(qep->getQuerySubPlanId(), std::make_shared<QueryStatistics>());
                //                NES_DEBUG("QueryManager: Join QEP already found " << qep << " to Source" << source->getOperatorId() << " add pipeline stage 1");
                //                operatorIdToPipelineStage[source->getOperatorId()] = 1;
            } else {
                NES_DEBUG("QueryManager: Source " << source->getOperatorId() << " and QEP already exist.");
                return false;
            }
            // source does not exist, add source and unordered_set containing the qep
        } else {
            NES_DEBUG("QueryManager: Source " << source->getOperatorId() << " not found. Creating new element with with qep "
                                              << qep);
            std::unordered_set<Execution::ExecutableQueryPlanPtr> qepSet = {qep};
            operatorIdToQueryMap[source->getOperatorId()] = qepSet;
            queryToStatisticsMap.insert(qep->getQuerySubPlanId(), std::make_shared<QueryStatistics>());
            queryMapToOperatorId[qep->getQueryId()].push_back(source->getOperatorId());
            if (isBinaryOperator) {
                if (queryMapToOperatorId[qep->getQueryId()].size() == 1) {
                    NES_DEBUG("QueryManager: mm.size() == 1 " << qep << " to Source" << source->getOperatorId());
                    operatorIdToPipelineStage[source->getOperatorId()] = 0;
                } else {
                    operatorIdToPipelineStage[source->getOperatorId()] = 1;
                }
            }
            NES_DEBUG("QueryManager: mm.size() > 1 " << qep << " to Source" << source->getOperatorId());
        }
    }

    return true;
}

void QueryManager::addWork(const OperatorId operatorId, TupleBuffer& buf) {
    std::shared_lock queryLock(queryMutex);// we need this lock because operatorIdToQueryMap can be concurrently modified
    std::unique_lock workQueueLock(workMutex);
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
    for (const auto& qep : operatorIdToQueryMap[operatorId]) {
        // for each respective source, create new task and put it into queue
        // TODO: change that in the future that stageId is used properly
        uint64_t stageId = operatorIdToPipelineStage[operatorId];
        taskQueue.emplace_back(qep->getPipeline(operatorIdToPipelineStage[operatorId]), buf);

        NES_DEBUG("QueryManager: added Task for addWork" << taskQueue.back().toString() << " for query " << operatorId
                                                         << " for QEP " << qep << " inputBuffer " << buf
                                                         << " orgID=" << buf.getOriginId() << " stageID=" << stageId);
    }
    cv.notify_all();
}

bool QueryManager::startQuery(Execution::ExecutableQueryPlanPtr qep) {
    NES_DEBUG("QueryManager::startQuery: query id " << qep->getQuerySubPlanId() << " " << qep->getQueryId());
    NES_ASSERT(qep->getStatus() == Execution::ExecutableQueryPlanStatus::Created,
               "Invalid status for starting the QEP " << qep->getQuerySubPlanId());

    for (const auto& sink : qep->getSinks()) {
        NES_DEBUG("QueryManager: start sink " << sink);
        sink->setup();
    }

    if (!qep->setup() || !qep->start()) {
        NES_FATAL_ERROR("QueryManager: query execution plan could not started");
        return false;
    }

    for (const auto& source : qep->getSources()) {
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

bool QueryManager::stopQuery(Execution::ExecutableQueryPlanPtr qep) {
    NES_DEBUG("QueryManager::stopQuery: query" << qep);
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
                NES_DEBUG("QueryManager: stop source " << source->toString() << " because only " << qep << " is using it");
                bool success = source->stop();
                if (!success) {
                    NES_ERROR("QueryManager: could not stop source " << source->toString());
                    ret = false;
                } else {
                    NES_DEBUG("QueryManager: source " << source->toString() << " successfully stopped");
                }
            }
        }
        NES_DEBUG("QueryManager::stopQuery: query finished " << qep);
    }
    if (!qep->stop()) {
        NES_FATAL_ERROR("QueryManager: QEP could not be stopped");
        ret = false;
    };

    auto sinks = qep->getSinks();
    for (const auto& sink : sinks) {
        NES_DEBUG("QueryManager: stop sink " << sink->toString());
        // TODO: do we also have to prevent to shutdown sink that is still used by another qep
        sink->shutdown();
    }
    addReconfigurationTask(qep->getQuerySubPlanId(), ReconfigurationTask(qep->getQuerySubPlanId(), Destroy, this), true);
    return ret;
}

bool QueryManager::addReconfigurationTask(QuerySubPlanId queryExecutionPlanId, ReconfigurationTask descriptor, bool blocking) {
    NES_DEBUG("QueryManager: QueryManager::addReconfigurationTask begins on plan " << queryExecutionPlanId);
    auto optBuffer = bufferManager->getUnpooledBuffer(sizeof(ReconfigurationTask));
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();
    auto task = new (buffer.getBuffer())
        ReconfigurationTask(descriptor, threadPool->getNumberOfThreads(), blocking);// memcpy using copy ctor
    auto pipelineContext = std::make_shared<Execution::PipelineExecutionContext>(
        queryExecutionPlanId, bufferManager,
        [](TupleBuffer&, NES::NodeEngine::WorkerContext&) {
        },
        [](TupleBuffer&) {
        },
        std::vector<Execution::OperatorHandlerPtr>());
    auto pipeline = Execution::ExecutablePipeline::create(-1, queryExecutionPlanId, reconfigurationExecutable, pipelineContext,
                                                          nullptr, true);
    {
        std::unique_lock lock(workMutex);
        for (auto i = 0; i < threadPool->getNumberOfThreads(); ++i) {
            taskQueue.emplace_back(pipeline, buffer);
        }
        cv.notify_all();
        workMutex.unlock();
    }
    if (blocking) {
        task->postWait();
        task->postReconfiguration();
    }
    return true;
}

QueryManager::ExecutionResult QueryManager::processNextTask(std::atomic<bool>& running, WorkerContext& workerContext) {
    NES_DEBUG("QueryManager: QueryManager::getWork wait get lock");
    std::unique_lock lock(workMutex);
    NES_DEBUG("QueryManager:getWork wait got lock");
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
    NES_DEBUG("QueryManager::getWork queue is not empty");
    // there is a potential task in the queue and the thread pool is running
    if (running) {
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
    taskQueue.emplace_back(std::move(nextPipeline), buffer);
    NES_DEBUG("QueryManager: added Task for next pipeline  " << taskQueue.back().toString() << " for nextPipeline "
                                                             << nextPipeline << " inputBuffer " << buffer);
    cv.notify_all();
}

void QueryManager::completedWork(Task& task, WorkerContext&) {
    NES_INFO("QueryManager::completedWork: Work for task=" << task.toString());
    std::unique_lock lock(statisticsMutex);
    if (queryToStatisticsMap.contains(task.getPipeline()->getQepParentId())) {
        auto statistics = queryToStatisticsMap.find(task.getPipeline()->getQepParentId());

        statistics->incProcessedTasks();
        if (task.isWatermarkOnly()) {
            statistics->incProcessedWatermarks();
        } else if (!task.getPipeline()->isReconfiguration()) {
            statistics->incProcessedBuffers();
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

void QueryManager::reconfigure(ReconfigurationTask& task, WorkerContext& context) {
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

void QueryManager::destroyCallback(ReconfigurationTask& task) {
    Reconfigurable::destroyCallback(task);
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

}// namespace NES::NodeEngine
