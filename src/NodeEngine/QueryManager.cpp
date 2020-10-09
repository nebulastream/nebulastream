#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <Util/Logger.hpp>
#include <Windowing/Runtime/WindowHandler.hpp>
#include <iostream>
#include <memory>
#include <utility>

namespace NES {
using std::string;
namespace detail {
uint32_t reconfigurationTaskEntryPoint(TupleBuffer& buffer, void*, WindowManager*, PipelineExecutionContext&, WorkerContextRef workerContext) {
    NES_TRACE("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint begin on thread " << workerContext.getId());
    auto* task = buffer.getBufferAs<ReconfigurationTask>();
    NES_TRACE("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint going to wait on thread " << workerContext.getId());
    task->wait();
    NES_TRACE("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint going to reconfigure on thread " << workerContext.getId());
    task->getInstance()->reconfigure(*task, workerContext);
    NES_TRACE("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint post callback on thread " << workerContext.getId());
    task->postReconfiguration();
    NES_TRACE("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint completed on thread " << workerContext.getId());
    task->postWait();
    return 0;
}
}// namespace detail

QueryManager::QueryManager(BufferManagerPtr bufferManager, uint64_t nodeEngineId)
    : taskQueue(), sourceIdToQueryMap(), queryMutex(), workMutex(), bufferManager(std::move(bufferManager)), nodeEngineId(nodeEngineId) {
    NES_DEBUG("Init QueryManager::QueryManager");
    reconfigurationExecutable = std::make_shared<CompiledExecutablePipeline>(detail::reconfigurationTaskEntryPoint);
}

QueryManager::~QueryManager() {
    NES_DEBUG("~QueryManager()");
    destroy();
}

bool QueryManager::startThreadPool() {
    NES_DEBUG("startThreadPool: setup thread pool for nodeId=" << nodeEngineId);
    //Note: the shared_from_this prevents from starting this in the ctor because it expects one shared ptr from this
    NES_ASSERT(threadPool == nullptr, "thread pool already running");
    threadPool = std::make_shared<ThreadPool>(nodeEngineId, shared_from_this());
    nodeEngineId = nodeEngineId;
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
    NES_DEBUG("QueryManager: Destroy queryId_to_query_map " << sourceIdToQueryMap.size());

    sourceIdToQueryMap.clear();
    queryToStatisticsMap.clear();

    NES_DEBUG("QueryManager::resetQueryManager finished");
}

bool QueryManager::registerQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("QueryManager::registerQueryInNodeEngine: query" << qep);
    std::scoped_lock lock(queryMutex, statisticsMutex);

    // test if elements already exist
    NES_DEBUG("QueryManager: resolving sources for query " << qep);
    for (const auto& source : qep->getSources()) {
        if (sourceIdToQueryMap.find(source->getSourceId()) != sourceIdToQueryMap.end()) {
            // source already exists, add qep to source set if not there
            if (sourceIdToQueryMap[source->getSourceId()].find(qep) == sourceIdToQueryMap[source->getSourceId()].end()) {
                // qep not found in list, add it
                NES_DEBUG("QueryManager: Inserting QEP " << qep << " to Source" << source->getSourceId());
                sourceIdToQueryMap[source->getSourceId()].insert(qep);
                queryToStatisticsMap.insert(qep->getQuerySubPlanId(), std::make_shared<QueryStatistics>());
            } else {
                NES_DEBUG("QueryManager: Source " << source->getSourceId() << " and QEP already exist.");
                return false;
            }
        } else {
            // source does not exist, add source and unordered_set containing the qep
            NES_DEBUG("QueryManager: Source " << source->getSourceId()
                                              << " not found. Creating new element with with qep " << qep);
            std::unordered_set<QueryExecutionPlanPtr> qepSet = {qep};
            sourceIdToQueryMap[source->getSourceId()] = qepSet;
            queryToStatisticsMap.insert(qep->getQuerySubPlanId(), std::make_shared<QueryStatistics>());
        }
    }
    return true;
}

bool QueryManager::startQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("QueryManager::startQuery: query id " << qep->getQuerySubPlanId() << " " << qep->getQueryId());
    NES_ASSERT(qep->getStatus() == QueryExecutionPlan::QueryExecutionPlanStatus::Created, "Invalid status for starting the QEP " << qep->getQuerySubPlanId());

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

bool QueryManager::deregisterQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("QueryManager::deregisterAndUndeployQuery: query" << qep);

    std::unique_lock lock(queryMutex);
    bool succeed = true;
    auto sources = qep->getSources();

    for (const auto& source : sources) {
        NES_DEBUG("QueryManager: stop source " << source->toString());
        if (sourceIdToQueryMap.find(source->getSourceId()) != sourceIdToQueryMap.end()) {
            // source exists, remove qep from source if there
            if (sourceIdToQueryMap[source->getSourceId()].find(qep) != sourceIdToQueryMap[source->getSourceId()].end()) {
                // qep found, remove it
                NES_DEBUG("QueryManager: Removing QEP " << qep << " from source" << source->getSourceId());
                if (sourceIdToQueryMap[source->getSourceId()].erase(qep) == 0) {
                    NES_FATAL_ERROR("QueryManager: Removing QEP " << qep << " for source " << source->getSourceId()
                                                                  << " failed!");
                    succeed = false;
                }

                // if source has no qeps remove the source from map
                if (sourceIdToQueryMap[source->getSourceId()].empty()) {
                    if (sourceIdToQueryMap.erase(source->getSourceId()) == 0) {
                        NES_FATAL_ERROR("QueryManager: Removing source " << source->getSourceId() << " failed!");
                        succeed = false;
                    }
                }
            }
        }
    }
    return succeed;
}

bool QueryManager::stopQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("QueryManager::stopQuery: query" << qep);
    {
        std::unique_lock lock(queryMutex);

        auto sources = qep->getSources();
        for (const auto& source : sources) {
            NES_DEBUG("QueryManager: stop source " << source->toString());
            // TODO what if two qeps use the same source

            if (sourceIdToQueryMap[source->getSourceId()].size() != 1) {
                NES_WARNING("QueryManager: could not stop source " << source->toString() << " because other qeps are using it n="
                                                                   << sourceIdToQueryMap[source->getSourceId()].size());
            } else {
                NES_DEBUG("QueryManager: stop source " << source->toString() << " because only " << qep << " is using it");
                bool success = source->stop();
                if (!success) {
                    NES_ERROR("QueryManager: could not stop source " << source->toString());
                    return false;
                } else {
                    NES_DEBUG("QueryManager: source " << source->toString() << " successfully stopped");
                }
            }
        }
        NES_DEBUG("QueryManager::stopQuery: query finished " << qep);
    }
    if (!qep->stop()) {
        NES_FATAL_ERROR("QueryManager: QEP could not be stopped");
        return false;
    };

    auto sinks = qep->getSinks();
    for (const auto& sink : sinks) {
        NES_DEBUG("QueryManager: stop sink " << sink->toString());
        // TODO: do we also have to prevent to shutdown sink that is still used by another qep
        sink->shutdown();
    }
    addReconfigurationTask(qep->getQuerySubPlanId(), ReconfigurationTask(qep->getQuerySubPlanId(), Destroy, this), true);
    return true;
}

bool QueryManager::addReconfigurationTask(QuerySubPlanId queryExecutionPlanId, ReconfigurationTask descriptor, bool blocking) {
    NES_DEBUG("QueryManager: QueryManager::addReconfigurationTask begins on plan " << queryExecutionPlanId);
    auto optBuffer = bufferManager->getUnpooledBuffer(sizeof(ReconfigurationTask));
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();
    auto task = new (buffer.getBuffer()) ReconfigurationTask(descriptor, threadPool->getNumberOfThreads(), blocking);// memcpy using copy ctor
    auto pipelineContext = std::make_shared<PipelineExecutionContext>(queryExecutionPlanId, bufferManager, [](TupleBuffer&, NES::WorkerContext&) {
    });
    auto pipeline = PipelineStage::create(-1, queryExecutionPlanId, reconfigurationExecutable, pipelineContext, nullptr);
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
            if (task.getPipelineStage()->isReconfiguration()) {
                hitReconfiguration = true;
            }
            lock.lock();
            continue;
        } else {
            if (task.getPipelineStage()->isReconfiguration()) {// execute only pending reconfigurations
                task(workerContext);
            }
            lock.lock();
        }
    }
    lock.unlock();
    return Finished;
}

void QueryManager::addWorkForNextPipeline(TupleBuffer& buffer, PipelineStagePtr nextPipeline) {
    std::unique_lock lock(workMutex);

    // dispatch buffer as task
    taskQueue.emplace_back(std::move(nextPipeline), buffer);
    NES_TRACE("QueryManager: added Task " << taskQueue.back().toString() << " for nextPipeline " << nextPipeline << " inputBuffer " << buffer);

    cv.notify_all();
}

void QueryManager::addWork(const std::string& sourceId, TupleBuffer& buf) {
    std::shared_lock queryLock(queryMutex);// we need this lock because sourceIdToQueryMap can be concurrently modified
    std::unique_lock workQueueLock(workMutex);
    for (const auto& qep : sourceIdToQueryMap[sourceId]) {
        // for each respective source, create new task and put it into queue
        // TODO: change that in the future that stageId is used properly
        taskQueue.emplace_back(qep->getStage(0), buf);
        NES_DEBUG("QueryManager: added Task " << taskQueue.back().toString() << " for query " << sourceId << " for QEP " << qep
                                              << " inputBuffer " << buf);
    }
    cv.notify_all();
}

void QueryManager::completedWork(Task& task, WorkerContext&) {
    NES_INFO("QueryManager::completedWork: Work for task=" << task.toString());
    std::unique_lock lock(statisticsMutex);
    auto statistics = queryToStatisticsMap.find(task.getPipelineStage()->getQepParentId());

    statistics->incProcessedTasks();
    if (task.isWatermarkOnly()) {
        statistics->incProcessedWatermarks();
    } else if (!task.getPipelineStage()->isReconfiguration()) {
        statistics->incProcessedBuffers();
    }
    statistics->incProcessedTuple(task.getNumberOfTuples());
}

QueryExecutionPlan::QueryExecutionPlanStatus QueryManager::getQepStatus(QuerySubPlanId id) {
    std::unique_lock lock(queryMutex);
    auto it = runningQEPs.find(id);
    if (it != runningQEPs.end()) {
        return it->second->getStatus();
    }
    return QueryExecutionPlan::QueryExecutionPlanStatus::Invalid;
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
            NES_ASSERT(status == QueryExecutionPlan::Stopped || status == QueryExecutionPlan::ErrorState, "query plan is not in valid state " << status);
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

}// namespace NES
