#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <Windows/WindowHandler.hpp>
#include <iostream>
#include <memory>

namespace NES {
using std::string;

QueryManager::QueryManager()
      : queryMutex(), workMutex() {
    NES_DEBUG("QueryManager()");
}


QueryManager::~QueryManager() {
    NES_DEBUG("~QueryManager()");
    NES_DEBUG("QueryManager(): stop thread pool");
    NES_DEBUG("ResetQueryManager");
    resetQueryManager();
}

bool QueryManager::startThreadPool() {
    NES_DEBUG("startThreadPool: setup thread pool ");
    //Note: the shared_from_this prevents from starting this in the ctor because it expects one shared ptr from this
    threadPool = std::make_shared<ThreadPool>(shared_from_this());
    return threadPool->start();
}

bool QueryManager::stopThreadPool() {
    NES_DEBUG("QueryManager::stopThreadPool: stop");
    return threadPool->stop();
}

void QueryManager::resetQueryManager() {
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
                queryToStatisticsMap.insert(qep->getQueryId(), std::make_shared<QueryStatistics>());
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
            queryToStatisticsMap.insert(qep->getQueryId(), std::make_shared<QueryStatistics>());
        }
    }
    return true;
}

bool QueryManager::startQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("QueryManager::startQuery: query" << qep);
    std::unique_lock lock(queryMutex);

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

    runningQEPs.emplace(qep);
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
    NES_DEBUG("QueryManager::stopQuery: query finished " << qep);
    runningQEPs.erase(qep);
    return true;
}

Task QueryManager::getWork(std::atomic<bool>& threadPool_running) {
    NES_DEBUG("QueryManager: QueryManager::getWork wait get lock");
    std::unique_lock<std::mutex> lock(workMutex);
    NES_DEBUG("QueryManager:getWork wait got lock");
    // wait while queue is empty but thread pool is running
    while (taskQueue.empty() && threadPool_running) {
        cv.wait(lock);
        if (!threadPool_running) {
            // return empty task if thread pool was shut down
            NES_DEBUG("QueryManager: Thread pool was shut down while waiting");
            return Task();
        }
    }
    NES_DEBUG("QueryManager::getWork queue is not empty");
    // there is a potential task in the queue and the thread pool is running
    if (threadPool_running) {
        auto task = taskQueue.front();
        NES_TRACE("QueryManager: provide task" << task << " to thread (getWork())");
        taskQueue.pop_front();
        return task;
    } else {
        NES_DEBUG("QueryManager: Thread pool was shut down while waiting");
        cleanupUnsafe();
        return Task();
    }
}

void QueryManager::cleanupUnsafe() {
    // Call this only if you are holding workMutex
    if (taskQueue.size()) {
        NES_DEBUG("QueryManager::cleanupUnsafe: Thread pool was shut down while waiting but data is queued.");
        taskQueue.clear();
    }
    NES_DEBUG("QueryManager::cleanupUnsafe: finished");
}

void QueryManager::cleanup() {
    NES_DEBUG("QueryManager::cleanup: get lock");
    std::unique_lock<std::mutex> lock(workMutex);
    NES_DEBUG("QueryManager::cleanup: got lock");
    cleanupUnsafe();
    NES_DEBUG("QueryManager::cleanup: finished");
}

void QueryManager::addWorkForNextPipeline(TupleBuffer& buffer, PipelineStagePtr nextPipeline) {
    std::unique_lock lock(workMutex);

    // dispatch buffer as task
    taskQueue.emplace_back(std::move(nextPipeline), buffer);
    NES_TRACE("QueryManager: added Task " << taskQueue.back() << " for nextPipeline " << nextPipeline << " inputBuffer " << buffer);

    cv.notify_all();
}

void QueryManager::addWork(const std::string& sourceId, TupleBuffer& buf) {
    std::shared_lock queryLock(queryMutex);// we need this lock because sourceIdToQueryMap can be concurrently modified
    std::unique_lock workQueueLock(workMutex);
    for (const auto& qep : sourceIdToQueryMap[sourceId]) {
        // for each respective source, create new task and put it into queue
        // TODO: change that in the future that stageId is used properly
        taskQueue.emplace_back(qep->getStage(0), buf);
        NES_DEBUG("QueryManager: added Task " << taskQueue.back() << " for query " << sourceId << " for QEP " << qep
                                              << " inputBuffer " << buf);
    }
    cv.notify_all();
}

void QueryManager::completedWork(Task& task, WorkerContext&) {
    NES_INFO("QueryManager::completedWork: Work for task=" << task);
    std::unique_lock lock(statisticsMutex);
    auto statistics = queryToStatisticsMap.find(task.getPipelineStage()->getQepParentId());

    statistics->incProcessedTasks();
    if (task.isWatermarkOnly()) {
        statistics->incProcessedWatermarks();
    } else {
        statistics->incProcessedBuffers();
    }
    statistics->incProcessedTuple(task.getNumberOfTuples());
}

std::string QueryManager::getQueryManagerStatistics() {
    std::unique_lock lock(statisticsMutex);
    std::stringstream ss;
    ss << "QueryManager Statistics:";
    for (auto& qep : runningQEPs) {
        auto stats = queryToStatisticsMap.find(qep->getQueryId());
        ss << "Query=" << qep;
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

QueryStatisticsPtr QueryManager::getQueryStatistics(QueryExecutionPlanId qepId) {
    std::unique_lock lock(statisticsMutex);
    NES_DEBUG("QueryManager::getQueryStatistics: for qep=" << qepId);
    return queryToStatisticsMap.find(qepId);
}

}// namespace NES
