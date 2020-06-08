#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <Windows/WindowHandler.hpp>
#include <iostream>
#include <memory>

namespace NES {
using std::string;

QueryManager::QueryManager()
    : taskQueue(), sourceIdToQueryMap(), bufferMutex(), queryMutex(), workMutex() {
    NES_DEBUG("Init QueryManager::QueryManager()");
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
    std::scoped_lock locks(queryMutex, workMutex);
    NES_DEBUG("QueryManager: Destroy Task Queue " << taskQueue.size());
    taskQueue.clear();
    NES_DEBUG("QueryManager: Destroy queryId_to_query_map " << sourceIdToQueryMap.size());

    sourceIdToQueryMap.clear();
    queryToStatisticsMap.clear();

    NES_DEBUG("QueryManager::resetQueryManager finished");
}

bool QueryManager::registerQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("QueryManager::registerQueryInNodeEngine: query" << qep);
    std::unique_lock<std::mutex> lock(queryMutex);

    // test if elements already exist
    NES_DEBUG("QueryManager: resolving sources for query " << qep);
    for (const auto& source : qep->getSources()) {
        source->setQueryManager(shared_from_this());
        source->setBufferManger(qep->getBufferManager());
        if (sourceIdToQueryMap.find(source->getSourceId()) != sourceIdToQueryMap.end()) {
            // source already exists, add qep to source set if not there
            if (sourceIdToQueryMap[source->getSourceId()].find(qep) == sourceIdToQueryMap[source->getSourceId()].end()) {
                // qep not found in list, add it
                NES_DEBUG("QueryManager: Inserting QEP " << qep << " to Source" << source->getSourceId());
                sourceIdToQueryMap[source->getSourceId()].insert(qep);
                QueryStatisticsPtr stats = std::make_shared<QueryStatistics>();
                queryToStatisticsMap.insert({qep, stats});
            } else {
                NES_DEBUG("QueryManager: Source " << source->getSourceId() << " and QEP already exist.");
                return false;
            }
        } else {
            // source does not exist, add source and unordered_set containing the qep
            NES_DEBUG("QueryManager: Source " << source->getSourceId()
                                              << " not found. Creating new element with with qep " << qep);
            std::unordered_set<QueryExecutionPlanPtr> qepSet = {qep};
            sourceIdToQueryMap.insert({source->getSourceId(), qepSet});
            QueryStatisticsPtr stats = std::make_shared<QueryStatistics>();
            queryToStatisticsMap.insert({qep, stats});
        }
    }
    return true;
}

bool QueryManager::startQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("QueryManager::startQuery: query" << qep);
    std::unique_lock<std::mutex> lock(queryMutex);

    // start elements
    auto sources = qep->getSources();
    for (const auto& source : sources) {
        NES_DEBUG("QueryManager: start source " << source << " str=" << source->toString());
        if (!source->start()) {
            NES_WARNING("QueryManager: source " << source << " could not started as it is already running");
        } else {
            NES_DEBUG("QueryManager: source " << source << " started successfully");
        }
    }

    auto sinks = qep->getSinks();
    for (const auto& sink : sinks) {
        NES_DEBUG("QueryManager: start sink " << sink);
        sink->setup();
    }

    if (!qep->setup() || !qep->start()) {
        NES_FATAL_ERROR("QueryManager: query execution plan could not started");
        return false;
    }
    return true;
}

bool QueryManager::deregisterQuery(QueryExecutionPlanPtr qep) {
    NES_DEBUG("QueryManager::deregisterAndUndeployQuery: query" << qep);

    std::unique_lock<std::mutex> lock(queryMutex);
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
    std::unique_lock<std::mutex> lock(queryMutex);

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
    NES_DEBUG("QueryManager::stopQuery: query finished");
    return true;
}

TaskPtr QueryManager::getWork(std::atomic<bool>& threadPool_running) {
    NES_DEBUG("QueryManager: QueryManager::getWork wait get lock");
    std::unique_lock<std::mutex> lock(workMutex);
    NES_DEBUG("QueryManager:getWork wait got lock");
    // wait while queue is empty but thread pool is running
    while (taskQueue.empty() && threadPool_running) {
        //NES_DEBUG("QueryManager::getWork wait for work as queue is emtpy");
        cv.wait(lock);
        if (!threadPool_running) {
            // return empty task if thread pool was shut down
            NES_DEBUG("QueryManager: Thread pool was shut down while waiting");
            return TaskPtr();
        }
    }
    NES_DEBUG("QueryManager::getWork queue is not empty");
    // there is a potential task in the queue and the thread pool is running
    TaskPtr task;
    if (threadPool_running) {
        task = taskQueue.front();
        NES_DEBUG("QueryManager: provide task" << task.get() << " to thread (getWork())");
        taskQueue.pop_front();
    } else {
        NES_DEBUG("QueryManager: Thread pool was shut down while waiting");
        cleanupUnsafe();
        task = TaskPtr();
    }
    NES_DEBUG("QueryManager:getWork return task");
    return task;
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

void QueryManager::addWorkForNextPipeline(TupleBuffer& buffer, QueryExecutionPlanPtr queryExecutionPlan,
                                          uint32_t pipelineId) {
    std::unique_lock<std::mutex> lock(workMutex);

    // dispatch buffer as task
    TaskPtr task = std::make_shared<Task>(queryExecutionPlan, pipelineId + 1, buffer);
    taskQueue.push_back(task);
    NES_DEBUG("QueryManager: added Task " << task.get() << " for QEP " << queryExecutionPlan << " inputBuffer " << buffer);

    cv.notify_all();
}

void QueryManager::addWork(const string& sourceId, TupleBuffer& buf) {
    std::unique_lock<std::mutex> lock(workMutex);
    for (const auto& qep : sourceIdToQueryMap[sourceId]) {
        // for each respective source, create new task and put it into queue
        // TODO: change that in the future that stageId is used properly
        TaskPtr task = std::make_shared<Task>(qep, 0, buf);
        taskQueue.push_back(task);
        NES_DEBUG("QueryManager: added Task " << task.get() << " for query " << sourceId << " for QEP " << qep
                                              << " inputBuffer " << buf);
    }
    cv.notify_all();
}

void QueryManager::completedWork(TaskPtr task) {
    std::unique_lock<std::mutex> lock(workMutex);// TODO is necessary?
    NES_INFO("Complete Work for task" << task);

    auto statistics = queryToStatisticsMap[task->getQep()];
    statistics->incProcessedTasks();
    statistics->incProcessedBuffers();
    statistics->incProcessedTuple(task->getNumberOfTuples());

    task.reset();
}

std::string QueryManager::getQueryManagerStatistics() {
    std::stringstream ss;
    ss << "QueryManager Statistics:";
    for (auto& qep : queryToStatisticsMap) {
        ss << "Query=" << qep.first;
        ss << "\t processedTasks =" << qep.second->getProcessedTasks();
        ss << "\t processedTuple =" << qep.second->getProcessedTuple();
        ss << "\t processedBuffers =" << qep.second->getProcessedBuffers();

        ss << "Source Statistics:";
        auto sources = qep.first->getSources();
        for (auto source : sources) {
            ss << "Source:" << source;
            ss << "\t Generated Buffers=" << source->getNumberOfGeneratedBuffers();
            ss << "\t Generated Tuples=" << source->getNumberOfGeneratedTuples();
        }
        auto sinks = qep.first->getSinks();
        for (auto sink : sinks) {
            ss << "Sink:" << sink;
            ss << "\t Generated Buffers=" << sink->getNumberOfSentBuffers();
            ss << "\t Generated Tuples=" << sink->getNumberOfSentTuples();
        }
    }
    return ss.str();
}

QueryStatisticsPtr QueryManager::getQueryStatistics(QueryExecutionPlanPtr qep) {
    NES_DEBUG("QueryManager::getQueryStatistics: for qep=" << qep);
    return queryToStatisticsMap[qep];
}

}// namespace NES
