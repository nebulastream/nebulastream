#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <Util/Logger.hpp>
#include <Windows/WindowHandler.hpp>
#include <iostream>
#include <memory>
#include <utility>

namespace NES {
using std::string;
namespace detail {
uint32_t reconfigurationTaskEntryPoint(TupleBuffer& buffer, void*, WindowManager*, PipelineExecutionContext&, WorkerContextRef workerContext) {
    NES_DEBUG("QueryManager: QueryManager::addReconfigurationTask reconfigurationTaskEntryPoint");
    auto* descriptor = buffer.getBufferAs<ReconfigurationDescriptor>();
    switch (descriptor->getType()) {
        case Initialize: {
            descriptor->getInstance()->reconfigure(workerContext);
            break;
        }
        default: NES_THROW_RUNTIME_ERROR("unsupported switch condition");
    }
    return 0;
}
}

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

bool QueryManager::addReconfigurationTask(QuerySubPlanId queryExecutionPlanId, ReconfigurationDescriptor descriptor) {
    NES_DEBUG("QueryManager: QueryManager::addReconfigurationTask begin");
    std::unique_lock<std::mutex> lock(workMutex);
    auto optBuffer = bufferManager->getUnpooledBuffer(sizeof(ReconfigurationDescriptor));
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();
    new (buffer.getBuffer()) ReconfigurationDescriptor(descriptor); // memcpy using copy ctor
    auto pipelineContext = std::make_shared<PipelineExecutionContext>(queryExecutionPlanId, bufferManager, [](TupleBuffer&, NES::WorkerContext&){});
    auto pipeline = PipelineStage::create(-1, queryExecutionPlanId, reconfigurationExecutable, pipelineContext, nullptr);
    for (auto i = 0; i < threadPool->getNumberOfThreads(); ++i) {
        taskQueue.emplace_back(pipeline, buffer);
    }
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
        NES_TRACE("QueryManager: provide task" << task.toString() << " to thread (getWork())");
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
        auto stats = queryToStatisticsMap.find(qep->getQuerySubPlanId());
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

QueryStatisticsPtr QueryManager::getQueryStatistics(QuerySubPlanId qepId) {
    std::unique_lock lock(statisticsMutex);
    NES_DEBUG("QueryManager::getQueryStatistics: for qep=" << qepId);
    return queryToStatisticsMap.find(qepId);
}

}// namespace NES
