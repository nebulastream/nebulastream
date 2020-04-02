/*
 * General considerations:
 *  - do we really need all mutex?
 */
#include <NodeEngine/Dispatcher.hpp>
#include <Util/Logger.hpp>
#include <iostream>

#include <Windows/WindowHandler.hpp>

namespace NES {
using std::string;

Dispatcher::Dispatcher()
    : task_queue(),
      sourceIdToQueryMap(),
      bufferMutex(),
      queryMutex(),
      workMutex(),
      workerHitEmptyTaskQueue(0),
      processedTasks(0),
      processedTuple(0),
      processedBuffers(0) {
    NES_DEBUG("Init Dispatcher")
}

Dispatcher::~Dispatcher() {
    NES_DEBUG("Reset Dispatcher")
    resetDispatcher();
}

void Dispatcher::resetDispatcher() {
    NES_DEBUG("Dispatcher: Destroy Task Queue")
    task_queue.clear();
    NES_DEBUG("Dispatcher: Destroy queryId_to_query_map")
    sourceIdToQueryMap.clear();

    workerHitEmptyTaskQueue = 0;
    processedTasks = 0;
    processedTuple = 0;
    processedBuffers = 0;
}

bool Dispatcher::registerQueryWithStart(QueryExecutionPlanPtr qep) {
    if (registerQueryWithoutStart(qep)) {
        std::unique_lock<std::mutex> lock(queryMutex);
        //start elements
        auto sources = qep->getSources();
        for (const auto& source : sources) {
            NES_DEBUG("Dispatcher: start source " << source << " str=" << source->toString())
            if (!source->start()) {
                NES_FATAL_ERROR("Dispatcher: source " << source << " could not started");
                return false;
            }
        }

        auto sinks = qep->getSinks();
        for (const auto& sink : sinks) {
            NES_DEBUG("Dispatcher: start sink " << sink)
            sink->setup();
        }

        if (!qep->setup() || !qep->start()) {
            NES_FATAL_ERROR("Dispatcher: query execution plan could not started");
            return false;
        }
        return true;
    } else {
        //registration failed, return false
        return false;
    }
}

bool Dispatcher::registerQueryWithoutStart(QueryExecutionPlanPtr qep) {
    std::unique_lock<std::mutex> lock(queryMutex);

    //test if elements already exist
    NES_DEBUG("Dispatcher: resolving sources for query " << qep)
    for (const auto& source: qep->getSources()) {
        if (sourceIdToQueryMap.find(source->getSourceId()) != sourceIdToQueryMap.end()) {
            //source already exists, add qep to source set if not there
            if (sourceIdToQueryMap[source->getSourceId()].find(qep)
                == sourceIdToQueryMap[source->getSourceId()].end()) {
                //qep not found in list, add it
                NES_DEBUG("Dispatcher: Inserting QEP " << qep << " to Source" << source->getSourceId())
                sourceIdToQueryMap[source->getSourceId()].insert(qep);
            } else {
                NES_DEBUG("Dispatcher: Source " << source->getSourceId() << " and QEP already exist.")
                return false;
            }
        } else {
            // source does not exist, add source and unordered_set containing the qep
            NES_DEBUG(
                "Dispatcher: Source " << source->getSourceId() << " not found. Creating new element with with qep "
                                      << qep)
            std::unordered_set<QueryExecutionPlanPtr> qepSet = {qep};
            sourceIdToQueryMap.insert({source->getSourceId(), qepSet});
        }
    }
    return true;
}

bool Dispatcher::deregisterQuery(QueryExecutionPlanPtr qep) {
    std::unique_lock<std::mutex> lock(queryMutex);
    bool succeed = true;

    auto sources = qep->getSources();
    for (const auto& source : sources) {
        NES_DEBUG("Dispatcher: stop source " << source)
        source->stop();
    }

    if (!qep->stop()) {
        NES_FATAL_ERROR(
            "Dispatcher: QEP could not be stopped");
        return false;
    };

    auto sinks = qep->getSinks();
    for (const auto& sink : sinks) {
        NES_DEBUG("Dispatcher: stop sink " << sink)
        sink->shutdown();
    }

    for (const auto& source : sources) {
        NES_DEBUG("Dispatcher: stop source " << source)
        if (sourceIdToQueryMap.find(source->getSourceId()) != sourceIdToQueryMap.end()) {
            //source exists, remove qep from source if there
            if (sourceIdToQueryMap[source->getSourceId()].find(qep)
                != sourceIdToQueryMap[source->getSourceId()].end()) {
                //qep found, remove it
                NES_DEBUG("Dispatcher: Removing QEP " << qep << " from Source" << source->getSourceId())
                if (sourceIdToQueryMap[source->getSourceId()].erase(qep) == 0) {
                    NES_FATAL_ERROR(
                        "Dispatcher: Removing QEP " << qep << " for source " << source->getSourceId() << " failed!")
                    succeed = false;
                }

                // if source has no qeps remove the source from map
                if (sourceIdToQueryMap[source->getSourceId()].empty()) {
                    if (sourceIdToQueryMap.erase(source->getSourceId()) == 0) {
                        NES_FATAL_ERROR("Dispatcher: Removing source " << source->getSourceId() << " failed!")
                        succeed = false;
                    }
                }
            }
        }
    }
    return succeed;
}

TaskPtr Dispatcher::getWork(std::atomic<bool>& threadPool_running) {
    std::unique_lock<std::mutex> lock(workMutex);

    //wait while queue is empty but thread pool is running
    while (task_queue.empty() && threadPool_running) {
        workerHitEmptyTaskQueue++;
        cv.wait(lock);
        if (!threadPool_running) {
            //return empty task if thread pool was shut down
            NES_DEBUG("Dispatcher: Thread pool was shut down while waiting")
            return TaskPtr();
        }
    }

    //there is a potential task in the queue and the thread pool is running
    TaskPtr task;
    if (threadPool_running) {
        task = task_queue.front();
        NES_DEBUG(
            "Dispatcher: provide task" << task.get() << " to thread (getWork())")
        task_queue.erase(task_queue.begin());
    } else {
        NES_DEBUG("Dispatcher: Thread pool was shut down while waiting");
        cleanup();
        task = TaskPtr();
    }
    return task;
}

void Dispatcher::cleanup() {
    if (task_queue.size()) {
        NES_DEBUG("Dispatcher: Thread pool was shut down while waiting but data is queued.");
        while (task_queue.size()) {
            auto task = task_queue.front();
            task->releaseInputBuffer();
            task_queue.erase(task_queue.begin());
        }
    }
}

void Dispatcher::addWorkForNextPipeline(const TupleBufferPtr buffer,
                                        QueryExecutionPlanPtr queryExecutionPlan,
                                        uint32_t pipelineId) {
    std::unique_lock<std::mutex> lock(workMutex);

    //dispatch buffer as task
    buffer->setUseCnt(1);
    TaskPtr task(new Task(queryExecutionPlan, pipelineId + 1, buffer));
    task_queue.push_back(task);
    NES_DEBUG(
        "Dispatcher: added Task " << task.get() << " for QEP " << queryExecutionPlan
                                  << " inputBuffer " << buffer)

    cv.notify_all();
}

void Dispatcher::addWork(const string& sourceId, const TupleBufferPtr& buf) {
    std::unique_lock<std::mutex> lock(workMutex);

    //set the useCount of the buffer
    buf->setUseCnt(sourceIdToQueryMap[sourceId].size());

    for (const auto& qep : sourceIdToQueryMap[sourceId]) {
        // for each respective source, create new task and put it into queue
        //TODO: change that in the future that stageId is used properly
        TaskPtr task(new Task(qep, 0, buf));
        task_queue.push_back(task);
        NES_DEBUG(
            "Dispatcher: added Task " << task.get() << " for query " << sourceId << " for QEP " << qep
                                      << " inputBuffer " << buf)
    }
    cv.notify_all();
}

void Dispatcher::completedWork(TaskPtr task) {
    std::unique_lock<std::mutex> lock(workMutex);
    NES_INFO("Complete Work for task" << task)

    processedTasks++;
    processedBuffers++;
    processedTuple += task->getNumberOfTuples();

    task->releaseInputBuffer();
}

Dispatcher& Dispatcher::instance() {
    static Dispatcher instance;
    return instance;
}

void Dispatcher::printGeneralStatistics() {
    NES_INFO("Dispatcher Statistics:")
    NES_INFO("\t workerHitEmptyTaskQueue =" << workerHitEmptyTaskQueue)
    NES_INFO("\t processedTasks =" << processedTasks)
    NES_INFO("\t processedTuple =" << processedTuple)

    BufferManager::instance().printStatistics();

}

void Dispatcher::printQEPStatistics(const QueryExecutionPlanPtr qep) {

    NES_INFO("Source Statistics:")
    auto sources = qep->getSources();
    for (auto source : sources) {
        NES_INFO("Source:" << source)
        NES_INFO("\t Generated Buffers=" << source->getNumberOfGeneratedBuffers())
        NES_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples())
    }
    auto sinks = qep->getSinks();
    for (auto sink : sinks) {
        NES_INFO("Sink:" << sink)
        NES_INFO("\t Generated Buffers=" << sink->getNumberOfSentBuffers())
        NES_INFO("\t Generated Tuples=" << sink->getNumberOfSentTuples())
    }
}

}  // namespace NES
