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
      queryId_to_query_map(),
      bufferMutex(),
      queryMutex(),
      workMutex(),
      workerHitEmptyTaskQueue(0),
      processedTasks(0),
      processedTuple(0),
      processedBuffers(0){
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
  queryId_to_query_map.clear();

  workerHitEmptyTaskQueue = 0;
  processedTasks = 0;
  processedTuple = 0;
  processedBuffers = 0;
}

bool Dispatcher::registerQueryWithStart(QueryExecutionPlanPtr qep) {
  if (registerQueryWithoutStart(qep)) {

    std::unique_lock<std::mutex> lock(queryMutex);
    /**
     * start elements
     */
    auto sources = qep->getSources();
    for (const auto &source : sources) {
      NES_DEBUG("Dispatcher: start source " << source)
      source->start();
    }

    auto windows = qep->getWindows();
    for (const auto &window : windows) {
      NES_DEBUG("Dispatcher: start window " << window)
      window->setup();
      window->start();
    }

    auto sinks = qep->getSinks();
    for (const auto &sink : sinks) {
      NES_DEBUG("Dispatcher: start sink " << sink)
      sink->setup();
    }
    return true;
  }
  else {
    return false;
  }
}

bool Dispatcher::registerQueryWithoutStart(QueryExecutionPlanPtr qep) {
  std::unique_lock<std::mutex> lock(queryMutex);

  /**
   * test if elements already exist
   */
  NES_DEBUG("Dispatcher: search for query " << qep->getQueryId())

  if (this->queryId_to_query_map.find(qep->getQueryId()) != this->queryId_to_query_map.end()) {
    NES_ERROR("Dispatcher: qep already exists " << qep->getQueryId())
    return false;
  }
  else {
    NES_DEBUG("Dispatcher: adding query " << qep->getQueryId())
    this->queryId_to_query_map.insert({qep->getQueryId(), qep});

    auto windows = qep->getWindows();
    for (const auto& window : windows) {
      NES_DEBUG("Dispatcher: add window " << window)
      window_to_query_map.insert({window.get(), qep});
    }

    return true;
  }
}

void Dispatcher::deregisterQuery(const string& queryId) {
  std::unique_lock<std::mutex> lock(queryMutex);

  if (this->queryId_to_query_map.find(queryId) != this->queryId_to_query_map.end()) {
    NES_ERROR("Dispatcher: qep does not exist for query " << queryId)
  }
  else {
    QueryExecutionPlanPtr qep = queryId_to_query_map.at(queryId);

    auto sources = qep->getSources();
    for (const auto& source : sources) {
      NES_DEBUG("Dispatcher: stop source " << source)
      source->stop();
    }

    auto windows = qep->getWindows();
    for (const auto& window : windows) {
        NES_DEBUG("Dispatcher: stop window " << window)
        window->stop();
        window_to_query_map.erase(window.get());
    }

    auto sinks = qep->getSinks();
    for (const auto& sink : sinks) {
        NES_DEBUG("Dispatcher: stop sink " << sink)
        sink->shutdown();
    }

    queryId_to_query_map.erase(queryId);
  }
}

TaskPtr Dispatcher::getWork(bool &threadPool_running) {
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
    NES_DEBUG("Dispatcher: Thread pool was shut down while waiting")
    task = TaskPtr();
  }
  return task;
}

void Dispatcher::addWork(const NES::TupleBufferPtr window_aggregates, NES::WindowHandler *window) {
  std::unique_lock<std::mutex> lock(workMutex);

  //get the queries that contains this window
  QueryExecutionPlanPtr qep = window_to_query_map[window];

  // TODO We currently assume that a window ends a query execution plan
  // for each respective sink, create output the window aggregates
  auto sinks = qep->getSinks();
  for (auto &sink : sinks) {
    sink->writeData(window_aggregates);
    NES_DEBUG(
        "Dispatcher: write window aggregates to sink " << sink << " for QEP " << qep << " tupleBuffer "
                                                       << window_aggregates)
  }
  cv.notify_all();
}

void Dispatcher::addWork(const string& queryId, const TupleBufferPtr& buf) {
  std::unique_lock<std::mutex> lock(workMutex);

  //get the queries that fetches data from this source
  QueryExecutionPlanPtr qep = queryId_to_query_map[queryId];

  //set the useCount of the buffer
  buf->setUseCnt(1);

  auto sources = qep->getSources();
  for (const auto& source : sources) {
    // for each respective source, create new task and put it into queue
    //TODO: is this handeled right? how do we get the stateID here?
    //TODO: change that in the future that stageId is used properly
    TaskPtr task(new Task(qep, qep->stageIdFromSource(source.get()), buf));
    task_queue.push_back(task);
    NES_DEBUG(
        "Dispatcher: added Task " << task.get() << " for query " << queryId << " for QEP " << qep
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

Dispatcher &Dispatcher::instance() {
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
  auto windows = qep->getWindows();
  for (auto window : windows) {
    NES_INFO("WindowHandler:" << window)
    NES_INFO("WindowHandler Result:")
  }
  auto sinks = qep->getSinks();
  for (auto sink : sinks) {
    NES_INFO("Sink:" << sink)
    NES_INFO("\t Generated Buffers=" << sink->getNumberOfSentBuffers())
    NES_INFO("\t Generated Tuples=" << sink->getNumberOfSentTuples())
  }
}

}  // namespace NES
