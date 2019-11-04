/*
 * General considerations:
 *  - do we really need all mutexex=
 */
#include <NodeEngine/Dispatcher.hpp>
#include <Runtime/Window.hpp>
#include <Util/Logger.hpp>
#include <assert.h>
#include <iostream>
namespace iotdb {

Dispatcher::Dispatcher()
    : task_queue(),
      source_to_query_map(),
      window_to_query_map(),
      sink_to_query_map(),
      bufferMutex(),
      queryMutex(),
      workMutex(),
      workerHitEmptyTaskQueue(0),
      processedTasks(0),
      processedTuple(0),
      startTime(0) {
  IOTDB_DEBUG("Init Dispatcher")
}

Dispatcher::~Dispatcher() {
  resetDispatcher();
}

void Dispatcher::resetDispatcher() {
  IOTDB_DEBUG("Dispatcher: Destroy Task Queue")
  task_queue.clear();
  IOTDB_DEBUG("Dispatcher: Destroy source_to_query_map")
  source_to_query_map.clear();
  IOTDB_DEBUG("Dispatcher: Destroy sink_to_query_map")
  sink_to_query_map.clear();
  IOTDB_DEBUG("Dispatcher: Destroy window_to_query_map")
  window_to_query_map.clear();
}

bool Dispatcher::registerQuery(const QueryExecutionPlanPtr qep) {
  std::unique_lock<std::mutex> lock(queryMutex);
  //TODO: do we have to test/enforce here that each QEP has at least one sink and source?

  /**
   * test if elements already exist
   */
  auto sources = qep->getSources();
  for (auto source : sources) {
    IOTDB_DEBUG("Dispatcher: search for source" << source)
    if (std::find(source_to_query_map[source.get()].begin(),
                  source_to_query_map[source.get()].end(), qep)
        != source_to_query_map[source.get()].end()) {
      IOTDB_ERROR("Dispatcher: source/qep already exists" << source)
      return false;
    }
  }
  auto windows = qep->getWindows();
  for (auto window : windows) {
    IOTDB_DEBUG("Dispatcher: search for window" << window)
    if (std::find(window_to_query_map[window.get()].begin(),
                  window_to_query_map[window.get()].end(), qep)
        != window_to_query_map[window.get()].end()) {
      IOTDB_ERROR("Dispatcher: window/qep already exists" << window)
      return false;
    }
  }

  auto sinks = qep->getSinks();
  for (auto sink : sinks) {
    IOTDB_DEBUG("Dispatcher: search for sink" << sink)
    if (std::find(sink_to_query_map[sink.get()].begin(),
                  sink_to_query_map[sink.get()].end(), qep)
        != sink_to_query_map[sink.get()].end()) {
      IOTDB_ERROR("Dispatcher: sink/qep already exists" << sink)
      return false;
    }
  }

  /**
   * add elements
   */
  auto sources = qep->getSources();
  for (auto source : sources) {
    IOTDB_DEBUG("Dispatcher: add source" << source)
    source_to_query_map[source.get()].emplace_back(qep);
    source->start();
  }

  auto windows = qep->getWindows();
  for (auto window : windows) {
    IOTDB_DEBUG("Dispatcher: add window" << window)
    window_to_query_map[window.get()].emplace_back(qep);
    window->setup();
  }

  auto sinks = qep->getSinks();
  for (auto sink : sinks) {
    IOTDB_DEBUG("Dispatcher: add sink" << sink)
    sink_to_query_map[sink.get()].emplace_back(qep);
    sink->setup();
  }
}

void Dispatcher::deregisterQuery(const QueryExecutionPlanPtr qep) {
  std::unique_lock<std::mutex> lock(queryMutex);

  auto sources = qep->getSources();
  for (auto source : sources) {
    IOTDB_DEBUG("Dispatcher: try remove source" << source)
    if (source_to_query_map[source.get()].size() > 1) {
      //if more than one qep for the source, delete only qep
      IOTDB_DEBUG("Dispatcher: remove only qep" << qep)
      source_to_query_map[source.get()].erase(
          std::find(source_to_query_map[source.get()].begin(),
                    source_to_query_map[source.get()].end(), qep));
    } else  //if last qep for source, delete source
    {
      IOTDB_DEBUG("Dispatcher: stop source" << qep)
      source->stop();

      IOTDB_DEBUG("Dispatcher: delete source" << qep)
      source_to_query_map[source.get()].clear();
      source_to_query_map.erase(source.get());
    }
  }

  auto windows = qep->getWindows();
  for (auto window : windows) {
    IOTDB_DEBUG("Dispatcher: try remove window" << window)
    if (window_to_query_map[window.get()].size() > 1) {
      //if more than one qep for the window, delete only qep
      IOTDB_DEBUG("Dispatcher: remove only qep" << qep)
      window_to_query_map[window.get()].erase(
          std::find(window_to_query_map[window.get()].begin(),
                    window_to_query_map[window.get()].end(), qep));
    } else  //if last qep for window, delete window
    {
      IOTDB_DEBUG("Dispatcher: stop window" << window)
      window->shutdown();

      IOTDB_DEBUG("Dispatcher: delete window" << window)
      window_to_query_map[window.get()].clear();
      window_to_query_map.erase(window.get());
    }
  }

  auto sinks = qep->getSinks();
  for (auto sink : sinks) {
    IOTDB_DEBUG("Dispatcher: try remove sink" << sink)
    if (sink_to_query_map[sink.get()].size() > 1) {
      //if more than one qep for the window, delete only qep
      IOTDB_DEBUG("Dispatcher: remove only qep" << qep)
      sink_to_query_map[sink.get()].erase(
          std::find(sink_to_query_map[sink.get()].begin(),
                    sink_to_query_map[sink.get()].end(), qep));
    } else  //if last qep for window, delete window
    {
      IOTDB_DEBUG("Dispatcher: stop sink" << sink)
      sink->shutdown();

      IOTDB_DEBUG("Dispatcher: delete sink" << sink)
      sink_to_query_map[sink.get()].clear();
      sink_to_query_map.erase(sink.get());
    }
  }
}

TaskPtr Dispatcher::getWork(bool& threadPool_running) {
  std::unique_lock<std::mutex> lock(workMutex);

  //wait while queue is empty but thread pool is running
  while (task_queue.empty() && threadPool_running) {
    workerHitEmptyTaskQueue++;
    cv.wait(lock);
    if (!threadPool_running) {
      //return empty task if thread pool was shut down
      IOTDB_DEBUG("Dispatcher: Thread pool was shut down while waiting")
      return TaskPtr();
    }
  }

  //there is a potential task in the queue and the thread pool is running
  TaskPtr task;
  if (threadPool_running) {
    task = task_queue.front();
    IOTDB_DEBUG(
        "Dispatcher: provide task" << task.get() << " to thread (getWork())")
    task_queue.erase(task_queue.begin());
  } else {
    IOTDB_DEBUG("Dispatcher: Thread pool was shut down while waiting")
    task = TaskPtr();
  }
  return task;
}

void Dispatcher::addWork(const TupleBufferPtr buf, DataSource* source) {
  std::unique_lock<std::mutex> lock(workMutex);

  //get the queries that fetches data from this source
  std::vector<QueryExecutionPlanPtr>& queries = source_to_query_map[source];
  for (uint64_t i = 0; i < queries.size(); ++i) {
    // for each respective source, create new task and put it into queue
    TaskPtr task(
        new Task(queries[i], queries[i]->stageIdFromSource(source), source,
                 buf));
    task_queue.push_back(task);
    IOTDB_DEBUG(
        "Dispatcher: added Task " << task.get() << " for source " << source << " for QEP " << queries[i].get() << " inputBuffer " << buf)
  }
  cv.notify_all();
}

void Dispatcher::completedWork(TaskPtr task) {
  std::unique_lock<std::mutex> lock(workMutex);

  processedTasks++;
  processedTuple += task->getNumberOfTuples();

  task->releaseInputBuffer();
}

Dispatcher& Dispatcher::instance() {
  static Dispatcher instance;
  return instance;
}

void Dispatcher::printGeneralStatistics() {
  IOTDB_INFO("Dispatcher Statistics:")
  IOTDB_INFO("\t workerHitEmptyTaskQueue =" << workerHitEmptyTaskQueue)
  IOTDB_INFO("\t processedTasks =" << processedTasks)
  IOTDB_INFO("\t processedTuple =" << processedTuple)

  BufferManager::instance().printStatistics();

}

void Dispatcher::printQEPStatistics(const QueryExecutionPlanPtr qep) {

  IOTDB_INFO("Source Statistics:")
  auto sources = qep->getSources();
  for (auto source : sources) {
    IOTDB_INFO("Source:" << source)
    IOTDB_INFO("\t Generated Buffers=" << source->getNumberOfGeneratedBuffers())
    IOTDB_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples())
  }
  auto windows = qep->getWindows();
  for (auto window : windows) {
    IOTDB_INFO("Window:" << window)
    IOTDB_INFO("\t NumberOfEntries=" << window->getNumberOfEntries())
    IOTDB_INFO("Window Result:")
    window->print();
  }
  auto sinks = qep->getSinks();
  for (auto sink : sinks) {
    IOTDB_INFO("Sink:" << sink)
    IOTDB_INFO("\t Generated Buffers=" << sink->getNumberOfProcessedBuffers())
    IOTDB_INFO("\t Generated Tuples=" << sink->getNumberOfProcessedTuples())
  }
}

}  // namespace iotdb
