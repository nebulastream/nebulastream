/*
 * General considerations:
 *  - do we really need all mutex?
 */
#include <NodeEngine/Dispatcher.hpp>
#include <Util/Logger.hpp>
#include <assert.h>
#include <iostream>

#include <Windows/WindowHandler.hpp>

namespace NES {

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
  NES_DEBUG("Dispatcher: Destroy source_to_query_map")
  source_to_query_map.clear();
  NES_DEBUG("Dispatcher: Destroy sink_to_query_map")
  sink_to_query_map.clear();
  NES_DEBUG("Dispatcher: Destroy window_to_query_map")
  window_to_query_map.clear();

  workerHitEmptyTaskQueue = 0;
  processedTasks = 0;
  processedTuple = 0;
  processedBuffers = 0;
}

bool Dispatcher::registerQueryWithStart(const QueryExecutionPlanPtr qep) {
  std::unique_lock<std::mutex> lock(queryMutex);
//TODO: do we have to test/enforce here that each QEP has at least one sink and source?

  /**
   * test if elements already exist
   */
  auto sources = qep->getSources();
  for (auto source : sources) {
    NES_DEBUG("Dispatcher: search for source" << source)
    if (std::find(source_to_query_map[source.get()].begin(),
                  source_to_query_map[source.get()].end(), qep)
        != source_to_query_map[source.get()].end()) {
      NES_ERROR("Dispatcher: source/qep already exists" << source)
      return false;
    }
  }
  auto windows = qep->getWindows();
  for (auto window : windows) {
    NES_DEBUG("Dispatcher: search for window" << window)
    if (std::find(window_to_query_map[window.get()].begin(),
                  window_to_query_map[window.get()].end(), qep)
        != window_to_query_map[window.get()].end()) {
      NES_ERROR("Dispatcher: window/qep already exists" << window)
      return false;
    }
  }

  auto sinks = qep->getSinks();
  for (auto sink : sinks) {
    NES_DEBUG("Dispatcher: search for sink" << sink)
    if (std::find(sink_to_query_map[sink.get()].begin(),
                  sink_to_query_map[sink.get()].end(), qep)
        != sink_to_query_map[sink.get()].end()) {
      NES_ERROR("Dispatcher: sink/qep already exists" << sink)
      return false;
    }
  }

  /**
   * add elements
   */
  for (auto source : sources) {
    NES_DEBUG("Dispatcher: add source" << source)
    source_to_query_map[source.get()].emplace_back(qep);
    source->start();
  }

  for (auto window : windows) {
    NES_DEBUG("Dispatcher: add window" << window)
    window_to_query_map[window.get()].emplace_back(qep);
    window->setup();
    window->start();
  }

  for (auto sink : sinks) {
    NES_DEBUG("Dispatcher: add sink" << sink)
    sink_to_query_map[sink.get()].emplace_back(qep);
    sink->setup();
  }
  return true;
}

bool Dispatcher::registerQueryWithoutStart(const QueryExecutionPlanPtr qep) {
  std::unique_lock<std::mutex> lock(queryMutex);
//TODO: do we have to test/enforce here that each QEP has at least one sink and source?

  /**
   * test if elements already exist
   */
  auto sources = qep->getSources();
  for (auto source : sources) {
    NES_DEBUG("Dispatcher: search for source" << source.get())
    if (std::find(source_to_query_map[source.get()].begin(),
                  source_to_query_map[source.get()].end(), qep)
        != source_to_query_map[source.get()].end()) {
      NES_ERROR("Dispatcher: source/qep already exists" << source)
      return false;
    }
  }
  auto windows = qep->getWindows();
  for (auto window : windows) {
    NES_DEBUG("Dispatcher: search for window" << window.get())
    if (std::find(window_to_query_map[window.get()].begin(),
                  window_to_query_map[window.get()].end(), qep)
        != window_to_query_map[window.get()].end()) {
      NES_ERROR("Dispatcher: window/qep already exists" << window)
      return false;
    }
  }

  auto sinks = qep->getSinks();
  for (auto sink : sinks) {
    NES_DEBUG("Dispatcher: search for sink" << sink.get())
    if (std::find(sink_to_query_map[sink.get()].begin(),
                  sink_to_query_map[sink.get()].end(), qep)
        != sink_to_query_map[sink.get()].end()) {
      NES_ERROR("Dispatcher: sink/qep already exists" << sink)
      return false;
    }
  }

  /**
   * add elements
   */
  for (auto source : sources) {
    NES_DEBUG("Dispatcher: add source" << source.get())
    source_to_query_map[source.get()].emplace_back(qep);
  }

  for (auto window : windows) {
    NES_DEBUG("Dispatcher: add window" << window)
    window_to_query_map[window.get()].emplace_back(qep);
    window->setup();
  }

  for (auto sink : sinks) {
    NES_DEBUG("Dispatcher: add sink" << sink)
    sink_to_query_map[sink.get()].emplace_back(qep);
  }
  return true;
}

void Dispatcher::deregisterQuery(const QueryExecutionPlanPtr qep) {
  std::unique_lock<std::mutex> lock(queryMutex);

  auto sources = qep->getSources();
  for (auto source : sources) {
    NES_DEBUG("Dispatcher: try remove source" << source)
    if (source_to_query_map[source.get()].size() > 1) {
      //if more than one qep for the source, delete only qep
      NES_DEBUG("Dispatcher: remove only qep" << qep)
      source_to_query_map[source.get()].erase(
          std::find(source_to_query_map[source.get()].begin(),
                    source_to_query_map[source.get()].end(), qep));
    } else  //if last qep for source, delete source
    {
      NES_DEBUG("Dispatcher: stop source" << qep)
      source->stop();

      NES_DEBUG("Dispatcher: delete source" << qep)
      source_to_query_map[source.get()].clear();
      source_to_query_map.erase(source.get());
    }
  }

  auto windows = qep->getWindows();
  for (auto window : windows) {
    NES_DEBUG("Dispatcher: try remove window" << window)
    if (window_to_query_map[window.get()].size() > 1) {
      //if more than one qep for the window, delete only qep
      NES_DEBUG("Dispatcher: remove only qep" << qep)
      window_to_query_map[window.get()].erase(
          std::find(window_to_query_map[window.get()].begin(),
                    window_to_query_map[window.get()].end(), qep));
    } else  //if last qep for window, delete window
    {
      NES_DEBUG("Dispatcher: stop window" << window)
      window->stop();

      NES_DEBUG("Dispatcher: delete window" << window)
      window_to_query_map[window.get()].clear();
      window_to_query_map.erase(window.get());
    }
  }

  auto sinks = qep->getSinks();
  for (auto sink : sinks) {
    NES_DEBUG("Dispatcher: try remove sink" << sink)
    if (sink_to_query_map[sink.get()].size() > 1) {
      //if more than one qep for the window, delete only qep
      NES_DEBUG("Dispatcher: remove only qep" << qep)
      sink_to_query_map[sink.get()].erase(
          std::find(sink_to_query_map[sink.get()].begin(),
                    sink_to_query_map[sink.get()].end(), qep));
    } else  //if last qep for window, delete window
    {
      NES_DEBUG("Dispatcher: stop sink" << sink)
      sink->shutdown();

      NES_DEBUG("Dispatcher: delete sink" << sink)
      sink_to_query_map[sink.get()].clear();
      sink_to_query_map.erase(sink.get());
    }
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
  std::vector<QueryExecutionPlanPtr> &queries = window_to_query_map[window];
  // TODO We currently assume that a window ends a query execution plan
  for (uint64_t i = 0; i < queries.size(); ++i) {
    // for each respective sink, create output the window aggregates
    auto sinks = queries[i]->getSinks();
    for (auto &sink : sinks) {
      sink->writeData(window_aggregates);
      NES_DEBUG(
          "Dispatcher: write window aggregates to sink " << sink << " for QEP " << queries[i].get() << " tupleBuffer "
                                                         << window_aggregates)
    }
  }
  cv.notify_all();
}

void Dispatcher::addWork(const TupleBufferPtr buf, DataSource *source) {
  std::unique_lock<std::mutex> lock(workMutex);

  //get the queries that fetches data from this source
  std::vector<QueryExecutionPlanPtr> &queries = source_to_query_map[source];

  //set the useCount of the buffer
  buf->setUseCnt(queries.size());

  for (uint64_t i = 0; i < queries.size(); ++i) {
    // for each respective source, create new task and put it into queue
    //TODO: is this handeled right? how do we get the stateID here?
    TaskPtr task(
        new Task(queries[i], queries[i]->stageIdFromSource(source), buf));
    task_queue.push_back(task);
    NES_DEBUG(
        "Dispatcher: added Task " << task.get() << " for source " << source << " for QEP " << queries[i].get()
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
