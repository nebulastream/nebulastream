/*
 * Dispatcher.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */
#include <Runtime/Dispatcher.hpp>
#include <iostream>
#include <assert.h>
#include <Runtime/Window.hpp>
#include <Util/Logger.hpp>
namespace iotdb {

Dispatcher::Dispatcher() : task_queue(), source_to_query_map(), window_to_query_map(),
		sink_to_query_map(), bufferMutex(), queryMutex(), workMutex(), workerHitEmptyTaskQueue(0) {
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

void Dispatcher::registerQuery(const QueryExecutionPlanPtr qep) {
  std::unique_lock<std::mutex> lock(queryMutex);

  auto sources = qep->getSources();
  for (auto source : sources) {
    source_to_query_map[source.get()].emplace_back(qep);
    source->start();
  }
  auto windows = qep->getWindows();
	for (auto window : windows) {
	  window_to_query_map[window.get()].emplace_back(qep);
	  window->setup();
	}
	auto sinks = qep->getSinks();
	  for (auto sink : sinks) {
		  sink_to_query_map[sink.get()].emplace_back(qep);
	    sink->setup();
	  }

}

void Dispatcher::deregisterQuery(const QueryExecutionPlanPtr qep)
{
	std::unique_lock<std::mutex> lock(queryMutex);
	auto sources = qep->getSources();
	for (auto source : sources) {
		source_to_query_map.erase(source.get());
		source->stop();
	  }
	  auto windows = qep->getWindows();

	for (auto window : windows) {
		  window_to_query_map.erase(window.get());
		  window->shutdown();
		}

	auto sinks = qep->getSinks();
	for (auto sink : sinks) {
		sink_to_query_map.erase(sink.get());
		sink->shutdown();
	  }
}

TaskPtr Dispatcher::getWork(bool &run_thread) {
  std::unique_lock<std::mutex> lock(workMutex);
  while (task_queue.empty() && run_thread) {
	workerHitEmptyTaskQueue++;
    cv.wait(lock);
    if (!run_thread)
      return TaskPtr();
  }

  TaskPtr task;
  if (run_thread) {
      task = task_queue.front();
  } else {
      task = TaskPtr();
  }

  task_queue.erase(task_queue.begin());
  IOTDB_DEBUG("Dispatcher: give task" << task.get() << " to thread (getWork())")
  return task;
}

void Dispatcher::addWork(const TupleBufferPtr buf, DataSource *source) {
  std::unique_lock<std::mutex> lock(workMutex);
  std::vector<QueryExecutionPlanPtr> &queries = source_to_query_map[source];
  for (uint64_t i = 0; i < queries.size(); ++i) {
    TaskPtr task(new Task(queries[i], queries[i]->stageIdFromSource(source), source, buf));
    task_queue.push_back(task);
    IOTDB_DEBUG("Dispatcher: added Task " << task.get()
    		<< " for source " << source << " for QEP "
			<< queries[i].get()
    		<< " inputBuffer " << buf)
  }
  cv.notify_all();
}

void Dispatcher::completedWork(TaskPtr task) {
  std::unique_lock<std::mutex> lock(workMutex);
  processedTasks++;
  task->releaseInputBuffer();
}

Dispatcher &Dispatcher::instance() {
  static Dispatcher instance;
  return instance;
}

void Dispatcher::printStatistics(const QueryExecutionPlanPtr qep)
{
	IOTDB_INFO("Dispatcher Statistics:")
	IOTDB_INFO("\t workerHitEmptyTaskQueue=" << workerHitEmptyTaskQueue)
	IOTDB_INFO("\t processedTasks=" << processedTasks)

	BufferManager::instance().printStatistics();
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
		IOTDB_INFO("Window Final Result:")
		window->print();
	}
	auto sinks = qep->getSinks();
	for (auto sink : sinks) {
		IOTDB_INFO("Sink:" << sink)
		IOTDB_INFO("\t Generated Buffers=" << sink->getNumberOfProcessedBuffers())
		IOTDB_INFO("\t Generated Tuples=" << sink->getNumberOfProcessedTuples())
	}
}


} // namespace iotdb
