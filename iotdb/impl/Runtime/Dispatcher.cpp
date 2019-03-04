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

Dispatcher::Dispatcher() : sources(), task_queue(), source_to_query_map(), mutex() {
  IOTDB_DEBUG("Init Dispatcher")
}

Dispatcher::~Dispatcher() {
	IOTDB_DEBUG("Dispatcher: Enter Destructor of Dispatcher")
	sources.clear();
	IOTDB_DEBUG("Dispatcher: Destroyed Source")
 	task_queue.clear();
 	IOTDB_DEBUG("Dispatcher: Destroyed Task Queue")
 	source_to_query_map.clear();
 	IOTDB_DEBUG("Dispatcher: Destroyed Dispatcher")

}

TupleBufferPtr Dispatcher::getBuffer() {
//  std::unique_lock<std::mutex> lock(mutex);
//  uint64_t buffer_size = number_of_tuples * tuple_size_bytes;
//  void *buffer = malloc(buffer_size);
//  TupleBuffer buf(buffer, buffer_size, tuple_size_bytes, number_of_tuples);
	IOTDB_DEBUG("Dispatcher: Dispatcher returns buffer")
  return BufferManager::instance().getBuffer();
}

void Dispatcher::registerSource(DataSourcePtr source) {
  if (source) {
	  IOTDB_DEBUG("Dispatcher: Register Source " << source.get() << "in Dispatcher")
    sources.push_back(source);
  }
}

void Dispatcher::registerWindow(WindowPtr window) {
  // std::unique_lock<std::mutex> lock(mutex);
  if (window) {
	  IOTDB_DEBUG("Dispatcher: Register Window " << window.get() << "in Dispatcher")
    windows.push_back(window);
  }
}
void Dispatcher::deregisterSource(DataSourcePtr source)
{
//	std::unique_lock<std::mutex> lock(mutex);
	if (source) {
		std::vector<DataSourcePtr>::iterator it = sources.begin();
		while(it != sources.end()) {

	    	if(it->get() == source.get())
	    	{
	    		IOTDB_DEBUG("Dispatcher: Deregister Source " << source.get() << "in Dispatcher")
		        it = sources.erase(it);
		        return;
		    }
		    else
		    	++it;
		}
		IOTDB_ERROR("Dispatcher: ERROR: tried to deregister unregistered Buffer")
		assert(0);
	}
}

void Dispatcher::deregisterWindow(WindowPtr window)
{
//	std::unique_lock<std::mutex> lock(mutex);
	if (window) {
		std::vector<WindowPtr>::iterator it = windows.begin();
		while(it != windows.end()) {

	    	if(it->get() == window.get())
	    	{
	    		IOTDB_DEBUG("Dispatcher: Deregister Window " << window.get() << "in Dispatcher")
		        it = windows.erase(it);
		        return;
		    }
		    else
		    	++it;
		}
		IOTDB_ERROR("Dispatcher: ERROR: tried to deregister unregistered window")
		assert(0);
	}
}

void Dispatcher::registerQuery(const QueryExecutionPlanPtr qep) {
  std::unique_lock<std::mutex> lock(mutex);

  auto sources = qep->getSources();
  for (auto source : sources) {
    registerSource(source);
    source_to_query_map[source.get()].emplace_back(qep);
    source->start();
  }
  auto windows = qep->getWindows();
	for (auto window : windows) {
	  registerWindow(window);
	  window_to_query_map[window.get()].emplace_back(qep);
	  window->setup();
	}
}

void Dispatcher::deregisterQuery(const QueryExecutionPlanPtr qep)
{
	std::unique_lock<std::mutex> lock(mutex);
	auto sources = qep->getSources();
	for (auto source : sources) {
		deregisterSource(source);
		source_to_query_map.erase(source.get());
		source->stop();
	  }
	  auto windows = qep->getWindows();

	for (auto window : windows) {
		  deregisterWindow(window);
		  window_to_query_map.erase(window.get());
		  window->shutdown();
		}
}

TaskPtr Dispatcher::getWork(bool &run_thread) {
  std::unique_lock<std::mutex> lock(mutex);
  while (task_queue.empty() && run_thread) {
    cv.wait(lock);
    if (!run_thread)
      return TaskPtr();
  }
  TaskPtr task = task_queue.front();
  task_queue.erase(task_queue.begin());
  IOTDB_DEBUG("Dispatcher: give task" << task.get() << " to thread (getWork())")
  return task;
}

void Dispatcher::addWork(const TupleBufferPtr buf, DataSource *source) {
  std::unique_lock<std::mutex> lock(mutex);
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
  std::unique_lock<std::mutex> lock(mutex);
  task->releaseInputBuffer();
}

Dispatcher &Dispatcher::instance() {
  static Dispatcher instance;
  return instance;
}
} // namespace iotdb
