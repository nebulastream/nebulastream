/*
 * Dispatcher.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */
#include <Runtime/Dispatcher.hpp>
#include <iostream>

namespace iotdb {

Dispatcher::Dispatcher() : sources(), task_queue(), source_to_query_map(), mutex() {
  std::cout << "Init Dispatcher" << std::endl;
}

Dispatcher::~Dispatcher() {
  std::cout << "Enter Destructor of Dispatcher" << std::endl;
  sources.clear();
  std::cout << "Destroyed Sources" << std::endl;
  task_queue.clear();
  std::cout << "Destroyed Task Queue" << std::endl;
  source_to_query_map.clear();
  std::cout << "Destroyed Dispatcher" << std::endl;
}

TupleBuffer Dispatcher::getBuffer(uint32_t number_of_tuples) {
  std::unique_lock<std::mutex> lock(mutex);
  uint32_t tuple_size_bytes = sizeof(uint64_t);
  uint64_t buffer_size = number_of_tuples * tuple_size_bytes;
  void *buffer = malloc(buffer_size);
  TupleBuffer buf(buffer, buffer_size, tuple_size_bytes, number_of_tuples);
  return buf;
}

void Dispatcher::registerSource(DataSourcePtr source) {
  // std::unique_lock<std::mutex> lock(mutex);
  if (source) {
    std::cout << "Register Source " << source.get() << "in Dispatcher" << std::endl;
    sources.push_back(source);
  }
}

void Dispatcher::deregisterSource(DataSourcePtr) { std::unique_lock<std::mutex> lock(mutex); }

void Dispatcher::registerQuery(const QueryExecutionPlanPtr qep) {
  std::unique_lock<std::mutex> lock(mutex);

  auto sources = qep->getSources();
  for (auto source : sources) {
    registerSource(source);
    source_to_query_map[source.get()].push_back(qep);
    source->start();
  }
}

void Dispatcher::deregisterQuery(const QueryExecutionPlanPtr) { std::unique_lock<std::mutex> lock(mutex); }

TaskPtr Dispatcher::getWork(bool &run_thread) {
  std::unique_lock<std::mutex> lock(mutex);
  while (task_queue.empty() && run_thread) {
    cv.wait(lock);
    if (!run_thread)
      return TaskPtr();
  }
  TaskPtr task = task_queue.front();
  task_queue.erase(task_queue.begin());
  return task;
}

void Dispatcher::addWork(const TupleBuffer &buf, DataSource *source) {
  std::unique_lock<std::mutex> lock(mutex);
  std::vector<QueryExecutionPlanPtr> &queries = source_to_query_map[source];
  for (uint64_t i = 0; i < queries.size(); ++i) {
    TaskPtr task(new Task(queries[i], queries[i]->stageIdFromSource(source), source, buf));
    task_queue.push_back(task);
    std::cout << "Dispatcher: added Task for source " << source << " for QEP " << queries[i].get() << std::endl;
  }
  cv.notify_all();
}

void Dispatcher::completedWork(TaskPtr) { std::unique_lock<std::mutex> lock(mutex); }

Dispatcher &Dispatcher::instance() {
  static Dispatcher instance;
  return instance;
}
}
