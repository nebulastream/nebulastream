/*
 * Dispatcher.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_DISPATCHER_H_
#define INCLUDE_DISPATCHER_H_

#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include <CodeGen/QueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Task.hpp>

namespace iotdb {

class Dispatcher {
public:

  void registerQuery(const QueryExecutionPlanPtr);
  void deregisterQuery(const QueryExecutionPlanPtr);

  TaskPtr getWork(bool &run_thread);
  void addWork(const TupleBufferPtr, DataSource *);
  void completedWork(TaskPtr task);

  void printStatistics(const QueryExecutionPlanPtr qep);

  static Dispatcher &instance();

  void unblockThreads() { cv.notify_all(); }

private:
  /* implement singleton semantics: no construction,
   * copying or destruction of Dispatcher objects
   * outside of the class */
  Dispatcher();
  Dispatcher(const Dispatcher &);
  Dispatcher &operator=(const Dispatcher &);
  ~Dispatcher();

  std::vector<TaskPtr> task_queue;
  std::map<DataSource *, std::vector<QueryExecutionPlanPtr>> source_to_query_map;
  std::map<Window *, std::vector<QueryExecutionPlanPtr>> window_to_query_map;
  std::map<DataSink*, std::vector<QueryExecutionPlanPtr>> sink_to_query_map;


  std::mutex bufferMutex;
  std::mutex queryMutex;
  std::mutex workMutex;

  std::condition_variable cv;

  //statistics:
  size_t workerHitEmptyTaskQueue;
  size_t processedTasks;
};
typedef std::shared_ptr<Dispatcher> DispatcherPtr;
} // namespace iotdb

#endif /* INCLUDE_DISPATCHER_H_ */
