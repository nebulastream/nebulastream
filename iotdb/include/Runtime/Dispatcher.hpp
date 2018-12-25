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

#include <Runtime/DataSource.hpp>
#include <Runtime/Task.hpp>
#include <core/QueryExecutionPlan.hpp>
#include <core/TupleBuffer.hpp>

namespace iotdb {

class Dispatcher {
public:
  TupleBuffer getBuffer();

  void registerQuery(const QueryExecutionPlanPtr);
  void deregisterQuery(const QueryExecutionPlanPtr);

  TaskPtr getWork(bool &run_thread);
  void addWork(const TupleBuffer &, DataSource *);
  void completedWork(TaskPtr);

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
  void registerSource(DataSourcePtr);
  void deregisterSource(DataSourcePtr);

  std::vector<DataSourcePtr> sources;
  std::vector<TaskPtr> task_queue;
  std::map<DataSource *, std::vector<QueryExecutionPlanPtr>> source_to_query_map;
  std::mutex mutex;
  std::condition_variable cv;
};
typedef std::shared_ptr<Dispatcher> DispatcherPtr;
}

#endif /* INCLUDE_DISPATCHER_H_ */
