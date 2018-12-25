/*
 * Task.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_TASK_H_
#define INCLUDE_TASK_H_
#include <core/TupleBuffer.hpp>
#include <memory>

namespace iotdb {
class DataSource;
class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class Task {
public:
  Task(QueryExecutionPlanPtr _qep, uint32_t _pipeline_stage_id, DataSource *_source, const TupleBuffer &_buf);
  bool execute();

private:
  QueryExecutionPlanPtr qep;
  uint32_t pipeline_stage_id;
  DataSource *source;
  const TupleBuffer buf;
};

typedef std::shared_ptr<Task> TaskPtr;
}

#endif /* INCLUDE_TASK_H_ */
