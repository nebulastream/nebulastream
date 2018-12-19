/*
 * Task.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_TASK_H_
#define INCLUDE_TASK_H_
#include <memory>
#include "TupleBuffer.hpp"
class DataSource;
class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class Task {
public:
  Task(QueryExecutionPlanPtr _qep, uint32_t _pipeline_stage_id, DataSource *_source, const TupleBuffer &_buf);
  bool execute();

private:
  uint32_t pipeline_stage_id;
  QueryExecutionPlanPtr qep;
  DataSource *source;
  const TupleBuffer buf;
};

typedef std::shared_ptr<Task> TaskPtr;


#endif /* INCLUDE_TASK_H_ */
