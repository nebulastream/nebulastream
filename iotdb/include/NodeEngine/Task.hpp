/*
 * Task.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_TASK_H_
#define INCLUDE_TASK_H_
#include <Core/TupleBuffer.hpp>
#include <memory>

namespace iotdb {
class DataSource;
class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class Task {
  public:
    Task(QueryExecutionPlanPtr _qep, uint32_t _pipeline_stage_id, DataSource* _source, const TupleBufferPtr buf);
    void releaseInputBuffer();
    bool execute();
    size_t getNumberOfTuples(){return buf->num_tuples;};
  private:
    QueryExecutionPlanPtr qep;
    uint32_t pipeline_stage_id;
    DataSource* source;
    const TupleBufferPtr buf;
};

typedef std::shared_ptr<Task> TaskPtr;
} // namespace iotdb

#endif /* INCLUDE_TASK_H_ */
