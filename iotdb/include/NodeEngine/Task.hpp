#ifndef INCLUDE_TASK_H_
#define INCLUDE_TASK_H_
#include <memory>

namespace NES {
class DataSource;
class QueryExecutionPlan;
class TupleBuffer;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

/**
 * @brief Task abstraction to bind processing (compiled binary) and data (incoming buffers
 * @Limitations:
 *    -
 */
class Task {
  public:

    /**
     * @brief Task constructor
     * @param pointer to query execution plan that should be applied on the incoming buffer
     * @param id of the pipeline stage inside the QEP that should be applied
     * @param pointer to the tuple buffer that has to be process
     */
    explicit Task(QueryExecutionPlanPtr _qep, uint32_t _pipeline_stage_id, TupleBuffer& buf);

    ~Task() = default;

    /**
     * @brief execute the task by calling executeStage of QEP and providing the stageId and the buffer
     */
    bool execute();

    /**
     * @brief return the number of tuples in the input buffer (for statistics)
     * @return number of input tuples in buffer
     */
    size_t getNumberOfTuples();

    /**
     * @brief method to return the qep of a task
     * @return
     */
    QueryExecutionPlanPtr getQep();
  private:
    QueryExecutionPlanPtr qep;
    uint32_t pipeline_stage_id;
    TupleBuffer buf;
};

typedef std::shared_ptr<Task> TaskPtr;
}  // namespace NES

#endif /* INCLUDE_TASK_H_ */
