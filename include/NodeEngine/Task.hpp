#ifndef INCLUDE_TASK_H_
#define INCLUDE_TASK_H_
#include <NodeEngine/TupleBuffer.hpp>
#include <memory>
namespace NES {

class WorkerContext;
class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

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
    explicit Task(PipelineStagePtr pipeline, TupleBuffer& buf);

    explicit Task() : pipeline(nullptr), buf() {
        // nop
    }

    ~Task() = default;

    /**
     * @brief execute the task by calling executeStage of QEP and providing the stageId and the buffer
     */
    bool operator()(WorkerContext& workerContext);

    /**
     * @brief return the number of tuples in the input buffer (for statistics)
     * @return number of input tuples in buffer
     */
    size_t getNumberOfTuples();

    /**
     * @brief method to return the qep of a task
     * @return
     */
    PipelineStagePtr getPipelineStage();

    /**
     * @brief method to check if it is a watermark-only buffer
     * @retun bool indicating if this buffer is for watermarks only
     */
     bool isWaterMarkOnly();

    /**
     * @return true if this Task is valid and it is safe to execute
     */
    explicit operator bool() const;

    /**
     * @return true if this Task is valid and it is safe to execute
     */
    bool operator!() const;

    friend std::ostream& operator<<(std::ostream& os, const Task&) {
        os << "Task()";
        return os;
    }

  private:
    PipelineStagePtr pipeline;
    TupleBuffer buf;
};

}// namespace NES

#endif /* INCLUDE_TASK_H_ */
