#ifndef NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_

#include <memory>
#include <string>
namespace NES {

class TupleBuffer;
class WindowManager;
typedef std::shared_ptr<WindowManager> WindowManagerPtr;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class PipelineExecutionContext;
typedef std::shared_ptr<PipelineExecutionContext> QueryExecutionContextPtr;

class WorkerContext;
typedef WorkerContext& WorkerContextRef;

// TODO move state and windowManager inside the PipelineExecutionContext
class ExecutablePipeline {
  public:
    virtual ~ExecutablePipeline() = default;

    /**
     * @brief Executes the pipeline given the input
     * @return error code: 0 for valid execution, 1 for error
     */
    // TODO use dedicate type for return
    virtual uint32_t execute(TupleBuffer& input_buffers,
                             void* state,
                             WindowManagerPtr window_manager,
                             QueryExecutionContextPtr context,
                             WorkerContextRef wctx) = 0;
};

}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_
