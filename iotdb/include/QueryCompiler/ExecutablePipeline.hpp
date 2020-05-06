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

class ExecutablePipeline {
  public:
    virtual ~ExecutablePipeline() = default;

    /**
     * @brief Creates a copy of the executable pipeline.
     * @return ExecutablePipelinePtr
     */
    virtual ExecutablePipelinePtr copy() const = 0;

    /**
     * @brief Executes the pipeline given the input
     * @return error code: 1 for valid execution, 0 for error
     */
    virtual uint32_t execute(TupleBuffer& input_buffers,
                             void* state,
                             WindowManagerPtr window_manager,
                             TupleBuffer& result_buf) = 0;
};

}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_
