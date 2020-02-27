#ifndef NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_

#include <string>
#include <memory>
namespace NES{

class TupleBuffer;
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;

class WindowManager;
typedef std::shared_ptr<WindowManager> WindowManagerPtr;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

class ExecutablePipeline {
 public:
  virtual ~ExecutablePipeline() = default;
  virtual ExecutablePipelinePtr copy() const = 0;
  virtual uint32_t execute(const TupleBufferPtr input_buffers,
          void *state, WindowManagerPtr window_manager,
               TupleBufferPtr result_buf) = 0;
};



}
#endif //NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_
