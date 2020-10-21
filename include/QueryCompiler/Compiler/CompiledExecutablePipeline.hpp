#ifndef INCLUDE_COMPILED_EXECUTABLE_PIPELINE_HPP_
#define INCLUDE_COMPILED_EXECUTABLE_PIPELINE_HPP_

#include <QueryCompiler/ExecutablePipeline.hpp>
#include <memory>
#include <string>
namespace NES {

class CompiledCode;
typedef std::shared_ptr<CompiledCode> CompiledCodePtr;

/**
 * @brief A executable pipeline that executes compiled code if invoked.
 */
class CompiledExecutablePipeline : public ExecutablePipeline {
  public:
    typedef uint32_t PipelineFunctionReturnType;
    typedef PipelineFunctionReturnType (*PipelineFunctionPtr)(TupleBuffer&, void*, Windowing::WindowManager*, PipelineExecutionContext&, WorkerContextRef);

  public:
    CompiledExecutablePipeline() = delete;

    explicit CompiledExecutablePipeline(PipelineFunctionPtr func);

    explicit CompiledExecutablePipeline(CompiledCodePtr compiled_code);

    static ExecutablePipelinePtr create(CompiledCodePtr compiledCode);

    uint32_t execute(TupleBuffer& inBuffer,
                     void* state,
                     Windowing::WindowManagerPtr windowManager,
                     QueryExecutionContextPtr context,
                     WorkerContextRef wctx) override;

  private:
    CompiledCodePtr compiledCode;
    PipelineFunctionPtr pipelineFunc;
};
}// namespace NES

#endif//INCLUDE_COMPILED_EXECUTABLE_PIPELINE_HPP_
