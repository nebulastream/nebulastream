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
    typedef uint32_t (*PipelineFunctionPtr)(TupleBuffer&, void*, WindowManager*, QueryExecutionContextPtr::element_type&);
  public:

    CompiledExecutablePipeline(CompiledCodePtr compiled_code);

    static ExecutablePipelinePtr create(CompiledCodePtr compiledCode);

    uint32_t execute(TupleBuffer& inBuffer,
                     void* state,
                     WindowManagerPtr windowManager,
                     QueryExecutionContextPtr context) override;

  private:
    CompiledCodePtr compiledCode;
    PipelineFunctionPtr pipelineFunc;

};
}// namespace NES

#endif//INCLUDE_COMPILED_EXECUTABLE_PIPELINE_HPP_
