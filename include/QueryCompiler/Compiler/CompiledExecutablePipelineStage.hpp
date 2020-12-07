#ifndef NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#include <QueryCompiler/ExecutablePipelineStage.hpp>

namespace NES {

class CompiledExecutablePipelineStage : public ExecutablePipelineStage {
    // TODO this might change across OS
#if defined(__linux__)
    static constexpr auto MANGELED_ENTY_POINT = "_ZN3NES6createEv";
#else
#error "unsupported platform/OS"
#endif

  public:
    explicit CompiledExecutablePipelineStage(CompiledCodePtr compiledCode);
    static ExecutablePipelineStagePtr create(CompiledCodePtr compiledCode);
    ~CompiledExecutablePipelineStage();
    uint32_t execute(TupleBuffer& inputTupleBuffer,
                            PipelineExecutionContext& pipelineExecutionContext,
                            WorkerContext& workerContext) override;

  private:
    typedef std::shared_ptr<ExecutablePipelineStage> (*CreateFunctionPtr)();

    ExecutablePipelineStagePtr executablePipelineStage;
    CompiledCodePtr compiledCode;
};

typedef std::shared_ptr<ExecutablePipelineStage> ExecutablePipelineStagePtr;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
