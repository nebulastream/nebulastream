#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <utility>

namespace NES {

// TODO this might change across OS
static constexpr auto mangledEntryPoint = "_Z14compiled_queryRN3NES11TupleBufferEPvPNS_13WindowManagerERNS_24PipelineExecutionContextERNS_13WorkerContextE";

CompiledExecutablePipeline::CompiledExecutablePipeline(CompiledCodePtr compiled_code)
    : compiledCode(std::move(compiled_code)), pipelineFunc(compiledCode->getFunctionPointer<PipelineFunctionPtr>(mangledEntryPoint)) {
    // nop
}

uint32_t CompiledExecutablePipeline::execute(TupleBuffer& inputBuffer,
                                             void* state,
                                             WindowManagerPtr windowManager,
                                             QueryExecutionContextPtr context,
                                             WorkerContextRef wctx) {
    return (*pipelineFunc)(inputBuffer, state, windowManager.get(), *context.get(), wctx);
}

ExecutablePipelinePtr CompiledExecutablePipeline::create(const CompiledCodePtr compiledCode) {
    return std::make_shared<CompiledExecutablePipeline>(compiledCode);
}

}// namespace NES