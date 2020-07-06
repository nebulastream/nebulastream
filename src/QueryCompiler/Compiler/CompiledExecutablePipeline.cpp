#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <utility>

namespace NES {

static std::string mangledEntryPoint = "_Z14compiled_queryRN3NES11TupleBufferEPvPNS_13WindowManagerERNS_24PipelineExecutionContextE";

CompiledExecutablePipeline::CompiledExecutablePipeline(CompiledCodePtr compiled_code) : compiledCode(std::move(compiled_code)) {
}

typedef uint32_t (*PipelineFunctionPtr)(TupleBuffer&, void*, WindowManager*, const PipelineExecutionContext&);

uint32_t CompiledExecutablePipeline::execute(TupleBuffer& inputBuffer,
                                             void* state,
                                             WindowManagerPtr window_manager,
                                             PipelineExecutionContext& context) {
    PipelineFunctionPtr fn = compiledCode->getFunctionPointer<PipelineFunctionPtr>(mangledEntryPoint);

    return (*fn)(inputBuffer, state, window_manager.get(), context);
}

ExecutablePipelinePtr CompiledExecutablePipeline::create(const CompiledCodePtr compiledCode) {
    return std::make_shared<CompiledExecutablePipeline>(compiledCode);
}

}// namespace NES