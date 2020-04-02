#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <utility>

namespace NES {

static constexpr char mangledEntryPoint[] = "_Z14compiled_queryP11TupleBufferPvPN3NES13WindowManagerES0_";

CompiledExecutablePipeline::CompiledExecutablePipeline(CompiledCodePtr compiled_code) :
    compiled_code_(std::move(compiled_code)) {
}

CompiledExecutablePipeline::CompiledExecutablePipeline(const CompiledExecutablePipeline& other) {
    /* TODO consider deep copying this! */
    this->compiled_code_ = other.compiled_code_;
}

ExecutablePipelinePtr CompiledExecutablePipeline::copy() const {
    return ExecutablePipelinePtr(new CompiledExecutablePipeline(*this));
}

typedef uint32_t (*PipelineFunctionPtr)(TupleBuffer&, void*, WindowManager*, TupleBuffer&);

uint32_t CompiledExecutablePipeline::execute(TupleBuffer& inputBuffer,
                                             void* state,
                                             WindowManagerPtr window_manager,
                                             TupleBuffer& outputBuffer) {
    PipelineFunctionPtr fn = compiled_code_->getFunctionPointer<PipelineFunctionPtr>(mangledEntryPoint);
    return (*fn)(inputBuffer, state, window_manager.get(), outputBuffer);
}

ExecutablePipelinePtr createCompiledExecutablePipeline(const CompiledCodePtr& compiledCode) {
    return std::make_shared<CompiledExecutablePipeline>(compiledCode);
}

}