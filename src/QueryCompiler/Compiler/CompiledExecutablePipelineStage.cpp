#include <QueryCompiler/Compiler/CompiledExecutablePipelineStage.hpp>
#include <QueryCompiler/Compiler/CompiledCode.hpp>
namespace NES{

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(CompiledCodePtr compiledCode):compiledCode(compiledCode) {
    auto createFunction = compiledCode->getFunctionPointer<CreateFunctionPtr>(MANGELED_ENTY_POINT);
    this->executablePipelineStage = (*createFunction)();
}

ExecutablePipelineStagePtr CompiledExecutablePipelineStage::create(CompiledCodePtr compiledCode) {
    return std::make_shared<CompiledExecutablePipelineStage>(compiledCode);
}

uint32_t CompiledExecutablePipelineStage::execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) {
    return this->executablePipelineStage->execute(inputTupleBuffer, pipelineExecutionContext, workerContext);
}

CompiledExecutablePipelineStage::~CompiledExecutablePipelineStage() {
    // First we have to destroy the pipeline stage only afterwards we can remove the associated code.
    this->executablePipelineStage.reset();
    this->compiledCode.reset();
}


}