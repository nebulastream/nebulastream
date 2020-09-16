#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <Windows/WindowHandler.hpp>
#include <utility>
namespace NES {

PipelineExecutionContext::PipelineExecutionContext(
    BufferManagerPtr bufferManager,
    std::function<void(TupleBuffer&)>&& emitFunctionHandler)
    : bufferManager(std::move(bufferManager)),
      emitFunctionHandler(std::move(emitFunctionHandler)) {
    // nop
}

TupleBuffer PipelineExecutionContext::allocateTupleBuffer() {
    return bufferManager->getBufferBlocking();
}

void PipelineExecutionContext::emitBuffer(TupleBuffer& outputBuffer) {
    // call the function handler
    emitFunctionHandler(outputBuffer);
}
WindowDefinitionPtr PipelineExecutionContext::getWindowDef() {
    return windowDef;
}
void PipelineExecutionContext::setWindowDef(WindowDefinitionPtr windowDef) {
    this->windowDef = windowDef;
}
SchemaPtr PipelineExecutionContext::getInputSchema() {
    return inputSchema;
}
void PipelineExecutionContext::setInputSchema(SchemaPtr inputSchema) {
    this->inputSchema = inputSchema;
}

}// namespace NES