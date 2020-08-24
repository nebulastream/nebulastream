#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <utility>

namespace NES {


PipelineExecutionContext::PipelineExecutionContext(
    QueryExecutionPlanId queryId,
    BufferManagerPtr bufferManager,
    std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunction)
    : queryId(queryId), bufferManager(std::move(bufferManager)), emitFunctionHandler(std::move(emitFunction)) {
    // nop
}

TupleBuffer PipelineExecutionContext::allocateTupleBuffer() {
    return bufferManager->getBufferBlocking();
}

void PipelineExecutionContext::emitBuffer(TupleBuffer& outputBuffer, WorkerContextRef workerContext) {
    // call the function handler
    emitFunctionHandler(outputBuffer, workerContext);
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