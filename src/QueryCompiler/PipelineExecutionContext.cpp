
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <utility>
namespace NES {

PipelineExecutionContext::PipelineExecutionContext(
    BufferManagerPtr bufferManager,
    std::function<void(TupleBuffer&)>&& emitFunction)
    : bufferManager(std::move(bufferManager)), emitFunctionHandler(std::move(emitFunction)) {
    // nop
}

TupleBuffer PipelineExecutionContext::allocateTupleBuffer() {
    return bufferManager->getBufferBlocking();
}

void PipelineExecutionContext::emitBuffer(TupleBuffer& outputBuffer) {
    // call the function handler
    emitFunctionHandler(outputBuffer);
}

}// namespace NES