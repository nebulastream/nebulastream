
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <utility>
namespace NES{

PipelineExecutionContext::PipelineExecutionContext(BufferManagerPtr bufferManager,
                                                   std::function<void(TupleBuffer & )> emitFunction):
                                                   bufferManager(std::move(bufferManager)),
                                                   emitFunctionHandler(std::move(emitFunction)) {
}

TupleBuffer PipelineExecutionContext::allocateTupleBuffer() {
    return bufferManager->getBufferBlocking();
}

void PipelineExecutionContext::emitBuffer(TupleBuffer &outputBuffer) {
    // call the function handler
    emitFunctionHandler(outputBuffer);
}



}