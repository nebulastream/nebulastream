#include <Experimental/Runtime/ExecutionContext.hpp>

namespace NES::Runtime::Execution {

ExecutionContext::ExecutionContext(Runtime::WorkerContext* workerContextPtr,
                                   PipelineContext* PipelineContextPtr)
    : workerContextPtr(workerContextPtr), pipelineContextPtr(PipelineContextPtr) {}

PipelineContext* ExecutionContext::getPipelineContext() { return pipelineContextPtr; }

Runtime::WorkerContext* ExecutionContext::getWorkerContext() { return workerContextPtr; }

}// namespace NES::Runtime::Execution