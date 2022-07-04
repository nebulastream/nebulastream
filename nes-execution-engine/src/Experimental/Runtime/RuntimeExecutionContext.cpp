#include <Experimental/Runtime/RuntimeExecutionContext.hpp>

namespace NES::Runtime::Execution {

RuntimeExecutionContext::RuntimeExecutionContext(Runtime::WorkerContext* workerContextPtr,
                                                 RuntimePipelineContext* PipelineContextPtr)
    : workerContextPtr(workerContextPtr), pipelineContextPtr(PipelineContextPtr) {}

RuntimePipelineContext* RuntimeExecutionContext::getPipelineContext() { return pipelineContextPtr; }

Runtime::WorkerContext* RuntimeExecutionContext::getWorkerContext() { return workerContextPtr; }

}// namespace NES::Runtime::Execution