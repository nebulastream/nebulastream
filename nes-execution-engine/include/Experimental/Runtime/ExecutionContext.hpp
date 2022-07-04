#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_RUNTIME_EXECUTIONCONTEXT_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_RUNTIME_EXECUTIONCONTEXT_HPP_
#include <memory>
namespace NES::Runtime {
class WorkerContext;
}
namespace NES::Runtime::Execution {
class PipelineContext;
class ExecutionContext {
  public:
    ExecutionContext(Runtime::WorkerContext* workerContextPtr, PipelineContext* PipelineContextPtr);
    Runtime::WorkerContext* getWorkerContext();
    PipelineContext* getPipelineContext();

  private:
    Runtime::WorkerContext* workerContextPtr;
    PipelineContext* pipelineContextPtr;
};

}// namespace NES::Runtime::Execution

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_RUNTIME_EXECUTIONCONTEXT_HPP_
