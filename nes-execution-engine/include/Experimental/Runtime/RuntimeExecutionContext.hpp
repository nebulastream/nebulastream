#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_RUNTIME_RUNTIMEEXECUTIONCONTEXT_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_RUNTIME_RUNTIMEEXECUTIONCONTEXT_HPP_
#include <memory>
namespace NES::Runtime {
class WorkerContext;
}
namespace NES::Runtime::Execution {
class RuntimePipelineContext;
class RuntimeExecutionContext {
  public:
    RuntimeExecutionContext(Runtime::WorkerContext* workerContextPtr, RuntimePipelineContext* PipelineContextPtr);
    Runtime::WorkerContext* getWorkerContext();
    RuntimePipelineContext* getPipelineContext();

  private:
    Runtime::WorkerContext* workerContextPtr;
    RuntimePipelineContext* pipelineContextPtr;
};

}// namespace NES::Runtime::Execution

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_RUNTIME_RUNTIMEEXECUTIONCONTEXT_HPP_
