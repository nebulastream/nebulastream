#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_BabelfishExecutablePipeline_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_BabelfishExecutablePipeline_HPP_
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>

struct __graal_isolatethread_t;
typedef struct __graal_isolatethread_t graal_isolatethread_t;

namespace NES::ExecutionEngine::Experimental {

class BabelfishExecutablePipeline : public ExecutablePipeline {
  public:
    BabelfishExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                graal_isolatethread_t* isolate, void* pipelineContext);
    void setup() override;
    void execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) override;
    ~BabelfishExecutablePipeline() override;

  private:
   graal_isolatethread_t* isolate;
   void* pipelineContext;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_BabelfishExecutablePipeline_HPP_
