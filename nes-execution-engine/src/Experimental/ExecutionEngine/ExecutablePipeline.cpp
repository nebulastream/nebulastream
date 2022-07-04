#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/Runtime/ExecutionContext.hpp>

namespace NES::ExecutionEngine::Experimental {

ExecutablePipeline::ExecutablePipeline(std::shared_ptr<Runtime::Execution::PipelineContext> executionContext,
                                       std::unique_ptr<mlir::ExecutionEngine> engine)
    : executionContext(std::move(executionContext)), engine(std::move(engine)) {}

void ExecutablePipeline::execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) {
    auto runtimeExecutionContext = Runtime::Execution::ExecutionContext(&workerContext, executionContext.get());
    auto function = (void (*)(void*, void*)) engine->lookup("execute").get();
    function((void*) &runtimeExecutionContext, std::addressof(buffer));
}

}// namespace NES::ExecutionEngine::Experimental