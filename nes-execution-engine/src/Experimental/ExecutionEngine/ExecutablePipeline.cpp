#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>

namespace NES::ExecutionEngine::Experimental {

ExecutablePipeline::ExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                       std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                       std::unique_ptr<mlir::ExecutionEngine> engine)
    : executionContext(std::move(executionContext)), physicalOperatorPipeline(physicalOperatorPipeline),
      engine(std::move(engine)) {}

void ExecutablePipeline::execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(&workerContext, executionContext.get());
    auto function = (void (*)(void*, void*)) engine->lookup("execute").get();
    function((void*) &runtimeExecutionContext, std::addressof(buffer));
}

void ExecutablePipeline::setup() {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(nullptr, executionContext.get());
    auto runtimeExecutionContextRef = Interpreter::Value<Interpreter::MemRef>(
        std::make_unique<Interpreter::MemRef>(Interpreter::MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Operations::INT8PTR);
    auto ctx = Interpreter::RuntimeExecutionContext(runtimeExecutionContextRef);
    physicalOperatorPipeline->getRootOperator()->setup(ctx);
}

}// namespace NES::ExecutionEngine::Experimental