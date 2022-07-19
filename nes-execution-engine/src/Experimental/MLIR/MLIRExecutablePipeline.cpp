#include <Experimental/MLIR/MLIRExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>


namespace NES::ExecutionEngine::Experimental {

MLIRExecutablePipeline::MLIRExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                               std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                               std::unique_ptr<mlir::ExecutionEngine> engine)
    : ExecutablePipeline(executionContext, physicalOperatorPipeline), engine(std::move(engine)) {}

void MLIRExecutablePipeline::setup() {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(nullptr, executionContext.get());
    auto runtimeExecutionContextRef = Interpreter::Value<Interpreter::MemRef>(
        std::make_unique<Interpreter::MemRef>(Interpreter::MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Types::StampFactory::createAddressStamp());
    auto ctx = Interpreter::RuntimeExecutionContext(runtimeExecutionContextRef);
    physicalOperatorPipeline->getRootOperator()->setup(ctx);
}
void MLIRExecutablePipeline::execute(NES::Runtime::WorkerContext& workerContext, NES::Runtime::TupleBuffer& buffer) {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(&workerContext, executionContext.get());
    auto function = (void (*)(void*, void*)) engine->lookup("execute").get();
    function((void*) &runtimeExecutionContext, std::addressof(buffer));
}
}// namespace NES::ExecutionEngine::Experimental