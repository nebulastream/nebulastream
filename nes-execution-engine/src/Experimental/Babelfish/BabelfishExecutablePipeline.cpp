#include <Experimental/Babelfish/BabelfishExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <babelfish.h>

namespace NES::ExecutionEngine::Experimental {

BabelfishExecutablePipeline::BabelfishExecutablePipeline(
    std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
    std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
    graal_isolatethread_t* isolate,
    void* pipelineContext)
    : ExecutablePipeline(executionContext, physicalOperatorPipeline), isolate(isolate), pipelineContext(pipelineContext) {}

void BabelfishExecutablePipeline::setup() {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(nullptr, executionContext.get());
    auto runtimeExecutionContextRef = Interpreter::Value<Interpreter::MemRef>(
        std::make_unique<Interpreter::MemRef>(Interpreter::MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Types::StampFactory::createAddressStamp());
    auto ctx = Interpreter::RuntimeExecutionContext(runtimeExecutionContextRef);
    physicalOperatorPipeline->getRootOperator()->setup(ctx);
}
void BabelfishExecutablePipeline::execute(NES::Runtime::WorkerContext& workerContext, NES::Runtime::TupleBuffer& buffer) {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(&workerContext, executionContext.get());
    executePipeline(isolate, pipelineContext, (void*) &runtimeExecutionContext, std::addressof(buffer));
}

BabelfishExecutablePipeline::~BabelfishExecutablePipeline() { graal_detach_all_threads_and_tear_down_isolate(isolate); }
}// namespace NES::ExecutionEngine::Experimental