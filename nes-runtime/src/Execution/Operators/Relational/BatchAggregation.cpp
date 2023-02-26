#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/Relational/BatchAggregation.hpp>
#include <Execution/Operators/Relational/BatchAggregationHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {

void setupHandler(void* ss, void* ctx, uint64_t size) {
    auto handler = static_cast<BatchAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, size);
}

void* getThreadLocalState(void* op, uint64_t workerId) {
    auto handler = static_cast<BatchAggregationHandler*>(op);
    return handler->getThreadLocalState(workerId);
}

//void* getDefaultState(void* ss) {
//    auto handler = static_cast<BatchAggregationHandler*>(ss);
//    return handler->getDefaultState()->ptr;
//}

class ThreadLocalAggregationState : public OperatorState {
  public:
    explicit ThreadLocalAggregationState(const Value<MemRef>& stateReference) : stateReference(stateReference) {}
    Value<MemRef> stateReference;
};

BatchAggregation::BatchAggregation(
    uint64_t operatorHandlerIndex,
    const std::vector<Expressions::ExpressionPtr>& aggregationExpressions,
    const std::vector<std::shared_ptr<Execution::Aggregation::AggregationFunction>>& aggregationFunctions,
    const std::vector<std::string>& aggregationResultFields)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationExpressions(aggregationExpressions),
      aggregationFunctions(aggregationFunctions), aggregationResultFields(aggregationResultFields) {}

void BatchAggregation::setup(ExecutionContext& ctx) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<UInt64> entrySize = (uint64_t) 0;
    for (auto& function : aggregationFunctions) {
        entrySize = entrySize + function->getSize();
    }
    Nautilus::FunctionCall("setupHandler", setupHandler, globalOperatorHandler, ctx.getPipelineContext(), entrySize);
    //auto defaultState = Nautilus::FunctionCall("getDefaultState", getDefaultState, globalOperatorHandler);
    //for (const auto& function : aggregationFunctions) {
    //    function->reset(defaultState);
    //    defaultState = defaultState + function->getSize();
    //}
}

void BatchAggregation::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local slice store according to the worker id.
    auto defaultState =
        Nautilus::FunctionCall("getThreadLocalState", getThreadLocalState, globalOperatorHandler, ctx.getWorkerId());
    auto threadLocalState = std::make_unique<ThreadLocalAggregationState>(defaultState);
    ctx.setLocalOperatorState(this, std::move(threadLocalState));
    ExecutableOperator::open(ctx, rb);
}

void BatchAggregation::execute(ExecutionContext& ctx, Record& record) const {
    auto aggregationState = (ThreadLocalAggregationState*) ctx.getLocalState(this);
    for (uint64_t aggIndex = 0; aggIndex < aggregationFunctions.size(); aggIndex++) {
        auto inputValue = aggregationExpressions[aggIndex]->execute(record);
        aggregationFunctions[aggIndex]->lift(aggregationState->stateReference, inputValue);
    }
}

void BatchAggregation::close(ExecutionContext&, RecordBuffer&) const {}
void BatchAggregation::terminate(ExecutionContext&) const {}

}// namespace NES::Runtime::Execution::Operators