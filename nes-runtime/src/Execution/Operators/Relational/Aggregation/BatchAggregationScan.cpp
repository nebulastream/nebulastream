#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {

void* getStates(void* op, uint64_t workerId) {
    auto handler = static_cast<BatchAggregationHandler*>(op);
    return handler->getThreadLocalState(workerId);
}

BatchAggregationScan::BatchAggregationScan(
    uint64_t operatorHandlerIndex,
    const std::vector<std::shared_ptr<Execution::Aggregation::AggregationFunction>>& aggregationFunctions,
    const std::vector<std::string>& aggregationResultFields)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions),
      aggregationResultFields(aggregationResultFields) {}


void BatchAggregationScan::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    Operators::Operator::open(ctx, rb);

    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local slice store according to the worker id.
    auto state = Nautilus::FunctionCall("getThreadLocalState", getStates, globalOperatorHandler, ctx.getWorkerId());
    Record result;
    for (uint64_t aggIndex = 0; aggIndex < aggregationFunctions.size(); aggIndex++) {
        auto finalAggregationValue = aggregationFunctions[aggIndex]->lower(state);
        result.write(aggregationResultFields[aggIndex], finalAggregationValue);
    }
    child->execute(ctx, result);
}

}// namespace NES::Runtime::Execution::Operators