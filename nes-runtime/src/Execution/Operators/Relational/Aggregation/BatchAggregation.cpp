/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/StdInt.hpp>

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

class ThreadLocalAggregationState : public OperatorState {
  public:
    explicit ThreadLocalAggregationState(const Value<MemRef>& stateReference) : stateReference(stateReference) {}
    Value<MemRef> stateReference;
};

BatchAggregation::BatchAggregation(
    uint64_t operatorHandlerIndex,
    const std::vector<std::shared_ptr<Execution::Aggregation::AggregationFunction>>& aggregationFunctions)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions) {}

void BatchAggregation::setup(ExecutionContext& ctx) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<UInt64> entrySize = 0_u64;
    for (auto& function : aggregationFunctions) {
        entrySize = entrySize + function->getSize();
    }
    Nautilus::FunctionCall("setupHandler", setupHandler, globalOperatorHandler, ctx.getPipelineContext(), entrySize);
}

void BatchAggregation::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local state and stores the pointer/memref to it in the execution context.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local state according to the worker id.
    auto defaultState =
        Nautilus::FunctionCall("getThreadLocalState", getThreadLocalState, globalOperatorHandler, ctx.getWorkerId());
    auto threadLocalState = std::make_unique<ThreadLocalAggregationState>(defaultState);
    ctx.setLocalOperatorState(this, std::move(threadLocalState));
    ExecutableOperator::open(ctx, rb);
}

void BatchAggregation::execute(ExecutionContext& ctx, Record& record) const {
    // 1. get local state
    auto aggregationState = (ThreadLocalAggregationState*) ctx.getLocalState(this);
    // 2. update aggregates
    for (const auto& aggregationFunction : aggregationFunctions) {
        aggregationFunction->lift(aggregationState->stateReference, record);
    }
}

void BatchAggregation::close(ExecutionContext&, RecordBuffer&) const {}
void BatchAggregation::terminate(ExecutionContext&) const {}

}// namespace NES::Runtime::Execution::Operators