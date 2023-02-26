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

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/BatchKeyedAggregation.hpp>
#include <Execution/Operators/Relational/BatchKeyedAggregationHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getKeyedStateProxy(void* op, uint64_t workerId) {
    auto handler = static_cast<BatchKeyedAggregationHandler*>(op);
    return handler->getThreadLocalStore(workerId);
}

void setupHandler(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
    auto handler = static_cast<BatchKeyedAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, keySize, valueSize);
}

class LocalKeyedStoreState : public Operators::OperatorState {
  public:
    explicit LocalKeyedStoreState(Interface::ChainedHashMapRef  hm) : hm(std::move(hm)){};

    const Interface::ChainedHashMapRef hm;
};

BatchKeyedAggregation::BatchKeyedAggregation(
    uint64_t operatorHandlerIndex,
    const std::vector<Expressions::ExpressionPtr>& keyExpressions,
    const std::vector<PhysicalTypePtr>& keyDataTypes,
    const std::vector<Expressions::ExpressionPtr>& aggregationExpressions,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction)
    : operatorHandlerIndex(operatorHandlerIndex), keyExpressions(keyExpressions), keyDataTypes(keyDataTypes),
      aggregationExpressions(aggregationExpressions), aggregationFunctions(aggregationFunctions),
      hashFunction(std::move(hashFunction)), keySize(0), valueSize(0) {
    NES_ASSERT(aggregationFunctions.size() == aggregationExpressions.size(),
               "The number of aggregation expression and aggregation functions need to be equals");

    for (auto& keyType : keyDataTypes) {
        keySize = keySize + keyType->size();
    }
    for (auto& function : aggregationFunctions) {
        valueSize = valueSize + function->getSize();
    }
}

void BatchKeyedAggregation::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("setupHandler",
                           setupHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           Value<UInt64>(keySize),
                           Value<UInt64>(valueSize));
}

void BatchKeyedAggregation::open(ExecutionContext& ctx, RecordBuffer&) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local slice store according to the worker id.
    auto state = Nautilus::FunctionCall("getKeyedStateProxy", getKeyedStateProxy, globalOperatorHandler, ctx.getWorkerId());

    auto chainedHM = Interface::ChainedHashMapRef(state, keyDataTypes, keySize, valueSize);
    // 3. store the reference to the slice store in the local operator state.
    auto sliceStoreState = std::make_unique<LocalKeyedStoreState>(chainedHM);
    ctx.setLocalOperatorState(this, std::move(sliceStoreState));
}

void BatchKeyedAggregation::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {

    // 2. derive key values
    std::vector<Value<>> keyValues;
    for (const auto& exp : keyExpressions) {
        keyValues.emplace_back(exp->execute(record));
    }

    // 3. load the reference to the slice store and find the correct slice.
    auto sliceStore = reinterpret_cast<LocalKeyedStoreState*>(ctx.getLocalState(this));
    auto sliceState = sliceStore->hm;

    // 4. calculate hash
    auto hash = hashFunction->calculate(keyValues);

    // 5. create entry in the slice hash map. If the entry is new set default values for aggregations.
    auto entry = sliceState.findOrCreate(hash, keyValues, [this](auto& entry) {
        // set aggregation values if a new entry was created
        auto valuePtr = entry.getValuePtr();
        for (const auto& aggFunction : aggregationFunctions) {
            aggFunction->reset(valuePtr);
            valuePtr = valuePtr + aggFunction->getSize();
        }
    });

    // 6. manipulate the current aggregate values
    auto valuePtr = entry.getValuePtr();
    for (size_t i = 0; i < aggregationFunctions.size(); ++i) {
        auto value = aggregationExpressions[i]->execute(record);
        aggregationFunctions[i]->lift(valuePtr, value);
        valuePtr = valuePtr + aggregationFunctions[i]->getSize();
    }
}
}// namespace NES::Runtime::Execution::Operators