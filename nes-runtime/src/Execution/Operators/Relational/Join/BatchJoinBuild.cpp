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
#include <Execution/Operators/Relational/Join/BatchJoinBuild.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getPagedVectorProxy(void* op, uint64_t workerId) {
    auto handler = static_cast<BatchJoinHandler*>(op);
    return handler->getThreadLocalState(workerId);
}

void setupJoinBuildHandler(void* ss, void* ctx, uint64_t entrySize, uint64_t keySize, uint64_t valueSize) {
    auto handler = static_cast<BatchJoinHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, entrySize, keySize, valueSize);
}

class LocalJoinBuildState : public Operators::OperatorState {
  public:
    explicit LocalJoinBuildState(Interface::PagedVectorRef pagedVector) : pagedVector(std::move(pagedVector)){};

    Interface::PagedVectorRef pagedVector;
};

BatchJoinBuild::BatchJoinBuild(uint64_t operatorHandlerIndex,
                               const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                               const std::vector<PhysicalTypePtr>& keyDataTypes,
                               const std::vector<Expressions::ExpressionPtr>& valueExpressions,
                               const std::vector<PhysicalTypePtr>& valueDataTypes,
                               std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction)
    : operatorHandlerIndex(operatorHandlerIndex), keyExpressions(keyExpressions), keyDataTypes(keyDataTypes),
      valueExpressions(valueExpressions), valueDataTypes(valueDataTypes), hashFunction(std::move(hashFunction)), keySize(0),
      valueSize(0) {

    for (auto& keyType : keyDataTypes) {
        keySize = keySize + keyType->size();
    }
    for (auto& valueType : valueDataTypes) {
        valueSize = valueSize + valueType->size();
    }
}

void BatchJoinBuild::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto entrySize = keySize + valueSize + /*next ptr*/ sizeof(int64_t) + /*hash*/ sizeof(int64_t);
    Nautilus::FunctionCall("setupJoinBuildHandler",
                           setupJoinBuildHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           Value<UInt64>(entrySize),
                           Value<UInt64>(keySize),
                           Value<UInt64>(valueSize));
}

void BatchJoinBuild::open(ExecutionContext& ctx, RecordBuffer&) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local pagedVector and store it in the local state.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local pagedVector according to the worker id.
    auto state = Nautilus::FunctionCall("getPagedVectorProxy", getPagedVectorProxy, globalOperatorHandler, ctx.getWorkerId());
    auto entrySize = keySize + valueSize + /*next ptr*/ sizeof(int64_t) + /*hash*/ sizeof(int64_t);
    auto pagedVector = Interface::PagedVectorRef(state, entrySize);
    // 3. store the reference to the pagedVector in the local operator state.
    auto sliceStoreState = std::make_unique<LocalJoinBuildState>(pagedVector);
    ctx.setLocalOperatorState(this, std::move(sliceStoreState));
}

void BatchJoinBuild::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // 1. derive key values
    std::vector<Value<>> keyValues;
    for (const auto& exp : keyExpressions) {
        keyValues.emplace_back(exp->execute(record));
    }

    // 3. load the reference to the pagedVector.
    auto state = reinterpret_cast<LocalJoinBuildState*>(ctx.getLocalState(this));
    auto& pagedVector = state->pagedVector;

    // 4. store entry in the pagedVector

    // 4.1 calculate hash
    auto hash = hashFunction->calculate(keyValues);

    // 4.2 create entry and store it in pagedVector
    auto entry = pagedVector.allocateEntry();
    // 4.3a store hash value at next offset
    auto hashPtr = (entry + (uint64_t) sizeof(int64_t)).as<MemRef>();
    hashPtr.store(hash);

    // 4.3b store key values
    auto keyPtr = (hashPtr + (uint64_t) sizeof(int64_t)).as<MemRef>();
    storeKeys(keyValues, keyPtr);

    // 4.3c store value values
    std::vector<Value<>> values;
    for (const auto& exp : valueExpressions) {
        values.emplace_back(exp->execute(record));
    }
    auto valuePtr = (keyPtr + keySize).as<MemRef>();
    storeValues(values, valuePtr);
}

void BatchJoinBuild::storeKeys(std::vector<Value<>> keys, Value<MemRef> keyPtr) const {
    for (size_t i = 0; i < keys.size(); i++) {
        auto& key = keys[i];
        keyPtr.store(key);
        keyPtr = keyPtr + keyDataTypes[i]->size();
    }
}

void BatchJoinBuild::storeValues(std::vector<Value<>> values, Value<MemRef> valuePtr) const {
    for (size_t i = 0; i < values.size(); i++) {
        auto& value = values[i];
        valuePtr.store(value);
        valuePtr = valuePtr + valueDataTypes[i]->size();
    }
}

}// namespace NES::Runtime::Execution::Operators