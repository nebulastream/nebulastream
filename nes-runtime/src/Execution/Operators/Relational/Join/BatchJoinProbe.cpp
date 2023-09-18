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
#include <Execution/Operators/Relational/Join/BatchJoinHandler.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinProbe.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getProbeHashMapProxy(void* op) {
    auto handler = static_cast<BatchJoinHandler*>(op);
    return handler->getGlobalHashMap();
}

class LocalJoinProbeState : public Operators::OperatorState {
  public:
    explicit LocalJoinProbeState(Interface::ChainedHashMapRef hashMap) : hashMap(std::move(hashMap)){};

    Interface::ChainedHashMapRef hashMap;
};

BatchJoinProbe::BatchJoinProbe(uint64_t operatorHandlerIndex,
                               const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                               const std::vector<PhysicalTypePtr>& keyDataTypes,
                               const std::vector<Record::RecordFieldIdentifier>& probeFieldIdentifiers,
                               const std::vector<PhysicalTypePtr>& valueDataTypes,
                               std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction)
    : operatorHandlerIndex(operatorHandlerIndex), keyExpressions(keyExpressions), keyDataTypes(keyDataTypes),
      probeFieldIdentifiers(probeFieldIdentifiers), valueDataTypes(valueDataTypes), hashFunction(std::move(hashFunction)),
      keySize(0), valueSize(0) {

    for (auto& keyType : keyDataTypes) {
        keySize = keySize + keyType->size();
    }
    for (auto& valueType : valueDataTypes) {
        valueSize = valueSize + valueType->size();
    }
}

void BatchJoinProbe::setup(ExecutionContext& executionCtx) const { ExecutableOperator::setup(executionCtx); }

void BatchJoinProbe::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the global probe hash table.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the hash map
    auto state = Nautilus::FunctionCall("getProbeHashMapProxy", getProbeHashMapProxy, globalOperatorHandler);
    auto entrySize = keySize + valueSize + /*next ptr*/ sizeof(int64_t) + /*hash*/ sizeof(int64_t);
    auto chainedHM = Interface::ChainedHashMapRef(state, keyDataTypes, keySize, valueSize);
    // 3. store the reference to the hash map in the local operator state.
    auto globalHashMap = std::make_unique<LocalJoinProbeState>(chainedHM);
    ctx.setLocalOperatorState(this, std::move(globalHashMap));
    ExecutableOperator::open(ctx, rb);
}

void BatchJoinProbe::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // 1. derive key values
    std::vector<Value<>> keyValues;
    for (const auto& exp : keyExpressions) {
        keyValues.emplace_back(exp->execute(record));
    }

    // 3. load the reference to the global hash map.
    auto state = reinterpret_cast<LocalJoinProbeState*>(ctx.getLocalState(this));
    auto& hashMap = state->hashMap;

    // 4. calculate hash
    auto hash = hashFunction->calculate(keyValues);

    // 5. lookup the key in the hashmap
    auto entry = hashMap.find(hash, keyValues);

    // 6. check if join partner was found
    if (entry != nullptr) {
        // 7. Create result record
        // 7.1 load values from the probe side and store them in the record.
        auto valuePtr = entry.getValuePtr();
        for (size_t i = 0; i < probeFieldIdentifiers.size(); i++) {
            auto value = MemRefUtils::loadValue(valuePtr, valueDataTypes[i]);
            record.write(probeFieldIdentifiers[i], value);
            valuePtr = valuePtr + valueDataTypes[i]->size();
        }
        this->child->execute(ctx, record);
    }
}

}// namespace NES::Runtime::Execution::Operators