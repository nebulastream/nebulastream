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
#include <Execution/Operators/Relational/Join/AbstractBatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Join/SemiBatchJoinProbe.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

SemiBatchJoinProbe::SemiBatchJoinProbe(uint64_t operatorHandlerIndex,
                               const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                               const std::vector<PhysicalTypePtr>& keyDataTypes,
                                       const std::vector<Record::RecordFieldIdentifier>& buildFieldIdentifiers,
                                       const std::vector<PhysicalTypePtr>& valueDataTypes,
                                       std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction)
    : AbstractBatchJoinProbe(operatorHandlerIndex,
                             keyExpressions,
                             keyDataTypes,
                             buildFieldIdentifiers,
                             valueDataTypes,
                             std::move(hashFunction)) {}

void SemiBatchJoinProbe::mark(Interface::ChainedHashMapRef::EntryRef entry) const {
    auto mark = Value<UInt8>(1_u8);
    auto keyPtr = entry.getKeyPtr();
    for (size_t i = 0; i < keyDataTypes.size() - 1; i++) {
        keyPtr = keyPtr + keyDataTypes[i]->size();
    }
    keyPtr.store(mark);
}

Interface::ChainedHashMapRef::KeyEntryIterator SemiBatchJoinProbe::findMatches(NES::Runtime::Execution::ExecutionContext& ctx,
                                                                               NES::Nautilus::Record& record) const {
    // 1. derive key values
    std::vector<Value<>> keyValues;
    for (const auto& exp : keyExpressions) {
        keyValues.emplace_back(exp->execute(record));
    }

    // 2. add mark
    auto mark = Value<UInt8>(0_u8);
    keyValues.emplace_back(mark);

    // 3. load the reference to the global hash map.
    auto state = reinterpret_cast<LocalJoinProbeState*>(ctx.getLocalState(this));
    auto& hashMap = state->hashMap;

    // 4. calculate hash
    auto hash = hashFunction->calculate(keyValues);

    // 5. lookup the key in the hashmap
    return hashMap.findAll(hash, keyValues);
}

void SemiBatchJoinProbe::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // Get matches iterator
    // In contrast to the inner join, we do not emit the probe side tuples.
    // But we need to make sure that we do not emit the same tuple twice.
    auto entry = findMatches(ctx, record);

    // Check if a match was found
    for (; entry != nullptr; ++entry) {
        mark(*entry);
        this->child->execute(ctx, record);
    }
}

}// namespace NES::Runtime::Execution::Operators