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
#include <Execution/Operators/Relational/Join/AbstractBatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Join/AntiBatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

AntiBatchJoinProbe::AntiBatchJoinProbe(uint64_t operatorHandlerIndex,
                               const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                               const std::vector<PhysicalTypePtr>& keyDataTypes,
                               const std::vector<Record::RecordFieldIdentifier>& probeFieldIdentifiers,
                               const std::vector<PhysicalTypePtr>& valueDataTypes,
                                       std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction,
                                       const std::vector<Record::RecordFieldIdentifier>& resultRecordFieldIdentifiers)
    : AbstractBatchJoinProbe(operatorHandlerIndex,
                             keyExpressions,
                             keyDataTypes,
                             probeFieldIdentifiers,
                             valueDataTypes,
                             std::move(hashFunction)),
      resultRecordFieldIdentifiers(resultRecordFieldIdentifiers) {}

void AntiBatchJoinProbe::mark(const Interface::ChainedHashMapRef::EntryRef& entry) {
    entry.getKeyPtr().store(Value<Boolean>(true));
}

bool AntiBatchJoinProbe::isMarked(const Interface::ChainedHashMapRef::EntryRef& entry) {
    return entry.getKeyPtr().load<Boolean>();
}

void AntiBatchJoinProbe::writeEntryIntoRecord(const Interface::ChainedHashMapRef::EntryRef& entry,
                                              NES::Nautilus::Record& record) const {
    auto keyPtr = entry.getKeyPtr();
    // skip the mark boolean
    keyPtr = keyPtr + 1;
    // write the key
    auto KeyPtr = entry.getKeyPtr();
    for (size_t i = 0; i < keyDataTypes.size(); i++) {
        auto key = MemRefUtils::loadValue(KeyPtr, keyDataTypes[i]);
        record.write(resultRecordFieldIdentifiers[i], key);
        KeyPtr = KeyPtr + keyDataTypes[i]->size();
    }
    // write the value
    auto valuePtr = entry.getValuePtr();
    for (size_t i = 0; i < valueDataTypes.size(); i++) {
        auto value = MemRefUtils::loadValue(valuePtr, valueDataTypes[i]);
        record.write(resultRecordFieldIdentifiers[i + keyDataTypes.size()], value);
        valuePtr = valuePtr + valueDataTypes[i]->size();
    }
}

void AntiBatchJoinProbe::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // Get matches iterator
    auto entry = findMatches(ctx, record);

    // Mark all matches as seen
    for (; entry != nullptr; ++entry) {
        mark(*entry);
    }
}

void AntiBatchJoinProbe::terminate(NES::Runtime::Execution::ExecutionContext& executionCtx) const {
    // for now, this execution is very slow as we do not create code for the termination

    // emit all unseen entries
    auto state = reinterpret_cast<LocalJoinProbeState*>(executionCtx.getLocalState(this));
    auto& hashMap = state->hashMap;
    for (auto entry : hashMap) {
        if (!isMarked(entry)) {
            auto record = Record();
            writeEntryIntoRecord(entry, record);
            child->execute(executionCtx, record);
        }
    }
    child->terminate(executionCtx);
}

}// namespace NES::Runtime::Execution::Operators