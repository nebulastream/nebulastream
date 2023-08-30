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
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getProbeHashMapProxyAntiJoin(void* op) {
    auto handler = static_cast<BatchJoinHandler*>(op);
    return handler->getGlobalHashMap();
}

AntiBatchJoinProbe::AntiBatchJoinProbe(uint64_t operatorHandlerIndex,
                               const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                               const std::vector<PhysicalTypePtr>& keyDataTypes,
                                       const std::vector<Record::RecordFieldIdentifier>& buildFieldIdentifiers,
                                       const std::vector<PhysicalTypePtr>& valueDataTypes,
                                       std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction,
                                       std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider)
    : AbstractBatchJoinProbe(operatorHandlerIndex,
                             keyExpressions,
                             keyDataTypes,
                             buildFieldIdentifiers,
                             valueDataTypes,
                             std::move(hashFunction)),
      maxRecordsPerBuffer(memoryProvider->getMemoryLayoutPtr()->getCapacity()), memoryProvider(std::move(memoryProvider)) {}

void AntiBatchJoinProbe::mark(const Interface::ChainedHashMapRef::EntryRef& entry) {
    entry.getKeyPtr().store(Value<Boolean>(true));
}

bool AntiBatchJoinProbe::isMarked(const Interface::ChainedHashMapRef::EntryRef& entry) {
    return entry.getKeyPtr().load<Boolean>();
}

void AntiBatchJoinProbe::writeEntryIntoRecord(const Interface::ChainedHashMapRef::EntryRef& entry,
                                              NES::Nautilus::Record& record) const {
    // write the key
    auto KeyPtr = entry.getKeyPtr();
    for (size_t i = 0; i < keyDataTypes.size() - 1; i++) {// -1 because the last key is the mark
        auto key = MemRefUtils::loadValue(KeyPtr, keyDataTypes[i]);
        record.write(buildFieldIdentifiers[i], key);
        KeyPtr = KeyPtr + keyDataTypes[i]->size();
    }
    // write the value
    auto valuePtr = entry.getValuePtr();
    for (size_t i = 0; i < valueDataTypes.size(); i++) {
        auto value = MemRefUtils::loadValue(valuePtr, valueDataTypes[i]);
        record.write(buildFieldIdentifiers[i + keyDataTypes.size() - 1], value);
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

    // For now hard-code this
    auto maxRecordsPerBuffer = 1000_u64;

    // For now, we have to create a record buffer here to pass it to the child.
    // This is suboptimal as we do not expect to create a record buffer here.
    // In the best cases, we devise a way to pass the record buffer from the parents open call to this terminated call
    executionCtx.allocateBuffer();
    auto memRef = Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef(0)));
    memRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 2, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto rb = RecordBuffer(memRef);

    // Get the global hash map during the terminate phase
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto state = Nautilus::FunctionCall("getProbeHashMapProxyAntiJoin", getProbeHashMapProxyAntiJoin, globalOperatorHandler);
    auto hashMap = Interface::ChainedHashMapRef(state, keyDataTypes, keySize, valueSize);

    auto outputIndex = Value<UInt64>(0_u64);

    // emit all unseen entries
    for (auto entry : hashMap) {
        if (!isMarked(entry)) {
            auto record = Record();
            writeEntryIntoRecord(entry, record);

            // For now, we emulate the emit operator here
            auto ref = rb.getBuffer();
            memoryProvider->write(outputIndex, ref, record);
            outputIndex = outputIndex + 1_u64;
            // emit buffer if it reached the maximal capacity
            if (outputIndex >= maxRecordsPerBuffer) {
                rb.setNumRecords(outputIndex);
                rb.setWatermarkTs(executionCtx.getWatermarkTs());
                rb.setOriginId(executionCtx.getOriginId());
                rb.setSequenceNr(executionCtx.getSequenceNumber());
                executionCtx.emitBuffer(rb);

                // new record buffer
                memRef = Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef(0)));
                memRef.ref =
                    Nautilus::Tracing::ValueRef(INT32_MAX, 2, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
                rb = RecordBuffer(memRef);
                outputIndex = 0_u64;
            }
        }
    }
}

}// namespace NES::Runtime::Execution::Operators