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

#include <ChainedHashMapCustomValueTestUtils.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ChainedHashMapTestUtils.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES::TestUtils
{

nautilus::engine::CompiledFunction<void(TupleBuffer*, TupleBuffer*, uint64_t, AbstractBufferProvider*, HashMap*)>
ChainedHashMapCustomValueTestUtils::compileFindAndInsertIntoPagedVector(
    const std::vector<Record::RecordFieldIdentifier>& projectionAllFields) const
{
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// Resharper disable once CppPassValueParameterByConstReference
    return nautilusEngine->registerFunction(std::function(
        [this, projectionAllFields](
            nautilus::val<TupleBuffer*> bufferKey,
            nautilus::val<TupleBuffer*> bufferValue,
            nautilus::val<uint64_t> keyPositionVal,
            nautilus::val<AbstractBufferProvider*> bufferManagerVal,
            nautilus::val<HashMap*> hashMapVal)
        {
            ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
            const RecordBuffer recordBufferKey(bufferKey);
            auto recordKey = inputBufferRef->readRecord(projectionKeys, recordBufferKey, keyPositionVal);

            auto foundEntry = hashMapRef.findOrCreateEntry(
                recordKey,
                *getMurMurHashFunction(),
                [&](const nautilus::val<AbstractHashMapEntry*>& entry)
                {
                    const ChainedHashMapRef::ChainedEntryRef ref(entry, hashMapVal, fieldKeys, fieldValues);
                    const nautilus::val<uint64_t> tupleSize = tupleLayout->getSchema().getSizeInBytes();
                    nautilus::invoke(
                        +[](AbstractBufferProvider* bufferManager, TupleBuffer* pagedVectorMemArea, const uint64_t tupleSize)
                        {
                            if (auto pagedVectorBuffer = bufferManager->getUnpooledBuffer(PagedVector::getMainBufferSize()))
                            {
                                /// initialize paged vector buffer
                                PagedVector::init(pagedVectorBuffer.value(), bufferManager->getMaxBufferSize(), tupleSize);
                                /// @warning: this will be refactored again during the ChainedHashMap refactor
                                new (pagedVectorMemArea) TupleBuffer(pagedVectorBuffer.value());
                                return;
                            }
                            throw BufferAllocationFailure("No unpooled TupleBuffer available for chained hash map entry's paged vector!");
                        },
                        bufferManagerVal,
                        ref.getValueMemArea(),
                        tupleSize);
                },
                bufferManagerVal);

            const ChainedHashMapRef::ChainedEntryRef ref(foundEntry, hashMapVal, fieldKeys, fieldValues);
            PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(ref.getValueMemArea()), tupleLayout);
            const RecordBuffer recordBufferValue(bufferValue);
            for (nautilus::val<uint64_t> idxValues = 0; idxValues < recordBufferValue.getNumRecords(); idxValues = idxValues + 1)
            {
                auto recordValue = inputBufferRef->readRecord(projectionAllFields, recordBufferValue, idxValues);
                pagedVectorRef.pushBack(recordValue, bufferManagerVal);
            }
        }));
}

nautilus::engine::CompiledFunction<void(TupleBuffer*, uint64_t, TupleBuffer*, AbstractBufferProvider*, HashMap*)>
ChainedHashMapCustomValueTestUtils::compileWriteAllRecordsIntoOutputBuffer(
    const std::vector<Record::RecordFieldIdentifier>& projectionAllFields, const std::shared_ptr<TupleBufferRef>& tupleBufferRef) const
{
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    return nautilusEngine->registerFunction(std::function(
        [this, projectionAllFields, tupleBufferRef](
            nautilus::val<TupleBuffer*> keyBufferRef,
            nautilus::val<uint64_t> keyPositionVal,
            nautilus::val<TupleBuffer*> outputBufferRef,
            nautilus::val<AbstractBufferProvider*> bufferManagerVal,
            nautilus::val<HashMap*> hashMapVal)
        {
            ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, {}, entriesPerPage, entrySize);
            const RecordBuffer recordBufferKey(keyBufferRef);
            RecordBuffer recordBufferOutput(outputBufferRef);
            const auto recordKey = inputBufferRef->readRecord(projectionKeys, recordBufferKey, keyPositionVal);
            const auto foundEntry
                = hashMapRef.findOrCreateEntry(recordKey, *getMurMurHashFunction(), ASSERT_VIOLATION_FOR_ON_INSERT, bufferManagerVal);

            const ChainedHashMapRef::ChainedEntryRef ref(foundEntry, hashMapVal, fieldKeys, fieldValues);
            const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(ref.getValueMemArea()), tupleLayout);
            nautilus::val<uint64_t> recordBufferIndex = 0;
            for (auto it = pagedVectorRef.begin(); it != pagedVectorRef.end(); ++it)
            {
                const auto record = *it;
                tupleBufferRef->writeRecord(recordBufferIndex, recordBufferOutput, record, bufferManagerVal);
                recordBufferIndex = recordBufferIndex + 1;
                recordBufferOutput.setNumRecords(recordBufferIndex);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)
}

}
