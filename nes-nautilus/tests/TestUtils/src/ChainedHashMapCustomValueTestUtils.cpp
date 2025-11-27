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
#include <vector>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ChainedHashMapTestUtils.hpp>
#include <Engine.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES::TestUtils
{

nautilus::engine::CallableFunction<void, TupleBuffer*, TupleBuffer*, uint64_t, AbstractBufferProvider*, HashMap*>
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
                    nautilus::invoke(
                        +[](int8_t* pagedVectorMemArea)
                        {
                            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
                            auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea);
                            new (pagedVector) PagedVector();
                        },
                        ref.getValueMemArea());
                },
                bufferManagerVal);

            const ChainedHashMapRef::ChainedEntryRef ref(foundEntry, hashMapVal, fieldKeys, fieldValues);
            const PagedVectorRef pagedVectorRef(ref.getValueMemArea(), inputBufferRef);
            const RecordBuffer recordBufferValue(bufferValue);
            for (nautilus::val<uint64_t> idxValues = 0; idxValues < recordBufferValue.getNumRecords(); idxValues = idxValues + 1)
            {
                auto recordValue = inputBufferRef->readRecord(projectionAllFields, recordBufferValue, idxValues);
                pagedVectorRef.writeRecord(recordValue, bufferManagerVal);
            }
        }));
}

nautilus::engine::CallableFunction<void, TupleBuffer*, uint64_t, TupleBuffer*, AbstractBufferProvider*, HashMap*>
ChainedHashMapCustomValueTestUtils::compileWriteAllRecordsIntoOutputBuffer(
    const std::vector<Record::RecordFieldIdentifier>& projectionAllFields) const
{
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    return nautilusEngine->registerFunction(std::function(
        [this, projectionAllFields](
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
            const PagedVectorRef pagedVectorRef(ref.getValueMemArea(), inputBufferRef);
            nautilus::val<uint64_t> recordBufferIndex = 0;
            for (auto it = pagedVectorRef.begin(projectionAllFields); it != pagedVectorRef.end(projectionAllFields); ++it)
            {
                const auto record = *it;
                inputBufferRef->writeRecord(recordBufferIndex, recordBufferOutput, record, bufferManagerVal);
                recordBufferIndex = recordBufferIndex + 1;
                recordBufferOutput.setNumRecords(recordBufferIndex);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)
}

}
