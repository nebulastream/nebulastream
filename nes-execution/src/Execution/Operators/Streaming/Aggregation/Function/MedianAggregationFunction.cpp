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

#include <Execution/Operators/Streaming/Aggregation/Function/MedianAggregationFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include <nautilus/function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Aggregation
{

MedianAggregationFunction::MedianAggregationFunction(
    std::shared_ptr<PhysicalType> inputType,
    std::shared_ptr<PhysicalType> resultType,
    std::unique_ptr<Functions::Function> inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , memProviderPagedVector(std::move(memProviderPagedVector))
{
}

void MedianAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record)
{
    /// Reading the value from the record
    if (const auto value = inputFunction->execute(record, pipelineMemoryProvider.arena); inputType->type->nullable && value.isNull())
    {
        /// If the value is null, we do not add the record to the state.
        return;
    }

    /// Adding the record to the paged vector. We are storing the full record in the paged vector for now.
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(memArea, memProviderPagedVector, pipelineMemoryProvider.bufferProvider);
    pagedVectorRef.writeRecord(record);
}

void MedianAggregationFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Getting the paged vectors from the aggregation states
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2) -> void
        { vector1->copyFrom(*vector2); },
        memArea1,
        memArea2);
}

Nautilus::Record
MedianAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Getting the paged vector from the aggregation state
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector, pipelineMemoryProvider.bufferProvider);
    const auto allFieldNames = memProviderPagedVector->getMemoryLayout()->getSchema()->getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {
            const auto numberOfEntriesVal = pagedVector->getTotalNumberOfEntries();
            return numberOfEntriesVal;
        },
        pagedVectorPtr);

    if (numberOfEntries == 0)
    {
        /// If there are no entries, we return the result record with the value 0
        const Nautilus::VarVal zero = nautilus::val<uint64_t>(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, zero.castToType(resultType));
        return resultRecord;
    }

    /// Unless there are no entries, we return the result record with the median value
    /// Iterating in two nested loops over all the records in the paged vector to get the median.
    /// We pick a candidate and then count for each item, if the candidate is smaller and also if the candidate is less than the item.
    const nautilus::val<int64_t> medianPos1 = (numberOfEntries - 1) / 2;
    const nautilus::val<int64_t> medianPos2 = numberOfEntries / 2;
    nautilus::val<uint64_t> medianItemPos1 = 0;
    nautilus::val<uint64_t> medianItemPos2 = 0;
    nautilus::val<bool> medianFound1 = false;
    nautilus::val<bool> medianFound2 = false;


    /// Picking a candidate and counting how many items are smaller or equal to the candidate
    for (nautilus::val<uint64_t> candidateCnt = 0; candidateCnt < numberOfEntries; ++candidateCnt)
    {
        nautilus::val<int64_t> countLessThan = 0;
        nautilus::val<int64_t> countEqual = 0;
        const auto candidateRecord = pagedVectorRef.readRecord(candidateCnt, allFieldNames);
        const auto candidateValue = inputFunction->execute(candidateRecord, pipelineMemoryProvider.arena);

        /// Counting how many items are smaller or equal for the current candidate
        for (nautilus::val<uint64_t> itemCnt = 0; itemCnt < numberOfEntries; ++itemCnt)
        {
            const auto itemRecord = pagedVectorRef.readRecord(itemCnt, allFieldNames);
            const auto itemValue = inputFunction->execute(itemRecord, pipelineMemoryProvider.arena);
            const auto lessThanIncrement = (itemValue < candidateValue) * nautilus::val<int64_t>(1);
            const auto equalIncrement = (itemValue == candidateValue) * nautilus::val<int64_t>(1);
            countLessThan = countLessThan + (lessThanIncrement.getRawValueAs<nautilus::val<int64_t>>());
            countEqual = countEqual + (equalIncrement.getRawValueAs<nautilus::val<int64_t>>());
        }

        /// Checking if the current candidate is the median, and if so, storing the position of the median
        /// The current candidate is the median if the number of items that are smaller or equal to the candidate is larger than the median position
        if (not medianFound1 && countLessThan <= medianPos1 && medianPos1 < countLessThan + countEqual)
        {
            medianItemPos1 = candidateCnt;
            medianFound1 = true;
        }
        if (not medianFound2 && countLessThan <= medianPos2 && medianPos2 < countLessThan + countEqual)
        {
            medianItemPos2 = candidateCnt;
            medianFound2 = true;
        }
    }

    /// Calculating the median value. Regardless if the number of entries is odd or even, we calculate the median as the average of the two middle values.
    /// For even numbers of entries, this is its natural definition.
    /// For odd numbers of entries, both positions are pointing to the same item and thus, we are calculating the average of the same item, which is the item itself.
    const auto medianRecord1 = pagedVectorRef.readRecord(medianItemPos1, allFieldNames);
    const auto medianRecord2 = pagedVectorRef.readRecord(medianItemPos2, allFieldNames);

    const auto medianValue1 = inputFunction->execute(medianRecord1, pipelineMemoryProvider.arena);
    const auto medianValue2 = inputFunction->execute(medianRecord2, pipelineMemoryProvider.arena);
    const Nautilus::VarVal two = nautilus::val<uint64_t>(2);
    const auto medianValue = (medianValue1.castToType(resultType) + medianValue2.castToType(resultType)) / two.castToType(resultType);

    /// Adding the median to the result record
    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, medianValue);
    return resultRecord;
}

void MedianAggregationFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t MedianAggregationFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::PagedVector);
}

}
