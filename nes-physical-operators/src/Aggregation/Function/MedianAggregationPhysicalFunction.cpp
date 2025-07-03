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

#include <Aggregation/Function/MedianAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

MedianAggregationPhysicalFunction::MedianAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , memProviderPagedVector(std::move(memProviderPagedVector))
{
}

void MedianAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, ExecutionContext& executionContext, const Record& record)
{
    /// Adding the record to the paged vector. We are storing the full record in the paged vector for now.
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(memArea, memProviderPagedVector);
    pagedVectorRef.writeRecord(record, executionContext.pipelineMemoryProvider.bufferProvider);
}

void MedianAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Getting the paged vectors from the aggregation states
    const auto memArea1 = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState2);

    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](Interface::PagedVector* vector1, const Interface::PagedVector* vector2) -> void { vector1->copyFrom(*vector2); },
        memArea1,
        memArea2);
}

Nautilus::Record MedianAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Getting the paged vector from the aggregation state
    const auto pagedVectorPtr = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector);
    const auto allFieldNames = memProviderPagedVector->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Interface::PagedVector* pagedVector)
        {
            const auto numberOfEntriesVal = pagedVector->getTotalNumberOfEntries();
            INVARIANT(numberOfEntriesVal > 0, "The number of entries in the paged vector must be greater than 0");
            return numberOfEntriesVal;
        },
        pagedVectorPtr);

    /// Iterating in two nested loops over all the records in the paged vector to get the median.
    /// We pick a candidate and then count for each item, if the candidate is smaller and also if the candidate is less than the item.
    const nautilus::val<int64_t> medianPos1 = (numberOfEntries - 1) / 2;
    const nautilus::val<int64_t> medianPos2 = numberOfEntries / 2;
    nautilus::val<uint64_t> medianItemPos1 = 0;
    nautilus::val<uint64_t> medianItemPos2 = 0;
    nautilus::val<bool> medianFound1(false);
    nautilus::val<bool> medianFound2(false);


    /// Picking a candidate and counting how many items are smaller or equal to the candidate
    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        nautilus::val<int64_t> countLessThan = 0;
        nautilus::val<int64_t> countEqual = 0;
        const auto candidateRecord = *candidateIt;
        const auto candidateValue = inputFunction.execute(candidateRecord, pipelineMemoryProvider.arena);

        /// Counting how many items are smaller or equal for the current candidate
        for (auto itemIt = pagedVectorRef.begin(allFieldNames); itemIt != endIt; ++itemIt)
        {
            const auto itemRecord = *itemIt;
            const auto itemValue = inputFunction.execute(itemRecord, pipelineMemoryProvider.arena);
            if (itemValue < candidateValue)
            {
                countLessThan = countLessThan + 1;
            }
            if (itemValue == candidateValue)
            {
                countEqual = countEqual + 1;
            }
        }

        /// Checking if the current candidate is the median, and if so, storing the position of the median
        /// The current candidate is the median if the number of items that are smaller or equal to the candidate is larger than the median position
        if (not medianFound1 && countLessThan <= medianPos1 && medianPos1 < countLessThan + countEqual)
        {
            medianItemPos1 = candidateIt - pagedVectorRef.begin(allFieldNames);
            medianFound1 = true;
        }
        if (not medianFound2 && countLessThan <= medianPos2 && medianPos2 < countLessThan + countEqual)
        {
            medianItemPos2 = candidateIt - pagedVectorRef.begin(allFieldNames);
            medianFound2 = true;
        }
    }

    /// Setting the default median value. If a median was found, the value will be overwritten
    const VarVal zero(nautilus::val<uint64_t>(0));
    VarVal medianValue = zero.castToType(resultType.type);
    if (medianFound1 and medianFound2)
    {
        /// Calculating the median value. Regardless if the number of entries is odd or even, we calculate the median as the average of the two middle values.
        /// For even numbers of entries, this is its natural definition.
        /// For odd numbers of entries, both positions are pointing to the same item and thus, we are calculating the average of the same item, which is the item itself.
        const auto medianRecord1 = pagedVectorRef.readRecord(medianItemPos1, allFieldNames);
        const auto medianRecord2 = pagedVectorRef.readRecord(medianItemPos2, allFieldNames);

        const auto medianValue1 = inputFunction.execute(medianRecord1, pipelineMemoryProvider.arena);
        const auto medianValue2 = inputFunction.execute(medianRecord2, pipelineMemoryProvider.arena);
        const VarVal two = nautilus::val<uint64_t>(2);
        medianValue
            = (medianValue1.castToType(resultType.type) + medianValue2.castToType(resultType.type)) / two.castToType(resultType.type);
    }

    /// Adding the median to the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, medianValue);

    return resultRecord;
}

void MedianAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
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

void MedianAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector
                = reinterpret_cast<Interface::PagedVector*>(pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}

size_t MedianAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Interface::PagedVector);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMedianAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.memProviderPagedVector.has_value(), "Memory provider paged vector not set");
    return std::make_shared<MedianAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.memProviderPagedVector.value());
}

}
