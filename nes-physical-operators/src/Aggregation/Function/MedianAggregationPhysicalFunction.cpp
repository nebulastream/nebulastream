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
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

MedianAggregationPhysicalFunction::MedianAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
{
}

void MedianAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (inputType.nullable)
    {
        /// SQL-standard: NULL inputs are not part of the median set, so skip writing them. Flip the null flag to
        /// false the first time we see a non-null value.
        if (not value.isNull())
        {
            storeNull(aggregationState, false);

            /// Skipping the first byte (null); the paged vector lives right after it.
            const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{1};
            PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(memArea), tupleLayout);
            pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
        }
    }
    else
    {
        /// Adding the record to the paged vector. We are storing the full record in the paged vector for now.
        const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
        PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(memArea), tupleLayout);
        pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
    }
}

void MedianAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Getting the paged vectors from the aggregation states
    auto memArea1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    auto memArea2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);

    if (inputType.nullable)
    {
        /// SQL-standard: only stay null when both partitions are still empty (no non-null value seen on either side)
        const auto containsNull1 = readNull(aggregationState1);
        const auto containsNull2 = readNull(aggregationState2);
        storeNull(aggregationState1, containsNull1 and containsNull2);

        /// Skipping the first byte (null)
        memArea1 += nautilus::val<uint64_t>{1};
        memArea2 += nautilus::val<uint64_t>{1};
    }

    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider, TupleBuffer* vector1Buffer, const TupleBuffer* vector2Buffer) -> void
        {
            auto vector1 = PagedVector::load(*vector1Buffer);
            const auto vector2 = PagedVector::load(*vector2Buffer);
            vector1.copyPagesFrom(*bufferProvider, vector2);
        },
        pipelineMemoryProvider.bufferProvider,
        memArea1,
        memArea2);
}

Record MedianAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// SQL-standard: if no non-null value was observed, the null flag is still set and the paged vector is empty,
    /// so we return NULL. The early skip below also guards the numberOfEntries > 0 invariant.
    auto containsNull = nautilus::val<bool>{false};
    if (inputType.nullable)
    {
        containsNull = readNull(aggregationState);
    }

    const VarVal zero{nautilus::val<uint64_t>(0), true, true};
    VarVal medianValue = zero.castToType(resultType.type);

    if (!containsNull)
    {
        /// Getting the paged vector from the aggregation state
        const auto pagedVectorBufferPtr = static_cast<nautilus::val<NES::TupleBuffer*>>(
            aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)});
        const auto numberOfEntries = invoke(
            +[](const TupleBuffer* pagedVectorBuffer)
            {
                const auto pagedVector = PagedVector::load(*pagedVectorBuffer);
                const auto numberOfEntriesVal = pagedVector.getTotalNumberOfRecords();
                INVARIANT(numberOfEntriesVal > 0, "The number of entries in the paged vector must be greater than 0");
                return numberOfEntriesVal;
            },
            pagedVectorBufferPtr);

        /// Iterating in two nested loops over all the records in the paged vector to get the median.
        /// We pick a candidate and then count for each item, if the candidate is smaller and also if the candidate is less than the item.
        const nautilus::val<int64_t> medianPos1 = (numberOfEntries - 1) / 2;
        const nautilus::val<int64_t> medianPos2 = numberOfEntries / 2;
        nautilus::val<uint64_t> medianItemPos1 = 0;
        nautilus::val<uint64_t> medianItemPos2 = 0;
        nautilus::val<bool> medianFound1(false);
        nautilus::val<bool> medianFound2(false);


        /// Picking a candidate and counting how many items are smaller or equal to the candidate.
        /// Iterator-based scan: the page lookup is amortized per page-crossing instead of paid per access,
        /// which matters here because the outer/inner pair is O(N^2) over the paged vector.
        const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVectorBufferPtr), tupleLayout);
        const auto pagedVectorEnd = pagedVectorRef.end();
        nautilus::val<uint64_t> candidatePos = 0;
        for (auto candidateIt = pagedVectorRef.begin(); candidateIt != pagedVectorEnd; ++candidateIt)
        {
            nautilus::val<int64_t> countLessThan = 0;
            nautilus::val<int64_t> countEqual = 0;
            const auto candidateRecord = *candidateIt;
            const auto candidateValue = inputFunction.execute(candidateRecord, pipelineMemoryProvider.arena);

            /// Counting how many items are smaller or equal for the current candidate
            for (const auto& itemRecord : pagedVectorRef)
            {
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
                medianItemPos1 = candidatePos;
                medianFound1 = true;
            }
            if (not medianFound2 && countLessThan <= medianPos2 && medianPos2 < countLessThan + countEqual)
            {
                medianItemPos2 = candidatePos;
                medianFound2 = true;
            }
            candidatePos = candidatePos + 1;
        }

        if (medianFound1 and medianFound2)
        {
            /// Calculating the median value. Regardless if the number of entries is odd or even, we calculate the median as the average of the two middle values.
            /// For even numbers of entries, this is its natural definition.
            /// For odd numbers of entries, both positions are pointing to the same item and thus, we are calculating the average of the same item, which is the item itself.
            const auto medianRecord1 = pagedVectorRef.at(medianItemPos1);
            const auto medianRecord2 = pagedVectorRef.at(medianItemPos2);

            const auto medianValue1 = inputFunction.execute(medianRecord1, pipelineMemoryProvider.arena);
            const auto medianValue2 = inputFunction.execute(medianRecord2, pipelineMemoryProvider.arena);
            const VarVal two = nautilus::val<uint64_t>(2);
            medianValue
                = (medianValue1.castToType(resultType.type) + medianValue2.castToType(resultType.type)) / two.castToType(resultType.type);
        }
    }


    /// Adding the median to the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, medianValue);

    return resultRecord;
}

void MedianAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = tupleLayout->getSchema().getAggregate().sizeWithNull;
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea, AbstractBufferProvider* bufferProvider, uint64_t tupleSize) -> void
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): aggregation state stores a TupleBuffer at this slot.
            auto* pagedVectorBufferMemArea = reinterpret_cast<TupleBuffer*>(pagedVectorMemArea);
            if (auto pagedVectorBuffer = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                /// initialize paged vector buffer
                PagedVector::init(pagedVectorBuffer.value(), bufferProvider->getBufferSize(), tupleSize);
                /// @warning: this will be refactored again during the ChainedHashMap refactor
                new (pagedVectorBufferMemArea) TupleBuffer(pagedVectorBuffer.value());
                return;
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for median aggregation paged vector!");
        },
        aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)},
        pipelineMemoryProvider.bufferProvider,
        tupleSize);

    if (inputType.nullable)
    {
        /// Initialize the null flag to "no value seen yet" so all-NULL windows correctly emit NULL
        storeNull(aggregationState, true);
    }
}

void MedianAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVectorBuffer
                = reinterpret_cast<TupleBuffer*>(pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVectorBuffer->~TupleBuffer();
        },
        aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)});
}

size_t MedianAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// ContainsNullValues (1B) + Pagedvector
    return static_cast<uint64_t>(inputType.nullable) + sizeof(TupleBuffer);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMedianAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "Tuple layout paged vector not set");
    return std::make_shared<MedianAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.tupleLayout.value());
}

}
