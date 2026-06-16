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
#include <DataTypes/DataTypesUtil.hpp>
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
    const AggregationStateRef& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    auto memArea = aggregationState.data();
    if (inputType.nullable)
    {
        /// Reading the current null value and combining it with the one of the record
        const auto oldContainsNull = aggregationState.readNull();
        aggregationState.storeNull(value.isNull() or oldContainsNull);

        /// Skipping the first byte (null)
        memArea += nautilus::val<uint64_t>{1};
    }

    /// Load the paged vector buffer from the parent via the stored child index
    auto pagedVecBuffer = aggregationState.getBuffer().getChild(static_cast<nautilus::val<size_t>>(readValueFromMemRef<uint32_t>(memArea)));
    PagedVectorRef pagedVectorRef{std::move(pagedVecBuffer), tupleLayout};
    pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
}

void MedianAggregationPhysicalFunction::combine(
    const AggregationStateRef& aggregationState1,
    const AggregationStateRef& aggregationState2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    auto memArea1 = aggregationState1.data();
    auto memArea2 = aggregationState2.data();

    if (inputType.nullable)
    {
        /// Combining the null values
        const auto containsNull1 = aggregationState1.readNull();
        const auto containsNull2 = aggregationState2.readNull();
        aggregationState1.storeNull(containsNull1 or containsNull2);

        /// Skipping the first byte (null)
        memArea1 += nautilus::val<uint64_t>{1};
        memArea2 += nautilus::val<uint64_t>{1};
    }

    /// Load both paged vector buffers via their stored child indices, then copy pages from source (vec2) into destination (vec1)
    auto vec1Buf = aggregationState1.getBuffer().getChild(static_cast<nautilus::val<size_t>>(readValueFromMemRef<uint32_t>(memArea1)));
    auto vec2Buf = aggregationState2.getBuffer().getChild(static_cast<nautilus::val<size_t>>(readValueFromMemRef<uint32_t>(memArea2)));
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider, TupleBuffer* vec1, TupleBuffer* vec2) -> void
        {
            auto vector1 = PagedVector::load(*vec1);
            const auto vector2 = PagedVector::load(*vec2);
            vector1.copyPagesFrom(*bufferProvider, vector2);
        },
        pipelineMemoryProvider.bufferProvider,
        vec1Buf.asArg(),
        vec2Buf.asArg());
}

Record MedianAggregationPhysicalFunction::lower(const AggregationStateRef& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// If it contains null values, we simply return a null value
    auto containsNull = nautilus::val<bool>{false};
    if (inputType.nullable)
    {
        containsNull = aggregationState.readNull();
    }

    const VarVal zero{nautilus::val<uint64_t>(0), true, true};
    VarVal medianValue = zero.castToType(resultType.type);

    if (!containsNull)
    {
        /// Load the paged vector buffer from the parent via its stored child index
        auto memArea = aggregationState.data();
        if (inputType.nullable)
        {
            memArea += nautilus::val<uint64_t>{1};
        }
        auto pagedVecBuffer
            = aggregationState.getBuffer().getChild(static_cast<nautilus::val<size_t>>(readValueFromMemRef<uint32_t>(memArea)));

        const auto numberOfEntries = invoke(
            +[](const TupleBuffer* pagedVectorBuffer)
            {
                const auto pagedVector = PagedVector::load(*pagedVectorBuffer);
                const auto numberOfEntriesVal = pagedVector.getTotalNumberOfRecords();
                INVARIANT(numberOfEntriesVal > 0, "The number of entries in the paged vector must be greater than 0");
                return numberOfEntriesVal;
            },
            pagedVecBuffer.asArg());

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
        const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
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

void MedianAggregationPhysicalFunction::reset(const AggregationStateRef& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = tupleLayout->getTupleSize();
    /// Allocate a fresh paged vector and store it as a child of the parent buffer; the returned index is kept in the aggregation state.
    auto pagedVector = PagedVectorRef::createBuffer(pipelineMemoryProvider.bufferProvider, tupleSize);
    const nautilus::val<uint32_t> childBufferIndexVal
        = static_cast<nautilus::val<uint32_t>>(aggregationState.getBuffer().storeChild(std::move(pagedVector)));

    auto memArea = aggregationState.data();
    if (inputType.nullable)
    {
        nautilus::memset(memArea, 0, 1);
        memArea += nautilus::val<uint64_t>{1};
    }
    /// write the index in both nullable and non-nullable cases
    auto indexMemArea = static_cast<nautilus::val<uint32_t*>>(memArea);
    *indexMemArea = childBufferIndexVal;
}

void MedianAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> /*aggregationState*/)
{
    /// No-op: the paged vector buffer is stored as a child of the parent hash map TupleBuffer and
    /// is released automatically when the parent is released.
}

size_t MedianAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// ContainsNullValues (1B, optional) + uint32_t child buffer index (4B)
    return static_cast<uint64_t>(inputType.nullable) + sizeof(uint32_t);
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
