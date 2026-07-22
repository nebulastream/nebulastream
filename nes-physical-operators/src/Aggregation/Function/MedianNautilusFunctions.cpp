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

#include <Aggregation/Function/MedianNautilusFunctions.hpp>

#include <cstdint>
#include <memory>
#include <new>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

nautilus::val<double> medianOf(
    const nautilus::val<TupleBuffer*>& pagedVectorBuffer,
    const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout,
    const Record::RecordFieldIdentifier& valueFieldName)
{
    const auto numberOfEntries = nautilus::invoke(
        +[](const TupleBuffer* buffer)
        {
            const auto pagedVector = PagedVector::load(*buffer);
            const auto numberOfEntriesVal = pagedVector.getTotalNumberOfRecords();
            INVARIANT(numberOfEntriesVal > 0, "The number of entries in the paged vector must be greater than 0");
            return numberOfEntriesVal;
        },
        pagedVectorBuffer);

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
    const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVectorBuffer), tupleLayout);
    const auto pagedVectorEnd = pagedVectorRef.end();
    nautilus::val<uint64_t> candidatePos = 0;
    for (auto candidateIt = pagedVectorRef.begin(); candidateIt != pagedVectorEnd; ++candidateIt)
    {
        nautilus::val<int64_t> countLessThan = 0;
        nautilus::val<int64_t> countEqual = 0;
        const auto candidateValue = (*candidateIt).read(valueFieldName);

        /// Counting how many items are smaller or equal for the current candidate
        for (const auto& itemRecord : pagedVectorRef)
        {
            const auto itemValue = itemRecord.read(valueFieldName);
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

    VarVal medianValue = VarVal(nautilus::val<double>(0));
    if (medianFound1 and medianFound2)
    {
        /// Calculating the median value. Regardless if the number of entries is odd or even, we calculate the median as the average of the two middle values.
        /// For even numbers of entries, this is its natural definition.
        /// For odd numbers of entries, both positions are pointing to the same item and thus, we are calculating the average of the same item, which is the item itself.
        const auto medianValue1 = pagedVectorRef.at(medianItemPos1).read(valueFieldName);
        const auto medianValue2 = pagedVectorRef.at(medianItemPos2).read(valueFieldName);
        const VarVal two = nautilus::val<double>(2);
        medianValue = (medianValue1.castToType(DataType::Type::FLOAT64) + medianValue2.castToType(DataType::Type::FLOAT64)) / two;
    }
    return medianValue.getRawValueAs<nautilus::val<double>>();
}

void medianCombineStates(
    const nautilus::val<AggregationState*>& aggregationState1,
    const nautilus::val<AggregationState*>& aggregationState2,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const bool nullable)
{
    /// Getting the paged vectors from the aggregation states
    auto memArea1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    auto memArea2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);

    if (nullable)
    {
        /// SQL-standard: only stay null when both partitions are still empty (no non-null value seen on either side)
        const auto containsNull1 = AggregationPhysicalFunction::readNull(aggregationState1);
        const auto containsNull2 = AggregationPhysicalFunction::readNull(aggregationState2);
        AggregationPhysicalFunction::storeNull(aggregationState1, containsNull1 and containsNull2);

        /// Skipping the first byte (null)
        memArea1 += nautilus::val<uint64_t>{1};
        memArea2 += nautilus::val<uint64_t>{1};
    }

    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](AbstractBufferProvider* provider, TupleBuffer* vector1Buffer, const TupleBuffer* vector2Buffer) -> void
        {
            auto vector1 = PagedVector::load(*vector1Buffer);
            const auto vector2 = PagedVector::load(*vector2Buffer);
            vector1.copyPagesFrom(*provider, vector2);
        },
        bufferProvider,
        memArea1,
        memArea2);
}

void medianResetState(
    const nautilus::val<AggregationState*>& aggregationState,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<uint64_t>& tupleSize,
    const bool nullable)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea, AbstractBufferProvider* provider, uint64_t sizeOfTuple) -> void
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): aggregation state stores a TupleBuffer at this slot.
            auto* pagedVectorBufferMemArea = reinterpret_cast<TupleBuffer*>(pagedVectorMemArea);
            if (auto pagedVectorBuffer = provider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                /// initialize paged vector buffer
                PagedVector::init(pagedVectorBuffer.value(), provider->getBufferSize(), sizeOfTuple);
                /// @warning: this will be refactored again during the ChainedHashMap refactor
                new (pagedVectorBufferMemArea) TupleBuffer(pagedVectorBuffer.value());
                return;
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for median aggregation paged vector!");
        },
        aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(nullable)},
        bufferProvider,
        tupleSize);

    if (nullable)
    {
        /// Initialize the null flag to "no value seen yet" so all-NULL windows correctly emit NULL
        AggregationPhysicalFunction::storeNull(aggregationState, true);
    }
}

}
