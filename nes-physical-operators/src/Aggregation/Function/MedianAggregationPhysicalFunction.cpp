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

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <magic_enum/magic_enum.hpp>
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

namespace
{
/// Median of the first count values of type T at data, as the average of the two middle values (for an odd count both are
/// the same value). Reorders the values.
template <typename T>
double medianOf(int8_t* data, const uint64_t count) noexcept
{
    auto* const values = reinterpret_cast<T*>(data); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    /// NaN is ordered after all numbers, so that the comparison stays a strict weak ordering.
    const auto less = [](const T lhs, const T rhs)
    {
        if constexpr (std::is_floating_point_v<T>)
        {
            return lhs < rhs || (!std::isnan(lhs) && std::isnan(rhs));
        }
        else
        {
            return lhs < rhs;
        }
    };
    auto* const lowerMiddle = values + ((count - 1) / 2);
    std::nth_element(values, lowerMiddle, values + count, less);
    const auto lowerMedian = static_cast<double>(*lowerMiddle);
    if (count % 2 == 1)
    {
        return lowerMedian;
    }
    /// After nth_element, all values after lowerMiddle are not smaller than it, so the upper middle is their minimum.
    const auto upperMedian = static_cast<double>(*std::min_element(lowerMiddle + 1, values + count, less));
    return (lowerMedian + upperMedian) / 2;
}

nautilus::val<double> selectMedian(const DataType::Type type, const nautilus::val<int8_t*>& values, const nautilus::val<uint64_t>& count)
{
    switch (type)
    {
        case DataType::Type::UINT8:
            return nautilus::invoke(medianOf<uint8_t>, values, count);
        case DataType::Type::UINT16:
            return nautilus::invoke(medianOf<uint16_t>, values, count);
        case DataType::Type::UINT32:
            return nautilus::invoke(medianOf<uint32_t>, values, count);
        case DataType::Type::UINT64:
            return nautilus::invoke(medianOf<uint64_t>, values, count);
        case DataType::Type::INT8:
            return nautilus::invoke(medianOf<int8_t>, values, count);
        case DataType::Type::INT16:
            return nautilus::invoke(medianOf<int16_t>, values, count);
        case DataType::Type::INT32:
            return nautilus::invoke(medianOf<int32_t>, values, count);
        case DataType::Type::INT64:
            return nautilus::invoke(medianOf<int64_t>, values, count);
        case DataType::Type::FLOAT32:
            return nautilus::invoke(medianOf<float>, values, count);
        case DataType::Type::FLOAT64:
            return nautilus::invoke(medianOf<double>, values, count);
        default:
            INVARIANT(false, "Median is only supported on numeric types, but got {}", magic_enum::enum_name(type));
            std::unreachable();
    }
}
}

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
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
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
            const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
            OwnedNautilusBuffer pagedVecBuffer;
            nautilus::invoke(
                +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
                { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
                parentBuffer.asArg(),
                pagedVecBuffer.asArg(),
                static_cast<nautilus::val<uint32_t*>>(memArea));

            PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
            pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
        }
    }
    else
    {
        /// Load the paged vector buffer from the parent via the stored child index
        const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
        OwnedNautilusBuffer pagedVecBuffer;
        nautilus::invoke(
            +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
            { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
            parentBuffer.asArg(),
            pagedVecBuffer.asArg(),
            static_cast<nautilus::val<uint32_t*>>(memArea));

        PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
        pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
    }
}

void MedianAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer parentBuffer1,
    const nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    auto memArea1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    auto memArea2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);

    if (inputType.nullable)
    {
        /// Combining the null values
        const auto containsNull1 = readNull(aggregationState1);
        const auto containsNull2 = readNull(aggregationState2);
        storeNull(aggregationState1, containsNull1 and containsNull2);

        /// Skipping the first byte (null)
        memArea1 += nautilus::val<uint64_t>{1};
        memArea2 += nautilus::val<uint64_t>{1};
    }

    /// Load both paged vector buffers via their stored child indices, then copy pages from source into destination
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider,
            TupleBuffer* parent1,
            const uint32_t* indexPtr1,
            TupleBuffer* parent2,
            const uint32_t* indexPtr2) -> void
        {
            const TupleBuffer vec1Buf = parent1->loadChildBuffer(ChildBufferIndex{*indexPtr1});
            const TupleBuffer vec2Buf = parent2->loadChildBuffer(ChildBufferIndex{*indexPtr2});
            auto vector1 = PagedVector::load(vec1Buf);
            const auto vector2 = PagedVector::load(vec2Buf);
            vector1.copyPagesFrom(*bufferProvider, vector2);
        },
        pipelineMemoryProvider.bufferProvider,
        parentBuffer1.asArg(),
        static_cast<nautilus::val<uint32_t*>>(memArea1),
        parentBuffer2.asArg(),
        static_cast<nautilus::val<uint32_t*>>(memArea2));
}

Record MedianAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// If it contains null values, we simply return a null value
    auto containsNull = nautilus::val<bool>{false};
    if (inputType.nullable)
    {
        containsNull = readNull(aggregationState);
    }

    const VarVal zero{nautilus::val<uint64_t>{0}, true, true};
    VarVal medianValue = zero.castToType(resultType.type);

    if (!containsNull)
    {
        /// Load the paged vector buffer from the parent via its stored child index
        auto memArea
            = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)});
        OwnedNautilusBuffer pagedVecBuffer;
        nautilus::invoke(
            +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
            { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
            parentBuffer.asArg(),
            pagedVecBuffer.asArg(),
            static_cast<nautilus::val<uint32_t*>>(memArea));

        const auto numberOfEntries = invoke(
            +[](const TupleBuffer* pagedVectorBuffer)
            {
                const auto pagedVector = PagedVector::load(*pagedVectorBuffer);
                const auto numberOfEntriesVal = pagedVector.getTotalNumberOfRecords();
                INVARIANT(numberOfEntriesVal > 0, "The number of entries in the paged vector must be greater than 0");
                return numberOfEntriesVal;
            },
            pagedVecBuffer.asArg());

        /// Copy the values into a contiguous array and select the median in C++ with std::nth_element. This is O(n) instead of
        /// comparing every value against every other value, and it traces a single loop instead of two nested ones.
        const nautilus::val<uint64_t> valueSize = inputType.getSizeInBytesWithoutNull();
        const auto values = pipelineMemoryProvider.arena.allocateMemory(numberOfEntries * valueSize);
        const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
        nautilus::val<int8_t*> valuePtr = values;
        for (const auto& itemRecord : pagedVectorRef)
        {
            inputFunction.execute(itemRecord, pipelineMemoryProvider.arena).castToType(inputType.type).writeToMemory(valuePtr);
            valuePtr = valuePtr + valueSize;
        }

        /// The result's nullability is a static property of the VarVal, so it has to match on both branches of containsNull.
        medianValue
            = VarVal{selectMedian(inputType.type, values, numberOfEntries), resultType.nullable, nautilus::val<bool>{false}}.castToType(
                resultType.type);
    }


    /// Adding the median to the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, medianValue);

    return resultRecord;
}

void MedianAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = getSizeInBytes(tupleLayout->getSchema());
    const nautilus::val<uint32_t> childBufferIndexVal = nautilus::invoke(
        +[](TupleBuffer* parentBuffer, AbstractBufferProvider* bufferProvider, uint64_t tupleSize)
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): aggregation state stores a TupleBuffer at this slot.
            if (auto pagedVectorBufferOpt = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                /// initialize paged vector buffer
                auto pagedVectorBuffer = pagedVectorBufferOpt.value();
                PagedVector::init(pagedVectorBuffer, bufferProvider->getBufferSize(), tupleSize);
                auto childBufferIndex = parentBuffer->storeChildBuffer(pagedVectorBuffer);
                return childBufferIndex.getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for median aggregation paged vector!");
        },
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider,
        tupleSize);

    auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    if (inputType.nullable)
    {
        /// Initialize the null flag to "no value seen yet" so all-NULL windows correctly emit NULL
        storeNull(aggregationState, true);
        /// Skipping the first byte (null); the paged vector lives right after it.
        memArea += nautilus::val<uint64_t>{1};
    }
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

AggregationPhysicalFunctionRegistryReturnType
MedianAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
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
