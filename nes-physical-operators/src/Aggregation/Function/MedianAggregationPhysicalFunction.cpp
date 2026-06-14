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
#include <functional>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/function.hpp>
#include <nautilus/nautilus_function.hpp>
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
/// The single field name under which the aggregated value is stored in / read from the paged vector.
constexpr auto valueFieldName = "val";

/// Maps a C++ type to its DataType::Type. Only the numeric types median accepts are provided.
template <typename T>
constexpr DataType::Type typeEnumOf();
// clang-format off
template <> constexpr DataType::Type typeEnumOf<int8_t>() { return DataType::Type::INT8; }
template <> constexpr DataType::Type typeEnumOf<int16_t>() { return DataType::Type::INT16; }
template <> constexpr DataType::Type typeEnumOf<int32_t>() { return DataType::Type::INT32; }
template <> constexpr DataType::Type typeEnumOf<int64_t>() { return DataType::Type::INT64; }
template <> constexpr DataType::Type typeEnumOf<uint8_t>() { return DataType::Type::UINT8; }
template <> constexpr DataType::Type typeEnumOf<uint16_t>() { return DataType::Type::UINT16; }
template <> constexpr DataType::Type typeEnumOf<uint32_t>() { return DataType::Type::UINT32; }
template <> constexpr DataType::Type typeEnumOf<uint64_t>() { return DataType::Type::UINT64; }
template <> constexpr DataType::Type typeEnumOf<float>() { return DataType::Type::FLOAT32; }
template <> constexpr DataType::Type typeEnumOf<double>() { return DataType::Type::FLOAT64; }
// clang-format on

template <typename T>
std::string typeName()
{
    return std::string(magic_enum::enum_name(typeEnumOf<T>()));
}

/// The non-nullable single-value DataType stored per row for the given C++ type.
template <typename T>
DataType valueDataType()
{
    return DataType{typeEnumOf<T>(), DataType::NULLABLE::NOT_NULLABLE};
}

/// Per-input-type, one-column buffer (the aggregated value) used to write/read stored values. It is fully
/// determined by the input type, hence identical across all medians of the same type. Lazily and thread-safely
/// initialized via a function-local static; selectMedianFunctions() initializes it with the query's page size
/// before any tracing happens, so the default page size below is only a fallback.
template <typename T>
const std::shared_ptr<TupleBufferRef>& bufferRefFor(const uint64_t pageSize = 4096)
{
    static const std::shared_ptr<TupleBufferRef> bufferRef = LowerSchemaProvider::lowerSchema(
        pageSize, Schema().addField(valueFieldName, valueDataType<T>()), MemoryLayoutType::ROW_LAYOUT);
    return bufferRef;
}

/// --- The extracted nautilus function bodies. Free, type-parametric (no per-instance capture) so they can be
/// global statics shared across all medians of the same type. ---

template <typename InputT, bool Nullable>
void medianLiftBody(
    nautilus::val<AggregationState*> aggregationState,
    nautilus::val<int8_t*> valueScratch,
    [[maybe_unused]] nautilus::val<bool> isNull,
    nautilus::val<AbstractBufferProvider*> bufferProvider)
{
    auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    if constexpr (Nullable)
    {
        /// Combine the incoming null information with the one stored so far in the first byte.
        AggregationPhysicalFunction::storeNull(aggregationState, isNull or AggregationPhysicalFunction::readNull(aggregationState));
        memArea += nautilus::val<uint64_t>{1};
    }

    const PagedVectorRef pagedVectorRef(static_cast<nautilus::val<PagedVector*>>(memArea), bufferRefFor<InputT>());
    const auto value = VarVal::readNonNullableVarValFromMemory(valueScratch, valueDataType<InputT>());
    Record record;
    record.write(valueFieldName, value);
    pagedVectorRef.writeRecord(record, bufferProvider);
}

template <bool Nullable>
void medianCombineBody(nautilus::val<AggregationState*> aggregationState1, nautilus::val<AggregationState*> aggregationState2)
{
    auto memArea1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    auto memArea2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    if constexpr (Nullable)
    {
        AggregationPhysicalFunction::storeNull(
            aggregationState1, AggregationPhysicalFunction::readNull(aggregationState1) or AggregationPhysicalFunction::readNull(aggregationState2));
        memArea1 += nautilus::val<uint64_t>{1};
        memArea2 += nautilus::val<uint64_t>{1};
    }

    /// Merge the two paged vectors by appending the content of the second to the first.
    nautilus::invoke(
        +[](PagedVector* vector1, const PagedVector* vector2) -> void { vector1->copyFrom(*vector2); }, memArea1, memArea2);
}

template <typename InputT, typename ResultT, bool Nullable>
void medianLowerBody(
    nautilus::val<AggregationState*> aggregationState, nautilus::val<int8_t*> resultScratch, nautilus::val<int8_t*> nullScratch)
{
    nautilus::val<bool> containsNull{false};
    if constexpr (Nullable)
    {
        containsNull = AggregationPhysicalFunction::readNull(aggregationState);
    }
    VarVal{containsNull}.writeToMemory(nullScratch);

    constexpr auto resultTypeEnum = typeEnumOf<ResultT>();
    const VarVal zero(nautilus::val<uint64_t>(0));
    VarVal medianValue = zero.castToType(resultTypeEnum);
    /// Single exit: on null the result stays the (irrelevant) default and the caller returns null based on the
    /// null flag written above. We deliberately avoid an early return inside the nautilus function body.
    if (not containsNull)
    {
        const auto pagedVectorPtr = static_cast<nautilus::val<PagedVector*>>(
            aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(Nullable)});
        const PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRefFor<InputT>());
        const std::vector<Record::RecordFieldIdentifier> projections{valueFieldName};
        const auto numberOfEntries = nautilus::invoke(
            +[](const PagedVector* pagedVector)
            {
                const auto numberOfEntriesVal = pagedVector->getTotalNumberOfEntries();
                INVARIANT(numberOfEntriesVal > 0, "The number of entries in the paged vector must be greater than 0");
                return numberOfEntriesVal;
            },
            pagedVectorPtr);

        /// Two nested loops over all stored values to find the median, identical in spirit to the original
        /// implementation but reading the single stored value instead of re-applying the input function to a
        /// full record.
        const nautilus::val<int64_t> medianPos1 = (numberOfEntries - 1) / 2;
        const nautilus::val<int64_t> medianPos2 = numberOfEntries / 2;
        nautilus::val<uint64_t> medianItemPos1 = 0;
        nautilus::val<uint64_t> medianItemPos2 = 0;
        nautilus::val<bool> medianFound1(false);
        nautilus::val<bool> medianFound2(false);

        const auto endIt = pagedVectorRef.end(projections);
        for (auto candidateIt = pagedVectorRef.begin(projections); candidateIt != endIt; ++candidateIt)
        {
            nautilus::val<int64_t> countLessThan = 0;
            nautilus::val<int64_t> countEqual = 0;
            const auto candidateRecord = *candidateIt;
            const auto candidateValue = candidateRecord.read(valueFieldName);

            for (auto itemIt = pagedVectorRef.begin(projections); itemIt != endIt; ++itemIt)
            {
                const auto itemRecord = *itemIt;
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

            if (not medianFound1 && countLessThan <= medianPos1 && medianPos1 < countLessThan + countEqual)
            {
                medianItemPos1 = candidateIt - pagedVectorRef.begin(projections);
                medianFound1 = true;
            }
            if (not medianFound2 && countLessThan <= medianPos2 && medianPos2 < countLessThan + countEqual)
            {
                medianItemPos2 = candidateIt - pagedVectorRef.begin(projections);
                medianFound2 = true;
            }
        }

        if (medianFound1 and medianFound2)
        {
            /// Regardless of an odd/even number of entries, the median is the average of the two middle values
            /// (for odd counts both positions point at the same item).
            const auto medianRecord1 = pagedVectorRef.readRecord(medianItemPos1, projections);
            const auto medianRecord2 = pagedVectorRef.readRecord(medianItemPos2, projections);
            const auto medianValue1 = medianRecord1.read(valueFieldName);
            const auto medianValue2 = medianRecord2.read(valueFieldName);
            const VarVal two = nautilus::val<uint64_t>(2);
            medianValue
                = (medianValue1.castToType(resultTypeEnum) + medianValue2.castToType(resultTypeEnum)) / two.castToType(resultTypeEnum);
        }
    }

    medianValue.writeToMemory(resultScratch);
}

template <bool Nullable>
void medianResetBody(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Allocates a new PagedVector in the memory area provided by the pointer.
            auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea);
            new (pagedVector) PagedVector();
        },
        aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(Nullable)});

    if constexpr (Nullable)
    {
        const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
        nautilus::memset(memArea, 0, 1);
    }
}

/// Global static, statically allocated table of the four extracted nautilus functions for one (input type,
/// result type, nullability) combination. Names are derived purely from the data types so that nautilus
/// deduplicates same-typed medians to a single compiled function. combine and reset depend only on
/// nullability and are named accordingly, so they are additionally shared across input types.
template <typename InputT, typename ResultT, bool Nullable>
struct MedianFunctionTable
{
    static inline MedianAggregationPhysicalFunction::LiftFunction lift{
        "Median_lift_" + typeName<InputT>() + (Nullable ? "_N" : "_X"), std::function(medianLiftBody<InputT, Nullable>)};
    static inline MedianAggregationPhysicalFunction::CombineFunction combine{
        std::string("Median_combine_") + (Nullable ? "N" : "X"), std::function(medianCombineBody<Nullable>)};
    static inline MedianAggregationPhysicalFunction::LowerFunction lower{
        "Median_lower_" + typeName<InputT>() + "_" + typeName<ResultT>() + (Nullable ? "_N" : "_X"),
        std::function(medianLowerBody<InputT, ResultT, Nullable>)};
    static inline MedianAggregationPhysicalFunction::ResetFunction reset{
        std::string("Median_reset_") + (Nullable ? "N" : "X"), std::function(medianResetBody<Nullable>)};
};

template <typename InputT, typename ResultT, bool Nullable>
MedianAggregationPhysicalFunction::MedianFunctions makeSelected(const uint64_t pageSize)
{
    /// Initialize the per-type buffer ref from the query's page size before any tracing happens.
    bufferRefFor<InputT>(pageSize);
    return {
        &MedianFunctionTable<InputT, ResultT, Nullable>::lift,
        &MedianFunctionTable<InputT, ResultT, Nullable>::combine,
        &MedianFunctionTable<InputT, ResultT, Nullable>::lower,
        &MedianFunctionTable<InputT, ResultT, Nullable>::reset};
}

template <typename InputT, bool Nullable>
MedianAggregationPhysicalFunction::MedianFunctions selectByResultType(const DataType::Type resultType, const uint64_t pageSize)
{
    /// Median's result type is always FLOAT64 (see MedianAggregationLogicalFunction), but we dispatch on it
    /// explicitly to keep the result type a real template parameter.
    switch (resultType)
    {
        case DataType::Type::FLOAT64:
            return makeSelected<InputT, double, Nullable>(pageSize);
        default:
            throw UnknownAggregationType("MedianAggregation: unsupported result type {}", magic_enum::enum_name(resultType));
    }
}

template <bool Nullable>
MedianAggregationPhysicalFunction::MedianFunctions
selectByInputType(const DataType::Type inputType, const DataType::Type resultType, const uint64_t pageSize)
{
    switch (inputType)
    {
        case DataType::Type::INT8:
            return selectByResultType<int8_t, Nullable>(resultType, pageSize);
        case DataType::Type::INT16:
            return selectByResultType<int16_t, Nullable>(resultType, pageSize);
        case DataType::Type::INT32:
            return selectByResultType<int32_t, Nullable>(resultType, pageSize);
        case DataType::Type::INT64:
            return selectByResultType<int64_t, Nullable>(resultType, pageSize);
        case DataType::Type::UINT8:
            return selectByResultType<uint8_t, Nullable>(resultType, pageSize);
        case DataType::Type::UINT16:
            return selectByResultType<uint16_t, Nullable>(resultType, pageSize);
        case DataType::Type::UINT32:
            return selectByResultType<uint32_t, Nullable>(resultType, pageSize);
        case DataType::Type::UINT64:
            return selectByResultType<uint64_t, Nullable>(resultType, pageSize);
        case DataType::Type::FLOAT32:
            return selectByResultType<float, Nullable>(resultType, pageSize);
        case DataType::Type::FLOAT64:
            return selectByResultType<double, Nullable>(resultType, pageSize);
        default:
            throw UnknownAggregationType("MedianAggregation: unsupported input type {}", magic_enum::enum_name(inputType));
    }
}

MedianAggregationPhysicalFunction::MedianFunctions
selectMedianFunctions(const DataType& inputType, const DataType& resultType, const uint64_t pageSize)
{
    return inputType.nullable ? selectByInputType<true>(inputType.type, resultType.type, pageSize)
                              : selectByInputType<false>(inputType.type, resultType.type, pageSize);
}
}

MedianAggregationPhysicalFunction::MedianAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<TupleBufferRef> bufferRefPagedVector)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
    /// We only store the single aggregated value per row, so we only need the page size of the provided buffer
    /// ref; the actual one-column buffer is derived from the input type inside the function table.
    functions = selectMedianFunctions(this->inputType, this->resultType, bufferRefPagedVector->getBufferSize());
}

void MedianAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    /// Column-specific work stays in the pipeline: compute the value and hand it to the nautilus function
    /// through a scratch slot (a Record/VarVal cannot cross the function boundary).
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto valueScratch
        = pipelineMemoryProvider.arena.allocateMemory(nautilus::val<size_t>(inputType.getSizeInBytesWithoutNull()));
    value.writeToMemory(valueScratch);
    (*functions.lift)(aggregationState, valueScratch, value.isNull(), pipelineMemoryProvider.bufferProvider);
}

void MedianAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    (*functions.combine)(aggregationState1, aggregationState2);
}

Record MedianAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto resultScratch
        = pipelineMemoryProvider.arena.allocateMemory(nautilus::val<size_t>(resultType.getSizeInBytesWithoutNull()));
    const auto nullScratch = pipelineMemoryProvider.arena.allocateMemory(nautilus::val<size_t>(1));
    (*functions.lower)(aggregationState, resultScratch, nullScratch);

    const auto isNull = readValueFromMemRef<bool>(nullScratch);
    const auto medianValue = VarVal::readVarValFromMemory(resultScratch, resultType, isNull);
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, medianValue);
    return resultRecord;
}

void MedianAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    (*functions.reset)(aggregationState);
}

void MedianAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)});
}

size_t MedianAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// ContainsNullValues (1B) + Pagedvector
    return static_cast<uint64_t>(inputType.nullable) + sizeof(PagedVector);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMedianAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.bufferRefPagedVector.has_value(), "Memory provider paged vector not set");
    return std::make_shared<MedianAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.bufferRefPagedVector.value());
}

}
