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
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Detail/TypedMedianPrimitives.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/Record.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{

/// Host-side dispatch over (DataType::Type, nullable) onto a generic callable parametrised by
/// `<typename T, bool Nullable>`. Each Median instance picks exactly one branch at trace-construction
/// time; the other branches are not traced.
template <typename Callable>
auto dispatchOnType(const DataType& type, Callable&& callable)
{
    using AggregationDetail::TypedMedian;
    if (type.nullable)
    {
        switch (type.type)
        {
            case DataType::Type::INT8:
                return callable.template operator()<int8_t, true>();
            case DataType::Type::INT16:
                return callable.template operator()<int16_t, true>();
            case DataType::Type::INT32:
                return callable.template operator()<int32_t, true>();
            case DataType::Type::INT64:
                return callable.template operator()<int64_t, true>();
            case DataType::Type::UINT8:
                return callable.template operator()<uint8_t, true>();
            case DataType::Type::UINT16:
                return callable.template operator()<uint16_t, true>();
            case DataType::Type::UINT32:
                return callable.template operator()<uint32_t, true>();
            case DataType::Type::UINT64:
                return callable.template operator()<uint64_t, true>();
            case DataType::Type::FLOAT32:
                return callable.template operator()<float, true>();
            case DataType::Type::FLOAT64:
                return callable.template operator()<double, true>();
            default:
                throw UnknownDataType("MEDIAN does not support input type {}", type);
        }
    }
    switch (type.type)
    {
        case DataType::Type::INT8:
            return callable.template operator()<int8_t, false>();
        case DataType::Type::INT16:
            return callable.template operator()<int16_t, false>();
        case DataType::Type::INT32:
            return callable.template operator()<int32_t, false>();
        case DataType::Type::INT64:
            return callable.template operator()<int64_t, false>();
        case DataType::Type::UINT8:
            return callable.template operator()<uint8_t, false>();
        case DataType::Type::UINT16:
            return callable.template operator()<uint16_t, false>();
        case DataType::Type::UINT32:
            return callable.template operator()<uint32_t, false>();
        case DataType::Type::UINT64:
            return callable.template operator()<uint64_t, false>();
        case DataType::Type::FLOAT32:
            return callable.template operator()<float, false>();
        case DataType::Type::FLOAT64:
            return callable.template operator()<double, false>();
        default:
            throw UnknownDataType("MEDIAN does not support input type {}", type);
    }
}

}

MedianAggregationPhysicalFunction::MedianAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

void MedianAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    using AggregationDetail::TypedMedian;
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto statePtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    dispatchOnType(
        inputType,
        [&]<typename T, bool Nullable>()
        {
            const auto typedValue = value.getRawValueAs<nautilus::val<T>>();
            if constexpr (Nullable)
            {
                TypedMedian<T, Nullable>::lift(statePtr, typedValue, value.isNull(), pipelineMemoryProvider.bufferProvider);
            }
            else
            {
                TypedMedian<T, Nullable>::lift(statePtr, typedValue, pipelineMemoryProvider.bufferProvider);
            }
        });
}

void MedianAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    using AggregationDetail::TypedMedian;
    const auto lhs = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto rhs = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    dispatchOnType(
        inputType, [&]<typename T, bool Nullable>() { TypedMedian<T, Nullable>::combine(lhs, rhs); });
}

void MedianAggregationPhysicalFunction::lower(
    Record& outputRecord, const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    using AggregationDetail::TypedMedian;
    const auto statePtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    dispatchOnType(
        inputType,
        [&]<typename T, bool Nullable>()
        {
            const nautilus::val<double> medianAsDouble = TypedMedian<T, Nullable>::lower(statePtr);
            VarVal medianValue{medianAsDouble};
            if constexpr (Nullable)
            {
                const auto isNull = TypedMedian<T, Nullable>::lowerIsNull(statePtr);
                medianValue = VarVal{medianAsDouble, true, isNull};
            }
            outputRecord.write(resultFieldIdentifier, medianValue.castToType(resultType.type));
        });
}

void MedianAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    using AggregationDetail::TypedMedian;
    const auto statePtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    dispatchOnType(
        inputType, [&]<typename T, bool Nullable>() { TypedMedian<T, Nullable>::reset(statePtr); });
}

void MedianAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    using AggregationDetail::TypedMedian;
    const auto statePtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    dispatchOnType(
        inputType, [&]<typename T, bool Nullable>() { TypedMedian<T, Nullable>::cleanup(statePtr); });
}

size_t MedianAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Layout: (nullable ? 8B isNull padded : 0) + sizeof(PagedVector). PagedVector dominates.
    const size_t head = inputType.nullable ? 8 : 0;
    return head + sizeof(PagedVector);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMedianAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<MedianAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier);
}

}
