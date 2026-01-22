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

#include <Aggregation/Function/AvgAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ExecutionContext.hpp>
#include <select.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

AvgAggregationPhysicalFunction::AvgAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const bool includeNullValues)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , includeNullValues(includeNullValues)
{
}

void AvgAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (inputType.nullable)
    {
        /// If the value is null and we do not include null values, we need to set the multiplication factor to 0
        const auto multiplicationFactor
            = nautilus::select(not includeNullValues and value.isNull(), nautilus::val<int8_t>{0}, nautilus::val<int8_t>{1});

        /// Reading old sum and count from the aggregation state. The sum is stored at the beginning of the aggregation state and the count is stored after the sum
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
        const auto memAreaCount = memAreaSum + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
        const auto isNull = readNull(aggregationState);
        const auto sum = VarVal::readVarValFromMemory(memAreaSum, inputType, isNull);
        const auto count = VarVal::readNonNullableVarValFromMemory(memAreaCount, countType);

        /// Updating the sum and count with the new value
        const auto newSum = (sum + (value * multiplicationFactor)).castToType(inputType.type);
        const auto newCount = count + multiplicationFactor;

        /// Writing the new isNull, sum, and count back to the aggregation state
        newSum.writeToMemory(memAreaSum);
        newCount.writeToMemory(memAreaCount);
        storeNull(aggregationState, newSum.isNull());
    }
    else
    {
        /// Reading old sum and count from the aggregation state. The sum is stored at the beginning of the aggregation state and the count is stored after the sum
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
        const auto memAreaCount = memAreaSum + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
        const auto sum = VarVal::readNonNullableVarValFromMemory(memAreaSum, inputType);
        const auto count = VarVal::readNonNullableVarValFromMemory(memAreaCount, countType);

        /// Updating the sum and count with the new value
        const auto newSum = (sum + value).castToType(inputType.type);
        const auto newCount = count + nautilus::val<uint64_t>{1};

        /// Writing the new sum, and count back to the aggregation state
        newSum.writeToMemory(memAreaSum);
        newCount.writeToMemory(memAreaCount);
    }
}

void AvgAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    if (inputType.nullable)
    {
        /// Reading the sum and count from the first aggregation state
        const auto memAreaSum1 = static_cast<nautilus::val<int8_t*>>(aggregationState1 + nautilus::val<uint64_t>{1});
        const auto memAreaCount1 = memAreaSum1 + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
        const auto isNull1 = readNull(aggregationState1);
        const auto sum1 = VarVal::readVarValFromMemory(memAreaSum1, inputType, isNull1);
        const auto count1 = VarVal::readNonNullableVarValFromMemory(memAreaCount1, countType);

        /// Reading the sum and count from the second aggregation state
        const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2 + nautilus::val<uint64_t>{1});
        const auto memAreaCount2 = memAreaSum2 + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
        const auto isNull2 = readNull(aggregationState2);
        const auto sum2 = VarVal::readVarValFromMemory(memAreaSum2, inputType, isNull2);
        const auto count2 = VarVal::readNonNullableVarValFromMemory(memAreaCount2, countType);

        /// Combining the sum and count
        const auto newSum = (sum1 + sum2).castToType(inputType.type);
        const auto newCount = count1 + count2;

        /// Writing the new sum, count and null back to the first aggregation state
        newSum.writeToMemory(memAreaSum1);
        newCount.writeToMemory(memAreaCount1);
        storeNull(aggregationState1, newSum.isNull());
    }
    else
    {
        /// Reading the sum and count from the first aggregation state
        const auto memAreaSum1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
        const auto memAreaCount1 = memAreaSum1 + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
        const auto sum1 = VarVal::readNonNullableVarValFromMemory(memAreaSum1, inputType);
        const auto count1 = VarVal::readNonNullableVarValFromMemory(memAreaCount1, countType);

        /// Reading the sum and count from the second aggregation state
        const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
        const auto memAreaCount2 = memAreaSum2 + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
        const auto sum2 = VarVal::readNonNullableVarValFromMemory(memAreaSum2, inputType);
        const auto count2 = VarVal::readNonNullableVarValFromMemory(memAreaCount2, countType);

        /// Combining the sum and count
        const auto newSum = (sum1 + sum2).castToType(inputType.type);
        const auto newCount = count1 + count2;

        /// Writing the new sum and count back to the first aggregation state
        newSum.writeToMemory(memAreaSum1);
        newCount.writeToMemory(memAreaCount1);
    }
}

Record AvgAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    if (inputType.nullable)
    {
        /// Reading the sum and count from the aggregation state
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
        const auto memAreaCount = memAreaSum + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
        const auto isNull = readNull(aggregationState);
        const auto sum = VarVal::readVarValFromMemory(memAreaSum, inputType, isNull);
        const auto count = VarVal::readNonNullableVarValFromMemory(memAreaCount, countType);

        /// Calculating the average and returning a record with the result
        const auto avg = sum.castToType(resultType.type) / count.castToType(resultType.type);
        return Record({{resultFieldIdentifier, avg}});
    }

    /// Reading the sum and count from the aggregation state
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaCount = memAreaSum + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
    const auto sum = VarVal::readNonNullableVarValFromMemory(memAreaSum, inputType);
    const auto count = VarVal::readNonNullableVarValFromMemory(memAreaCount, countType);

    /// Calculating the average and returning a record with the result
    const auto avg = sum.castToType(resultType.type) / count.castToType(resultType.type);
    return Record({{resultFieldIdentifier, avg}});
}

void AvgAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the isNull, sum, and count to 0
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    nautilus::memset(memArea, 0, getSizeOfStateInBytes());
}

void AvgAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t AvgAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Size of isNull + size of the sum value + size of the count value
    const auto inputSize = inputType.getSizeInBytesWithoutNull();
    const auto countTypeSize = countType.getSizeInBytesWithoutNull();
    return (inputType.nullable and includeNullValues ? sizeof(bool) : 0) + inputSize + countTypeSize;
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterAvgAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<AvgAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.includeNullValues);
}

}
