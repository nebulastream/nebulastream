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

#include <Aggregation/Function/SumAggregationPhysicalFunction.hpp>

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
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

SumAggregationPhysicalFunction::SumAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

void SumAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (inputType.nullable)
    {
        /// If the value is null and we do not include null values, we need to set the multiplication factor to 0
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
        const auto isNull = readNull(aggregationState) or value.isNull();
        const auto sum = VarVal::readVarValFromMemory(memAreaSum, inputType, isNull);

        /// If value is null, we keep the old value. Otherwise, we add the value to the sum.
        const auto newSum = VarVal::select(isNull, sum, (sum + value).castToType(inputType.type));
        newSum.writeToMemory(memAreaSum);
        storeNull(aggregationState, isNull);
    }
    else
    {
        /// Reading old sum from the aggregation state
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
        const auto sum = VarVal::readNonNullableVarValFromMemory(memAreaSum, inputType);

        /// Updating the sum and write it back to the aggregation state
        const auto newSum = (sum + value).castToType(inputType.type);
        newSum.writeToMemory(memAreaSum);
    }
}

void SumAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    if (inputType.nullable)
    {
        /// Reading the sum from the first aggregation state
        const auto memAreaSum1 = static_cast<nautilus::val<int8_t*>>(aggregationState1 + nautilus::val<uint64_t>{1});
        const auto isNull1 = readNull(aggregationState1);
        const auto sum1 = VarVal::readVarValFromMemory(memAreaSum1, inputType, isNull1);

        /// Reading the sum from the second aggregation state
        const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2 + nautilus::val<uint64_t>{1});
        const auto isNull2 = readNull(aggregationState2);
        const auto sum2 = VarVal::readVarValFromMemory(memAreaSum2, inputType, isNull2);

        /// Combining the sum
        const auto newSum = (sum1 + sum2).castToType(inputType.type);

        /// Writing the new sum and null back to the first aggregation state
        newSum.writeToMemory(memAreaSum1);
        storeNull(aggregationState1, newSum.isNull());
    }
    else
    {
        /// Reading the sum from the first aggregation state
        const auto memAreaSum1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
        const auto sum1 = VarVal::readNonNullableVarValFromMemory(memAreaSum1, inputType);

        /// Reading the sum from the second aggregation state
        const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
        const auto sum2 = VarVal::readNonNullableVarValFromMemory(memAreaSum2, inputType);

        /// Combining the sum and writing it back to the first aggregation state
        const auto newSum = (sum1 + sum2).castToType(inputType.type);
        newSum.writeToMemory(memAreaSum1);
    }
}

Record SumAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    if (inputType.nullable)
    {
        /// Reading the sum from the aggregation state
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
        const auto isNull = readNull(aggregationState);
        const auto sum = VarVal::readVarValFromMemory(memAreaSum, inputType, isNull);

        /// Creating a record with the sum
        Record record;
        record.write(resultFieldIdentifier, sum);
        return record;
    }

    /// Reading the sum from the aggregation state
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaCount = memAreaSum + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
    const auto sum = VarVal::readNonNullableVarValFromMemory(memAreaSum, inputType);

    /// Creating a record with the sum
    Record record;
    record.write(resultFieldIdentifier, sum);

    return record;
}

void SumAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the sum to 0
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    nautilus::memset(memArea, 0, getSizeOfStateInBytes());
}

void SumAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t SumAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return inputType.getSizeInBytesWithNull();
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterSumAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<SumAggregationPhysicalFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

}
