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
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ExecutionContext.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
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
        /// SQL-standard: skip NULL inputs
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
        const auto isNull = readNull(aggregationState);
        const auto sum = VarVal::readVarValFromMemory(memAreaSum, resultType, nautilus::val<bool>(false));
        const auto sumPlusValue = (sum + value).castToType(resultType.type);
        const auto newSum = VarVal::select(value.isNull(), sum, sumPlusValue);
        newSum.writeToMemory(memAreaSum);

        /// Stay null only while every input has been NULL
        storeNull(aggregationState, isNull and value.isNull());
    }
    else
    {
        /// Reading old sum from the aggregation state
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
        const auto sum = VarVal::readNonNullableVarValFromMemory(memAreaSum, resultType);

        /// Updating the sum and write it back to the aggregation state. The sum is accumulated in the widened result type
        /// (e.g. INT64 for INT8/16/32 inputs) to avoid overflowing the input type (#1694).
        const auto newSum = (sum + value).castToType(resultType.type);
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
        const auto sum1 = VarVal::readVarValFromMemory(memAreaSum1, resultType, nautilus::val<bool>(false));

        /// Reading the sum from the second aggregation state
        const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2 + nautilus::val<uint64_t>{1});
        const auto isNull2 = readNull(aggregationState2);
        const auto sum2 = VarVal::readVarValFromMemory(memAreaSum2, resultType, nautilus::val<bool>(false));

        /// Combining the sum and writing it back to the first aggregation state
        const auto newSum = (sum1 + sum2).castToType(resultType.type);
        newSum.writeToMemory(memAreaSum1);

        /// SQL-standard: only stay null when both partitions are still empty
        storeNull(aggregationState1, isNull1 and isNull2);
    }
    else
    {
        /// Reading the sum from the first aggregation state
        const auto memAreaSum1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
        const auto sum1 = VarVal::readNonNullableVarValFromMemory(memAreaSum1, resultType);

        /// Reading the sum from the second aggregation state
        const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
        const auto sum2 = VarVal::readNonNullableVarValFromMemory(memAreaSum2, resultType);

        /// Combining the sum and writing it back to the first aggregation state
        const auto newSum = (sum1 + sum2).castToType(resultType.type);
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
        const auto sum = VarVal::readVarValFromMemory(memAreaSum, resultType, isNull);

        /// Creating a record with the sum
        Record record;
        record.write(resultFieldIdentifier, sum);
        return record;
    }

    /// Reading the sum from the aggregation state
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto sum = VarVal::readNonNullableVarValFromMemory(memAreaSum, resultType);

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

    /// Initialize the null flag to "no value seen yet"; it flips to false on the first non-null input
    if (inputType.nullable)
    {
        storeNull(aggregationState, true);
    }
}

void SumAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t SumAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Size of isNull + size of the sum value, which is accumulated in the widened result type
    return (inputType.nullable ? sizeof(bool) : 0) + resultType.getSizeInBytesWithoutNull();
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterSumAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<SumAggregationPhysicalFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

}
