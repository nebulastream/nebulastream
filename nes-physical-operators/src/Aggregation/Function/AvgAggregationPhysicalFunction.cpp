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
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
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
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record,
    const nautilus::val<Timestamp>&)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (inputType.nullable)
    {
        /// SQL-standard: AVG always skips NULL inputs from both the running sum and count.
        const auto multiplicationFactor = nautilus::select(value.isNull(), nautilus::val<int8_t>{0}, nautilus::val<int8_t>{1});

        /// Reading old sum and count from the aggregation state. The sum is stored at the beginning of the aggregation state and the count is stored after the sum.
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
        const auto memAreaCount = memAreaSum + nautilus::val<uint64_t>(resultType.getSizeInBytesWithoutNull());
        const auto isNull = readNull(aggregationState);
        const auto sum = VarVal::readVarValFromMemory(memAreaSum, resultType, nautilus::val<bool>(false));
        const auto count = VarVal::readNonNullableVarValFromMemory(memAreaCount, countType);

        /// Updating the sum and count: skip NULL inputs entirely
        const auto sumPlusValue = (sum + value).castToType(resultType.type);
        const auto newSum = VarVal::select(value.isNull(), sum, sumPlusValue);
        const auto newCount = count + multiplicationFactor;

        /// Writing the new sum and count back to the aggregation state
        newSum.writeToMemory(memAreaSum);
        newCount.writeToMemory(memAreaCount);

        /// Stay null only while every input has been NULL
        storeNull(aggregationState, isNull and value.isNull());
    }
    else
    {
        /// Reading old sum and count from the aggregation state. The sum is stored at the beginning of the aggregation state and the count is stored after the sum
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
        const auto memAreaCount = memAreaSum + nautilus::val<uint64_t>(resultType.getSizeInBytesWithoutNull());
        const auto sum = VarVal::readVarValFromMemory(memAreaSum, resultType, nautilus::val<bool>(false));
        const auto count = VarVal::readNonNullableVarValFromMemory(memAreaCount, countType);

        /// Updating the sum and count with the new value
        const auto newSum = (sum + value).castToType(resultType.type);
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
        const auto memAreaCount1 = memAreaSum1 + nautilus::val<uint64_t>(resultType.getSizeInBytesWithoutNull());
        const auto isNull1 = readNull(aggregationState1);
        const auto sum1 = VarVal::readVarValFromMemory(memAreaSum1, resultType, nautilus::val<bool>(false));
        const auto count1 = VarVal::readNonNullableVarValFromMemory(memAreaCount1, countType);

        /// Reading the sum and count from the second aggregation state
        const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2 + nautilus::val<uint64_t>{1});
        const auto memAreaCount2 = memAreaSum2 + nautilus::val<uint64_t>(resultType.getSizeInBytesWithoutNull());
        const auto isNull2 = readNull(aggregationState2);
        const auto sum2 = VarVal::readVarValFromMemory(memAreaSum2, resultType, nautilus::val<bool>(false));
        const auto count2 = VarVal::readNonNullableVarValFromMemory(memAreaCount2, countType);

        /// Combining the sum and count
        const auto newSum = (sum1 + sum2).castToType(resultType.type);
        const auto newCount = count1 + count2;

        /// Writing the new sum, count and null back to the first aggregation state
        newSum.writeToMemory(memAreaSum1);
        newCount.writeToMemory(memAreaCount1);

        /// SQL-standard: only stay null when both partitions are still empty
        storeNull(aggregationState1, isNull1 and isNull2);
    }
    else
    {
        /// Reading the sum and count from the first aggregation state
        const auto memAreaSum1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
        const auto memAreaCount1 = memAreaSum1 + nautilus::val<uint64_t>(resultType.getSizeInBytesWithoutNull());
        const auto sum1 = VarVal::readNonNullableVarValFromMemory(memAreaSum1, resultType);
        const auto count1 = VarVal::readNonNullableVarValFromMemory(memAreaCount1, countType);

        /// Reading the sum and count from the second aggregation state
        const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
        const auto memAreaCount2 = memAreaSum2 + nautilus::val<uint64_t>(resultType.getSizeInBytesWithoutNull());
        const auto sum2 = VarVal::readNonNullableVarValFromMemory(memAreaSum2, resultType);
        const auto count2 = VarVal::readNonNullableVarValFromMemory(memAreaCount2, countType);

        /// Combining the sum and count
        const auto newSum = (sum1 + sum2).castToType(resultType.type);
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
        /// If no non-null value was observed the running count is zero. Short-circuit to NULL to avoid a divide-by-zero
        /// and to honour SQL-standard semantics for AVG over an empty (or all-NULL) input set.
        const auto isNull = readNull(aggregationState);
        if (isNull)
        {
            const VarVal nullResult{nautilus::val<uint64_t>(0), true, nautilus::val<bool>(true)};
            return Record({{resultFieldIdentifier, nullResult.castToType(resultType.type)}});
        }

        /// Reading the sum and count from the aggregation state
        const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
        const auto memAreaCount = memAreaSum + nautilus::val<uint64_t>(resultType.getSizeInBytesWithoutNull());
        const auto sum = VarVal::readVarValFromMemory(memAreaSum, resultType, nautilus::val<bool>(false));
        const auto count = VarVal::readNonNullableVarValFromMemory(memAreaCount, countType);

        /// Calculating the average and returning a record with the result
        const auto avg = sum / count.castToType(resultType.type);
        return Record({{resultFieldIdentifier, avg}});
    }

    /// Reading the sum and count from the aggregation state
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaCount = memAreaSum + nautilus::val<uint64_t>(resultType.getSizeInBytesWithoutNull());
    const auto sum = VarVal::readNonNullableVarValFromMemory(memAreaSum, resultType);
    const auto count = VarVal::readNonNullableVarValFromMemory(memAreaCount, countType);

    /// Calculating the average and returning a record with the result
    const auto avg = sum / count.castToType(resultType.type);
    return Record({{resultFieldIdentifier, avg}});
}

void AvgAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the isNull, sum, and count to 0
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    nautilus::memset(memArea, 0, getSizeOfStateInBytes());

    /// Initialize the null flag to "no value seen yet"; it flips to false on the first non-null input
    if (inputType.nullable)
    {
        storeNull(aggregationState, true);
    }
}

void AvgAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t AvgAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Size of isNull + size of the sum value (accumulated in resultType) + size of the count value
    const auto sumSize = resultType.getSizeInBytesWithoutNull();
    const auto countTypeSize = countType.getSizeInBytesWithoutNull();
    return (inputType.nullable ? sizeof(bool) : 0) + sumSize + countTypeSize;
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
