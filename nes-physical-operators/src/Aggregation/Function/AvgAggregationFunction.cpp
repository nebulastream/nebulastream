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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <Aggregation/Function/AggregationFunction.hpp>
#include <Aggregation/Function/AvgAggregationFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/std/cstring.h>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES
{

AvgAggregationFunction::AvgAggregationFunction(
    std::unique_ptr<PhysicalType> inputType,
    std::unique_ptr<PhysicalType> resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::unique_ptr<PhysicalType> countType)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , countType(std::move(countType))
{
}

void AvgAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record)
{
    /// Reading old sum and count from the aggregation state. The sum is stored at the beginning of the aggregation state and the count is stored after the sum
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(inputType->size());
    const auto sum = Nautilus::VarVal::readVarValFromMemory(memAreaSum, *inputType);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, *countType);

    /// Updating the sum and count with the new value
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto newSum = sum + value;
    const auto newCount = count + nautilus::val<uint64_t>(1);

    /// Writing the new sum and count back to the aggregation state
    newSum.writeToMemory(memAreaSum);
    newCount.writeToMemory(memAreaCount);
}

void AvgAggregationFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the sum and count from the first aggregation state
    const auto memAreaSum1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto memAreaCount1 = static_cast<nautilus::val<int8_t*>>(aggregationState1) + nautilus::val<uint64_t>(inputType->size());
    const auto sum1 = Nautilus::VarVal::readVarValFromMemory(memAreaSum1, *inputType);
    const auto count1 = Nautilus::VarVal::readVarValFromMemory(memAreaCount1, *countType);

    /// Reading the sum and count from the second aggregation state
    const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto memAreaCount2 = static_cast<nautilus::val<int8_t*>>(aggregationState2) + nautilus::val<uint64_t>(inputType->size());
    const auto sum2 = Nautilus::VarVal::readVarValFromMemory(memAreaSum2, *inputType);
    const auto count2 = Nautilus::VarVal::readVarValFromMemory(memAreaCount2, *countType);

    /// Combining the sum and count
    const auto newSum = sum1 + sum2;
    const auto newCount = count1 + count2;

    /// Writing the new sum and count back to the first aggregation state
    newSum.writeToMemory(memAreaSum1);
    newCount.writeToMemory(memAreaCount1);
}

Nautilus::Record AvgAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the sum and count from the aggregation state
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(inputType->size());
    const auto sum = Nautilus::VarVal::readVarValFromMemory(memAreaSum, *inputType);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, *countType);

    /// Calculating the average and returning a record with the result
    const auto avg = sum.castToType(*resultType) / count.castToType(*resultType);
    return Nautilus::Record({{resultFieldIdentifier, avg}});
}

void AvgAggregationFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the sum and count to 0
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    nautilus::memset(memArea, 0, getSizeOfStateInBytes());
}

void AvgAggregationFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t AvgAggregationFunction::getSizeOfStateInBytes() const
{
    /// Size of the sum value + size of the count value
    const auto inputSize = inputType->size();
    const auto countTypeSize = countType->size();
    return inputSize + countTypeSize;
}

}
