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
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

SumAggregationPhysicalFunction::SumAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

void SumAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, ExecutionContext& pipelineMemoryProvider, const Nautilus::Record& record)
{
    /// Reading the old sum from the aggregation state.
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto sum = Nautilus::VarVal::readVarValFromMemory(memAreaSum, inputType.type);

    /// Updating the sum and count with the new value
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.pipelineMemoryProvider.arena);
    const auto newSum = sum + value;

    /// Writing the new sum and count back to the aggregation state
    newSum.writeToMemory(memAreaSum);
}

void SumAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the sum from the first aggregation state
    const auto memAreaSum1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto sum1 = Nautilus::VarVal::readVarValFromMemory(memAreaSum1, inputType.type);

    /// Reading the sum from the second aggregation state
    const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto sum2 = VarVal::readVarValFromMemory(memAreaSum2, inputType.type);

    /// Adding the sums together
    const auto newSum = sum1 + sum2;

    /// Writing the new sum back to the first aggregation state
    newSum.writeToMemory(memAreaSum1);
}

Record SumAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the sum from the aggregation state
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto sum = VarVal::readVarValFromMemory(memAreaSum, inputType.type);

    /// Creating a record with the sum
    Nautilus::Record record;
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
    return inputType.getSizeInBytes();
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterSumAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<SumAggregationPhysicalFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

}
