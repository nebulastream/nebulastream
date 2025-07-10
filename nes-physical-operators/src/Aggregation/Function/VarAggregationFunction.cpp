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
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/VarAggregationFunction.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/std/cstring.h>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

VarAggregationFunction::VarAggregationFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    DataType countType)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , countType(std::move(countType))
{
}

void VarAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    ExecutionContext& executionContext,
    const Nautilus::Record& record)
{
    /// Reading old sum, sum of squares, and count from the aggregation state.
    /// Memory layout: [sum] [sum_of_squares] [count]
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaSumSq = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(inputType.getSizeInBytes());
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(2 * inputType.getSizeInBytes());
    
    const auto sum = Nautilus::VarVal::readVarValFromMemory(memAreaSum, inputType.type);
    const auto sumSq = Nautilus::VarVal::readVarValFromMemory(memAreaSumSq, inputType.type);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, countType.type);

    /// Getting the new value and updating the aggregates
    const auto value = inputFunction.execute(record, executionContext.pipelineMemoryProvider.arena);
    const auto newSum = sum + value;
    const auto newSumSq = sumSq + (value * value);
    const auto newCount = count + nautilus::val<uint64_t>(1);

    /// Writing the new aggregates back to the aggregation state
    newSum.writeToMemory(memAreaSum);
    newSumSq.writeToMemory(memAreaSumSq);
    newCount.writeToMemory(memAreaCount);
}

void VarAggregationFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the aggregates from the first aggregation state
    const auto memAreaSum1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto memAreaSumSq1 = static_cast<nautilus::val<int8_t*>>(aggregationState1) + nautilus::val<uint64_t>(inputType.getSizeInBytes());
    const auto memAreaCount1 = static_cast<nautilus::val<int8_t*>>(aggregationState1) + nautilus::val<uint64_t>(2 * inputType.getSizeInBytes());
    
    const auto sum1 = Nautilus::VarVal::readVarValFromMemory(memAreaSum1, inputType.type);
    const auto sumSq1 = Nautilus::VarVal::readVarValFromMemory(memAreaSumSq1, inputType.type);
    const auto count1 = Nautilus::VarVal::readVarValFromMemory(memAreaCount1, countType.type);

    /// Reading the aggregates from the second aggregation state
    const auto memAreaSum2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto memAreaSumSq2 = static_cast<nautilus::val<int8_t*>>(aggregationState2) + nautilus::val<uint64_t>(inputType.getSizeInBytes());
    const auto memAreaCount2 = static_cast<nautilus::val<int8_t*>>(aggregationState2) + nautilus::val<uint64_t>(2 * inputType.getSizeInBytes());
    
    const auto sum2 = Nautilus::VarVal::readVarValFromMemory(memAreaSum2, inputType.type);
    const auto sumSq2 = Nautilus::VarVal::readVarValFromMemory(memAreaSumSq2, inputType.type);
    const auto count2 = Nautilus::VarVal::readVarValFromMemory(memAreaCount2, countType.type);

    /// Combining the aggregates
    const auto newSum = sum1 + sum2;
    const auto newSumSq = sumSq1 + sumSq2;
    const auto newCount = count1 + count2;

    /// Writing the new aggregates back to the first aggregation state
    newSum.writeToMemory(memAreaSum1);
    newSumSq.writeToMemory(memAreaSumSq1);
    newCount.writeToMemory(memAreaCount1);
}

Nautilus::Record VarAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the aggregates from the aggregation state
    const auto memAreaSum = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaSumSq = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(inputType.getSizeInBytes());
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(2 * inputType.getSizeInBytes());
    
    const auto sum = Nautilus::VarVal::readVarValFromMemory(memAreaSum, inputType.type);
    const auto sumSq = Nautilus::VarVal::readVarValFromMemory(memAreaSumSq, inputType.type);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, countType.type);

    /// Calculating the variance using the formula: Var = (SumSq - (Sum^2 / N)) / N
    /// Cast to result type for precision
    const auto countFloat = count.castToType(resultType.type);
    const auto sumFloat = sum.castToType(resultType.type);
    const auto sumSqFloat = sumSq.castToType(resultType.type);
    
    // Calculate sample variance using Bessel's correction
    // Sample variance = (sum(x^2) - (sum(x))^2/n) / (n-1)
    const auto mean = sumFloat / countFloat;
    const auto one = Nautilus::VarVal(uint64_t(1)).castToType(resultType.type);
    const auto n_minus_1 = countFloat - one;
    const auto variance = ((sumSqFloat - (sumFloat * sumFloat) / countFloat) / n_minus_1);
    
    return Nautilus::Record({{resultFieldIdentifier, variance}});
}

void VarAggregationFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting all aggregates to 0
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    nautilus::memset(memArea, 0, getSizeOfStateInBytes());
}

void VarAggregationFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t VarAggregationFunction::getSizeOfStateInBytes() const
{
    /// Size of sum + size of sum of squares + size of count
    /// We need to store sum and sum_of_squares as the same type as input, and count as countType
    const auto inputSize = inputType.getSizeInBytes();
    const auto countTypeSize = countType.getSizeInBytes();
    return 2 * inputSize + countTypeSize; // sum + sumSq + count
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterVarAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    /// Variance aggregation expects INT64 as count type
    DataType countType = DataTypeProvider::provideDataType(DataType::Type::INT64);
    return std::make_shared<VarAggregationFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier, std::move(countType));
}

}