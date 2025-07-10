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
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <nautilus/DataTypes/VarVal.hpp>
#include <nautilus/Interface/Record.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/Util.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
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
    /// Reading old min, max, and count from the aggregation state.
    /// Memory layout: [min] [max] [count]
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(inputType.getSizeInBytes());
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(2 * inputType.getSizeInBytes());
    
    const auto min = Nautilus::VarVal::readVarValFromMemory(memAreaMin, inputType.type);
    const auto max = Nautilus::VarVal::readVarValFromMemory(memAreaMax, inputType.type);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, countType.type);

    /// Getting the new value and updating the aggregates
    const auto value = inputFunction.execute(record, executionContext.pipelineMemoryProvider.arena);
    
    /// Update min if value is smaller
    if (value < min)
    {
        value.writeToMemory(memAreaMin);
    }
    
    /// Update max if value is larger
    if (value > max)
    {
        value.writeToMemory(memAreaMax);
    }
    
    /// Increment count
    const auto newCount = count + nautilus::val<uint64_t>(1);
    newCount.writeToMemory(memAreaCount);
}

void VarAggregationFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the aggregates from the first aggregation state
    const auto memAreaMin1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto memAreaMax1 = static_cast<nautilus::val<int8_t*>>(aggregationState1) + nautilus::val<uint64_t>(inputType.getSizeInBytes());
    const auto memAreaCount1 = static_cast<nautilus::val<int8_t*>>(aggregationState1) + nautilus::val<uint64_t>(2 * inputType.getSizeInBytes());
    
    const auto min1 = Nautilus::VarVal::readVarValFromMemory(memAreaMin1, inputType.type);
    const auto max1 = Nautilus::VarVal::readVarValFromMemory(memAreaMax1, inputType.type);
    const auto count1 = Nautilus::VarVal::readVarValFromMemory(memAreaCount1, countType.type);

    /// Reading the aggregates from the second aggregation state
    const auto memAreaMin2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto memAreaMax2 = static_cast<nautilus::val<int8_t*>>(aggregationState2) + nautilus::val<uint64_t>(inputType.getSizeInBytes());
    const auto memAreaCount2 = static_cast<nautilus::val<int8_t*>>(aggregationState2) + nautilus::val<uint64_t>(2 * inputType.getSizeInBytes());
    
    const auto min2 = Nautilus::VarVal::readVarValFromMemory(memAreaMin2, inputType.type);
    const auto max2 = Nautilus::VarVal::readVarValFromMemory(memAreaMax2, inputType.type);
    const auto count2 = Nautilus::VarVal::readVarValFromMemory(memAreaCount2, countType.type);

    /// Combining the aggregates
    /// Update min if min2 is smaller
    if (min2 < min1)
    {
        min2.writeToMemory(memAreaMin1);
    }
    
    /// Update max if max2 is larger
    if (max2 > max1)
    {
        max2.writeToMemory(memAreaMax1);
    }
    
    /// Combine counts
    const auto newCount = count1 + count2;
    newCount.writeToMemory(memAreaCount1);
}

Nautilus::Record VarAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the aggregates from the aggregation state
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(inputType.getSizeInBytes());
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(2 * inputType.getSizeInBytes());
    
    const auto min = Nautilus::VarVal::readVarValFromMemory(memAreaMin, inputType.type);
    const auto max = Nautilus::VarVal::readVarValFromMemory(memAreaMax, inputType.type);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, countType.type);

    /// Calculate variance as max - min
    /// Cast to result type for consistency
    const auto minFloat = min.castToType(resultType.type);
    const auto maxFloat = max.castToType(resultType.type);
    const auto variance = maxFloat - minFloat;
    
    return Nautilus::Record({{resultFieldIdentifier, variance}});
}

void VarAggregationFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reset min to maximum value, max to minimum value, and count to 0
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(inputType.getSizeInBytes());
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(2 * inputType.getSizeInBytes());
    
    /// Initialize min with max value
    const auto minInit = Nautilus::Util::createNautilusMaxValue(inputType.type);
    minInit.writeToMemory(memAreaMin);
    
    /// Initialize max with min value
    const auto maxInit = Nautilus::Util::createNautilusMinValue(inputType.type);
    maxInit.writeToMemory(memAreaMax);
    
    /// Initialize count to 0 using memset
    nautilus::memset(memAreaCount, 0, countType.getSizeInBytes());
}

void VarAggregationFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t VarAggregationFunction::getSizeOfStateInBytes() const
{
    /// Size of min + size of max + size of count
    /// We need to store min and max as the same type as input, and count as countType
    const auto inputSize = inputType.getSizeInBytes();
    const auto countTypeSize = countType.getSizeInBytes();
    return 2 * inputSize + countTypeSize; // min + max + count
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterVarAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    /// VAR requires a count type (UINT64) for tracking the number of elements
    auto countType = DataTypeProvider::provideDataType(DataType::Type::UINT64);
    return std::make_shared<VarAggregationFunction>(
        std::move(arguments.inputType), 
        std::move(arguments.resultType), 
        arguments.inputFunction, 
        arguments.resultFieldIdentifier,
        std::move(countType));
}

}