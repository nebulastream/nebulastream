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

#include <Aggregation/Function/MinAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Util.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ExecutionContext.hpp>
#include <val_ptr.hpp>

namespace NES
{

MinAggregationPhysicalFunction::MinAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

void MinAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, ExecutionContext& executionContext, const Record& record)
{
    /// Reading the old min value from the aggregation state.
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto min = VarVal::readVarValFromMemory(memAreaMin, inputType.type);

    /// Updating the min value with the new value, if the new value is smaller
    const auto value = inputFunction.execute(record, executionContext.pipelineMemoryProvider.arena);
    if (value < min)
    {
        value.writeToMemory(memAreaMin);
    }
}

void MinAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the min value from the first aggregation state
    const auto memAreaMin1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto min1 = VarVal::readVarValFromMemory(memAreaMin1, inputType.type);

    /// Reading the min value from the second aggregation state
    const auto memAreaMin2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto min2 = VarVal::readVarValFromMemory(memAreaMin2, inputType.type);

    /// Updating the min value with the new min value, if the new min value is smaller
    if (min2 < min1)
    {
        min2.writeToMemory(memAreaMin1);
    }
}

Nautilus::Record MinAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the min value from the aggregation state
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto min = VarVal::readVarValFromMemory(memAreaMin, inputType.type);

    /// Creating a record with the min value
    Nautilus::Record record;
    record.write(resultFieldIdentifier, min);

    return record;
}

void MinAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the min value to the maximum value
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto min = Nautilus::Util::createNautilusMaxValue(inputType.type);
    min.writeToMemory(memAreaMin);
}

void MinAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t MinAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return inputType.getSizeInBytes();
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMinAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<MinAggregationPhysicalFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

}
