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

#include <Aggregation/Function/MaxAggregationPhysicalFunction.hpp>

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
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

MaxAggregationPhysicalFunction::MaxAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

void MaxAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    /// If the value is null and we are taking null values into account
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);

    /// Reading the old max value from the aggregation state.
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState);
    auto max = VarVal::readVarValFromMemory(memAreaMax, inputType, false);

    if (inputType.isNullable)
    {
        max = varValSelect(value.isNull(), max, varValSelect((max > value).cast<nautilus::val<bool>>(), max, value));
    }
    else
    {
        max = varValSelect((max > value).cast<nautilus::val<bool>>(), max, value);
    }

    max.writeToMemory(memAreaMax);
}

void MaxAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the max value from the first aggregation state
    const auto memAreaMax1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto max1 = VarVal::readVarValFromMemory(memAreaMax1, inputType, false);

    /// Reading the max value from the second aggregation state
    const auto memAreaMax2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto max2 = VarVal::readVarValFromMemory(memAreaMax2, inputType, false);

    varValSelect((max1 > max2).cast<nautilus::val<bool>>(), max1, max2).writeToMemory(memAreaMax1);
}

Record MaxAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the max value from the aggregation state
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto max = VarVal::readVarValFromMemory(memAreaMax, inputType, false);

    /// Creating a record with the max value
    Record record;
    record.write(resultFieldIdentifier, max);
    return record;
}

void MaxAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the max value to the minimum value
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto max = createNautilusMinValue(inputType.type);
    max.writeToMemory(memAreaMax);
}

void MaxAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t MaxAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return inputType.getSizeInBytes();
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMaxAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<MaxAggregationPhysicalFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

}
