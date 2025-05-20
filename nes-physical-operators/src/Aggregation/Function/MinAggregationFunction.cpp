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
#include <Aggregation/Function/MinAggregationFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Util.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ExecutionContext.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES
{

MinAggregationFunction::MinAggregationFunction(
    std::unique_ptr<PhysicalType> inputType,
    std::unique_ptr<PhysicalType> resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

void MinAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    /// Reading the old min value from the aggregation state.
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto min = VarVal::readVarValFromMemory(memAreaMin, *inputType);

    /// Updating the min value with the new value, if the new value is smaller
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (value < min)
    {
        value.writeToMemory(memAreaMin);
    }
}

void MinAggregationFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the min value from the first aggregation state
    const auto memAreaMin1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto min1 = VarVal::readVarValFromMemory(memAreaMin1, *inputType);

    /// Reading the min value from the second aggregation state
    const auto memAreaMin2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto min2 = VarVal::readVarValFromMemory(memAreaMin2, *inputType);

    /// Updating the min value with the new min value, if the new min value is smaller
    if (min2 < min1)
    {
        min2.writeToMemory(memAreaMin1);
    }
}

Nautilus::Record MinAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the min value from the aggregation state
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto min = VarVal::readVarValFromMemory(memAreaMin, *inputType);

    /// Creating a record with the min value
    Nautilus::Record record;
    record.write(resultFieldIdentifier, min);

    return record;
}

void MinAggregationFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the min value to the maximum value
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto min = Nautilus::Util::createNautilusMaxValue(*inputType);
    min.writeToMemory(memAreaMin);
}

void MinAggregationFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t MinAggregationFunction::getSizeOfStateInBytes() const
{
    return inputType->size();
}

}
