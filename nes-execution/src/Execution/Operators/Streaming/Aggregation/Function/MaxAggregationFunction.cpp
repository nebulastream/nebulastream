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
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/MaxAggregationFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Util.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::Aggregation
{

MaxAggregationFunction::MaxAggregationFunction(
    PhysicalType inputType,
    PhysicalType resultType,
    std::unique_ptr<Functions::Function> inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

void MaxAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record)
{
    /// Reading the old max value from the aggregation state.
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto max = Nautilus::VarVal::readVarValFromMemory(memAreaMax, inputType.type);

    /// Updating the max value with the new value, if the new value is larger
    const auto value = inputFunction->execute(record, pipelineMemoryProvider.arena);
    if (value > max)
    {
        value.writeToMemory(memAreaMax);
    }
}

void MaxAggregationFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the max value from the first aggregation state
    const auto memAreaMax1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto max1 = Nautilus::VarVal::readVarValFromMemory(memAreaMax1, inputType.type);

    /// Reading the max value from the second aggregation state
    const auto memAreaMax2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto max2 = Nautilus::VarVal::readVarValFromMemory(memAreaMax2, inputType.type);

    /// Updating the max value with the new value, if the new value is larger
    if (max2 > max1)
    {
        max2.writeToMemory(memAreaMax1);
    }
}

Nautilus::Record MaxAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the max value from the aggregation state
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto max = Nautilus::VarVal::readVarValFromMemory(memAreaMax, inputType.type);

    /// Creating a record with the max value
    Nautilus::Record record;
    record.write(resultFieldIdentifier, max);
    return record;
}

void MaxAggregationFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the max value to the minimum value
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto max = Nautilus::Util::createNautilusMinValue(inputType.type);
    max.writeToMemory(memAreaMax);
}

size_t MaxAggregationFunction::getSizeOfStateInBytes() const
{
    return inputType.getSizeInBytes();
}

}
