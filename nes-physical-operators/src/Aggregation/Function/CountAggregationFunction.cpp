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
#include <Aggregation/Function/CountAggregationFunction.hpp>
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

CountAggregationFunction::CountAggregationFunction(
    std::unique_ptr<PhysicalType> inputType,
    std::unique_ptr<PhysicalType> resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

void CountAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider&, const Nautilus::Record&)
{
    /// Reading the old count from the aggregation state.
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, *inputType);

    /// Updating the count with the new value
    const auto newCount = count + nautilus::val<uint64_t>(1);

    /// Writing the new count and count back to the aggregation state
    newCount.writeToMemory(memAreaCount);
}

void CountAggregationFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the count from the first aggregation state
    const auto memAreaCount1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto count1 = Nautilus::VarVal::readVarValFromMemory(memAreaCount1, *inputType);

    /// Reading the count from the second aggregation state
    const auto memAreaCount2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto count2 = Nautilus::VarVal::readVarValFromMemory(memAreaCount2, *inputType);

    /// Adding the counts together
    const auto newCount = count1 + count2;

    /// Writing the new count back to the first aggregation state
    newCount.writeToMemory(memAreaCount1);
}

Nautilus::Record CountAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the count from the aggregation state
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, *inputType);

    /// Creating a record with the count
    Nautilus::Record record;
    record.write(resultFieldIdentifier, count);

    return record;
}

void CountAggregationFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the count and count to 0
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    nautilus::memset(memArea, 0, getSizeOfStateInBytes());
}

void CountAggregationFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t CountAggregationFunction::getSizeOfStateInBytes() const
{
    return inputType->size();
}

}
