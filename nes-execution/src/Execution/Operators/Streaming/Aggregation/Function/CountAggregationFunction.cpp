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
#include <Execution/Operators/Streaming/Aggregation/Function/CountAggregationFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/std/cstring.h>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Aggregation
{

CountAggregationFunction::CountAggregationFunction(
    std::shared_ptr<PhysicalType> inputType,
    std::shared_ptr<PhysicalType> resultType,
    std::unique_ptr<Functions::Function> inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    const bool includeNullValues)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , includeNullValues(includeNullValues)
{
}

void CountAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record)
{
    /// Reading the value from the record
    if (const auto value = inputFunction->execute(record, pipelineMemoryProvider.arena);
        inputType->type->nullable && not includeNullValues && value.isNull())
    {
        /// If the value is null and we are not taking null values into account, we do not update the count.
        return;
    }


    /// Reading the old count from the aggregation state.
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, *resultType);

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
    const auto count1 = Nautilus::VarVal::readVarValFromMemory(memAreaCount1, *resultType);

    /// Reading the count from the second aggregation state
    const auto memAreaCount2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto count2 = Nautilus::VarVal::readVarValFromMemory(memAreaCount2, *resultType);

    /// Adding the counts together
    const auto newCount = count1 + count2;

    /// Writing the new count back to the first aggregation state
    newCount.writeToMemory(memAreaCount1);
}

Nautilus::Record CountAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the count from the aggregation state
    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto count = Nautilus::VarVal::readVarValFromMemory(memAreaCount, *resultType);

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

size_t CountAggregationFunction::getSizeOfStateInBytes() const
{
    return resultType->getSizeInBytes();
}

}
