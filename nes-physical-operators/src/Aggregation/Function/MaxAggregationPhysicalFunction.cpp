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
#include <select.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
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
    /// If the value is null, we need to set the multiplication factor
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto multiplicationFactor
        = nautilus::select(inputType.isNullableAsBool() and value.isNull(), nautilus::val<int8_t>{0}, nautilus::val<int8_t>{1});


    /// Reading the old max value from the aggregation state.
    const auto isNull = inputType.isNullableAsBool() ? readNull(aggregationState) : nautilus::val<bool>{false};
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(
        aggregationState + nautilus::val<uint64_t>{static_cast<unsigned long>(inputType.isNullableAsBool())});
    const auto max = VarVal::readVarValFromMemory(memAreaMax, inputType, isNull);
    const auto newIsNull = inputType.isNullableAsBool()
        & (static_cast<nautilus::val<int>>(static_cast<int>(value.isNull())) | static_cast<nautilus::val<int>>(static_cast<int>(isNull)));

    /// Updating the max value with the new value, if the new value is larger
    if ((value * multiplicationFactor) > max)
    {
        value.writeToMemory(memAreaMax);
    }
    inputType.isNullableAsBool() ? storeNull(aggregationState, newIsNull), void() : void();
}

void MaxAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Reading the max value from the first aggregation state
    const auto isNull1 = inputType.isNullableAsBool() ? readNull(aggregationState1) : nautilus::val<bool>{false};
    const auto memAreaMax1 = static_cast<nautilus::val<int8_t*>>(
        aggregationState1 + nautilus::val<uint64_t>{static_cast<unsigned long>(inputType.isNullableAsBool())});
    const auto max1 = VarVal::readVarValFromMemory(memAreaMax1, inputType, isNull1);

    /// Reading the max value from the second aggregation state
    const auto isNull2 = inputType.isNullableAsBool() ? readNull(aggregationState2) : nautilus::val<bool>{false};
    const auto memAreaMax2 = static_cast<nautilus::val<int8_t*>>(
        aggregationState2 + nautilus::val<uint64_t>{static_cast<unsigned long>(inputType.isNullableAsBool())});
    const auto max2 = VarVal::readVarValFromMemory(memAreaMax2, inputType, isNull2);

    /// Updating the max value with the new value, if the new value is larger
    if (max2 > max1)
    {
        max2.writeToMemory(memAreaMax1);
    }

    /// Creating the new null value and writing it
    const auto newNull = inputType.isNullableAsBool() ? max1.isNull() or max2.isNull() : nautilus::val<bool>{false};
    inputType.isNullableAsBool() ? storeNull(aggregationState1, static_cast<nautilus::val<bool>>(newNull)), void() : void();
}

Record MaxAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Reading the max value from the aggregation state
    const auto isNull = inputType.isNullableAsBool() ? readNull(aggregationState) : nautilus::val<bool>{false};
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(
        aggregationState + nautilus::val<uint64_t>{static_cast<unsigned long>(inputType.isNullableAsBool())});
    const auto max = VarVal::readVarValFromMemory(memAreaMax, inputType, isNull);

    /// Creating a record with the max value
    Record record;
    record.write(resultFieldIdentifier, max);
    return record;
}

void MaxAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the null value
    inputType.isNullableAsBool() ? storeNull(aggregationState, nautilus::val<bool>{false}), void() : void();

    /// Resetting the max value to the minimum value
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(
        aggregationState + nautilus::val<uint64_t>{static_cast<unsigned long>(inputType.isNullableAsBool())});
    const auto max = createNautilusMinValue(inputType.type);
    max.writeToMemory(memAreaMax);
}

void MaxAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t MaxAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return inputType.getSizeInBytesWithNull();
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMaxAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<MaxAggregationPhysicalFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

}
