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
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

MinAggregationPhysicalFunction::MinAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

void MinAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);

    if (not inputType.nullable)
    {
        /// Reading the old min value from the aggregation state.
        const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
        const auto min = VarVal::readNonNullableVarValFromMemory(memAreaMin, inputType);

        /// Updating the min value with the new value, if the new value is smaller
        const auto newMin = VarVal::select((value < min).cast<nautilus::val<bool>>(), value, min);
        newMin.writeToMemory(memAreaMin);
    }
    else
    {
        /// Reading the old min value from the aggregation state. We need to skip the first byte as null
        const auto isNull = readNull(aggregationState);
        const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
        const auto min = VarVal::readVarValFromMemory(memAreaMin, inputType, isNull);

        /// If value is null or min is smaller than value --> keep the current min
        const auto valueOrMin = VarVal::select(value.isNull(), min, value);
        const auto newMin = VarVal::select((valueOrMin < min).cast<nautilus::val<bool>>(), valueOrMin, min);
        newMin.writeToMemory(memAreaMin);

        /// Updating the null value
        const auto newIsNull = isNull or value.isNull();
        storeNull(aggregationState, newIsNull);
    }
}

void MinAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    if (not inputType.nullable)
    {
        /// Reading the old min value from the aggregation state.
        const auto memAreaMin1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
        const auto memAreaMin2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
        const auto min1 = VarVal::readNonNullableVarValFromMemory(memAreaMin1, inputType);
        const auto min2 = VarVal::readNonNullableVarValFromMemory(memAreaMin2, inputType);

        /// Updating the min value with the new value, if the new value is smaller
        const auto newMin = VarVal::select((min1 < min2).cast<nautilus::val<bool>>(), min1, min2);
        newMin.writeToMemory(memAreaMin1);
    }
    else
    {
        /// Reading the old min value from the aggregation state. We need to skip the first byte as null
        const auto isNull1 = readNull(aggregationState1);
        const auto isNull2 = readNull(aggregationState2);
        const auto memAreaMin1 = static_cast<nautilus::val<int8_t*>>(aggregationState1 + nautilus::val<uint64_t>{1});
        const auto memAreaMin2 = static_cast<nautilus::val<int8_t*>>(aggregationState2 + nautilus::val<uint64_t>{1});
        const auto min1 = VarVal::readVarValFromMemory(memAreaMin1, inputType, isNull1);
        const auto min2 = VarVal::readVarValFromMemory(memAreaMin2, inputType, isNull2);

        /// Updating the null value
        const auto newIsNull = isNull1 or isNull2;
        storeNull(aggregationState1, newIsNull);

        /// If min1 or min2 is null, the result is null. Otherwise, it is the minimum of min1 and min2
        const auto min1OrMin2 = VarVal::select((min1 < min2).cast<nautilus::val<bool>>(), min1, min2);
        const auto newMin = VarVal::select(newIsNull, min1, min1OrMin2);
        newMin.writeToMemory(memAreaMin1);
    }
}

Record MinAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    if (not inputType.nullable)
    {
        /// Reading the min value from the aggregation state
        const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState);
        const auto min = VarVal::readNonNullableVarValFromMemory(memAreaMin, inputType);

        /// Creating a record with the min value
        Record record;
        record.write(resultFieldIdentifier, min);
        return record;
    }

    /// Reading the min value from the aggregation state
    const auto isNull = readNull(aggregationState);
    const auto memAreaMin = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
    const auto min = VarVal::readVarValFromMemory(memAreaMin, inputType, isNull);

    /// Creating a record with the min value
    Record record;
    record.write(resultFieldIdentifier, min);
    return record;
}

void MinAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the null value
    if (inputType.nullable)
    {
        storeNull(aggregationState, false);
    }

    /// Resetting the min value to the maximum value
    const auto memAreaMin
        = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)});
    const auto min = createNautilusMaxValue(inputType.type);
    min.writeToMemory(memAreaMin);
}

void MinAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t MinAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return inputType.getSizeInBytesWithNull();
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMinAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<MinAggregationPhysicalFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

}
