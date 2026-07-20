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
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ExecutionContext.hpp>
#include <Util.hpp>
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
    const nautilus::val<AggregationState*>& aggregationState,
    nautilus::val<TupleBuffer*>,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);

    if (not inputType.nullable)
    {
        /// Reading the old max value from the aggregation state.
        const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState);
        const auto max = VarVal::readNonNullableVarValFromMemory(memAreaMax, inputType);

        /// Updating the max value with the new value, if the new value is smaller
        const auto newMax = VarVal::select((value > max).getRawValueAs<nautilus::val<bool>>(), value, max);
        newMax.writeToMemory(memAreaMax);
    }
    else
    {
        /// Reading the old max value from the aggregation state. We need to skip the first byte as null
        const auto isNull = readNull(aggregationState);
        const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
        const auto max = VarVal::readVarValFromMemory(memAreaMax, inputType, isNull);

        /// SQL-standard: skip NULL inputs
        const auto valueIsNotNull = not value.isNull();
        const auto adoptValue = valueIsNotNull and (isNull or (value > max).getRawValueAs<nautilus::val<bool>>());
        const auto newMax = VarVal::select(adoptValue, value, max);
        newMax.writeToMemory(memAreaMax);

        /// Stay null only while every input has been NULL
        const auto newIsNull = isNull and value.isNull();
        storeNull(aggregationState, newIsNull);
    }
}

void MaxAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<TupleBuffer*>,
    const nautilus::val<AggregationState*> aggregationState2,
    nautilus::val<TupleBuffer*>,
    PipelineMemoryProvider&)
{
    if (not inputType.nullable)
    {
        /// Reading the old max value from the aggregation state.
        const auto memAreaMax1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
        const auto memAreaMax2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
        const auto max1 = VarVal::readNonNullableVarValFromMemory(memAreaMax1, inputType);
        const auto max2 = VarVal::readNonNullableVarValFromMemory(memAreaMax2, inputType);

        /// Updating the max value with the new value, if the new value is smaller
        const auto newMax = VarVal::select((max1 > max2).getRawValueAs<nautilus::val<bool>>(), max1, max2);
        newMax.writeToMemory(memAreaMax1);
    }
    else
    {
        /// Reading the old max value from the aggregation state. We need to skip the first byte as null
        const auto isNull1 = readNull(aggregationState1);
        const auto isNull2 = readNull(aggregationState2);
        const auto memAreaMax1 = static_cast<nautilus::val<int8_t*>>(aggregationState1 + nautilus::val<uint64_t>{1});
        const auto memAreaMax2 = static_cast<nautilus::val<int8_t*>>(aggregationState2 + nautilus::val<uint64_t>{1});
        const auto max1 = VarVal::readVarValFromMemory(memAreaMax1, inputType, isNull1);
        const auto max2 = VarVal::readVarValFromMemory(memAreaMax2, inputType, isNull2);

        /// SQL-standard: only stay null when both partitions are still empty
        const auto newIsNull = isNull1 and isNull2;
        storeNull(aggregationState1, newIsNull);

        /// If state1 is empty take max2, if state2 is empty take max1, otherwise take the pairwise max
        const auto max1OrMax2 = VarVal::select((max1 > max2).getRawValueAs<nautilus::val<bool>>(), max1, max2);
        const auto newMax = VarVal::select(isNull1, max2, VarVal::select(isNull2, max1, max1OrMax2));
        newMax.writeToMemory(memAreaMax1);
    }
}

Record MaxAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, nautilus::val<TupleBuffer*>, PipelineMemoryProvider&)
{
    if (not inputType.nullable)
    {
        /// Reading the max value from the aggregation state
        const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState);
        const auto max = VarVal::readNonNullableVarValFromMemory(memAreaMax, inputType);

        /// Creating a record with the max value
        Record record;
        record.write(resultFieldIdentifier, max);
        return record;
    }

    /// Reading the max value from the aggregation state
    const auto isNull = readNull(aggregationState);
    const auto memAreaMax = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
    const auto max = VarVal::readVarValFromMemory(memAreaMax, inputType, isNull);

    /// Creating a record with the max value
    Record record;
    record.write(resultFieldIdentifier, max);
    return record;
}

void MaxAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState, nautilus::val<TupleBuffer*>, PipelineMemoryProvider&)
{
    /// Initialize the null flag to "no value seen yet" so the first non-null input becomes the running max
    if (inputType.nullable)
    {
        storeNull(aggregationState, true);
    }

    /// Resetting the max value to the minimum value
    const auto memAreaMax
        = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)});
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
