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
    const AggregationStateRef& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);

    if (not inputType.nullable)
    {
        /// Reading the old max value from the aggregation state.
        const auto memAreaMax = aggregationState.data();
        const auto max = VarVal::readNonNullableVarValFromMemory(memAreaMax, inputType);

        /// Updating the max value with the new value, if the new value is smaller
        const auto newMax = VarVal::select((value > max).getRawValueAs<nautilus::val<bool>>(), value, max);
        newMax.writeToMemory(memAreaMax);
    }
    else
    {
        /// Reading the old max value from the aggregation state. We need to skip the first byte as null
        const auto isNull = aggregationState.readNull();
        const auto memAreaMax = aggregationState.valueData(true);
        const auto max = VarVal::readVarValFromMemory(memAreaMax, inputType, isNull);

        /// If value is null or max is smaller than value --> keep the current max
        const auto valueOrMax = VarVal::select(value.isNull(), max, value);
        const auto newMax = VarVal::select((valueOrMax > max).getRawValueAs<nautilus::val<bool>>(), valueOrMax, max);
        newMax.writeToMemory(memAreaMax);

        /// Updating the null value
        const auto newIsNull = isNull or value.isNull();
        aggregationState.storeNull(newIsNull);
    }
}

void MaxAggregationPhysicalFunction::combine(
    const AggregationStateRef& aggregationState1, const AggregationStateRef& aggregationState2, PipelineMemoryProvider&)
{
    if (not inputType.nullable)
    {
        /// Reading the old max value from the aggregation state.
        const auto memAreaMax1 = aggregationState1.data();
        const auto memAreaMax2 = aggregationState2.data();
        const auto max1 = VarVal::readNonNullableVarValFromMemory(memAreaMax1, inputType);
        const auto max2 = VarVal::readNonNullableVarValFromMemory(memAreaMax2, inputType);

        /// Updating the max value with the new value, if the new value is smaller
        const auto newMax = VarVal::select((max1 > max2).getRawValueAs<nautilus::val<bool>>(), max1, max2);
        newMax.writeToMemory(memAreaMax1);
    }
    else
    {
        /// Reading the old max value from the aggregation state. We need to skip the first byte as null
        const auto isNull1 = aggregationState1.readNull();
        const auto isNull2 = aggregationState2.readNull();
        const auto memAreaMax1 = aggregationState1.valueData(true);
        const auto memAreaMax2 = aggregationState2.valueData(true);
        const auto max1 = VarVal::readVarValFromMemory(memAreaMax1, inputType, isNull1);
        const auto max2 = VarVal::readVarValFromMemory(memAreaMax2, inputType, isNull2);

        /// Updating the null value
        const auto newIsNull = isNull1 or isNull2;
        aggregationState1.storeNull(newIsNull);

        /// If max1 or max2 is null, the result is null. Otherwise, it is the maximum of max1 and max2
        const auto max1OrMax2 = VarVal::select((max1 > max2).getRawValueAs<nautilus::val<bool>>(), max1, max2);
        const auto newMax = VarVal::select(newIsNull, max1, max1OrMax2);
        newMax.writeToMemory(memAreaMax1);
    }
}

Record MaxAggregationPhysicalFunction::lower(const AggregationStateRef& aggregationState, PipelineMemoryProvider&)
{
    if (not inputType.nullable)
    {
        /// Reading the max value from the aggregation state
        const auto memAreaMax = aggregationState.data();
        const auto max = VarVal::readNonNullableVarValFromMemory(memAreaMax, inputType);

        /// Creating a record with the max value
        Record record;
        record.write(resultFieldIdentifier, max);
        return record;
    }

    /// Reading the max value from the aggregation state
    const auto isNull = aggregationState.readNull();
    const auto memAreaMax = aggregationState.valueData(true);
    const auto max = VarVal::readVarValFromMemory(memAreaMax, inputType, isNull);

    /// Creating a record with the max value
    Record record;
    record.write(resultFieldIdentifier, max);
    return record;
}

void MaxAggregationPhysicalFunction::reset(const AggregationStateRef& aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the null value
    if (inputType.nullable)
    {
        aggregationState.storeNull(false);
    }

    /// Resetting the max value to the minimum value
    const auto memAreaMax = aggregationState.valueData(inputType.nullable);
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
