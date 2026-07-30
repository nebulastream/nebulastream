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

#include <Sample/LastSampleAggregationPhysicalFunction.hpp>

#include <memory>
#include <DataTypes/VarVal.hpp>
#include <SampleAggregationPhysicalFunctionRegistry.hpp>
#include <static.hpp>

namespace NES
{

LastSampleAggregationPhysicalFunction::LastSampleAggregationPhysicalFunction(
    const Schema<QualifiedUnboundField, Ordered>& stateSchema, Record::RecordFieldIdentifier timestampFieldName)
{
    uint64_t offset = 1;
    for (const auto& field : stateSchema)
    {
        const auto fieldIdentifier = field.getFullyQualifiedName();
        const DataType type{field.getDataType().type, DataType::NULLABLE::NOT_NULLABLE};
        fields.push_back({fieldIdentifier, type, offset});
        if (fieldIdentifier == timestampFieldName)
        {
            timestampFieldOffset = offset;
            timestampFieldType = type;
        }
        offset += type.getSizeInBytesWithNull();
    }
    stateSize = offset;
}

void LastSampleAggregationPhysicalFunction::lift(
    const nautilus::val<SampleAggregationState*> state, PipelineMemoryProvider&, const Record& record) const
{
    const auto base = static_cast<nautilus::val<int8_t*>>(state);
    for (const auto& field : nautilus::static_iterable(fields))
    {
        record.read(field.fieldIdentifier).writeToMemory(base + field.offset);
    }
    storeNull(state, false);
}

void LastSampleAggregationPhysicalFunction::combine(
    const nautilus::val<SampleAggregationState*> state1, const nautilus::val<SampleAggregationState*> state2, PipelineMemoryProvider&) const
{
    const auto base1 = static_cast<nautilus::val<int8_t*>>(state1);
    const auto base2 = static_cast<nautilus::val<int8_t*>>(state2);

    const auto isNull1 = readNull(state1);
    const auto isNull2 = readNull(state2);

    const auto ts1 = VarVal::readNonNullableVarValFromMemory(base1 + timestampFieldOffset, timestampFieldType);
    const auto ts2 = VarVal::readNonNullableVarValFromMemory(base2 + timestampFieldOffset, timestampFieldType);
    const auto state2WinsOnTie = (ts2 >= ts1).getRawValueAs<nautilus::val<bool>>();

    /// Prefer state2 if state1 has no value yet, otherwise only if both have values and state2's tie-break wins.
    const auto takeState2 = isNull1 or (not isNull2 and state2WinsOnTie);

    for (const auto& field : nautilus::static_iterable(fields))
    {
        const auto val1 = VarVal::readNonNullableVarValFromMemory(base1 + field.offset, field.type);
        const auto val2 = VarVal::readNonNullableVarValFromMemory(base2 + field.offset, field.type);
        VarVal::select(takeState2, val2, val1).writeToMemory(base1 + field.offset);
    }

    storeNull(state1, isNull1 and isNull2);
}

Record LastSampleAggregationPhysicalFunction::lower(const nautilus::val<SampleAggregationState*> state, PipelineMemoryProvider&) const
{
    const auto base = static_cast<nautilus::val<int8_t*>>(state);
    Record record;
    for (const auto& field : nautilus::static_iterable(fields))
    {
        record.write(field.fieldIdentifier, VarVal::readNonNullableVarValFromMemory(base + field.offset, field.type));
    }
    return record;
}

nautilus::val<bool> LastSampleAggregationPhysicalFunction::isEmpty(const nautilus::val<SampleAggregationState*> state) const
{
    return readNull(state);
}

void LastSampleAggregationPhysicalFunction::reset(const nautilus::val<SampleAggregationState*> state, PipelineMemoryProvider&) const
{
    /// Initialize the null flag to "no value seen yet" so the first lift() establishes the running LAST value.
    storeNull(state, true);
}

void LastSampleAggregationPhysicalFunction::cleanup(nautilus::val<SampleAggregationState*>) const
{
}

size_t LastSampleAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return stateSize;
}

SampleAggregationPhysicalFunctionRegistryReturnType
SampleAggregationPhysicalFunctionGeneratedRegistrar::RegisterLastSampleAggregationPhysicalFunction(
    SampleAggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<LastSampleAggregationPhysicalFunction>(arguments.stateSchema, arguments.timestampFieldName);
}

}
