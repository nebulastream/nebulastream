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

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/LastAggregationPhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Util.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Common.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>
#include "DataTypes/DataTypeProvider.hpp"
#include "MemoryLayout/RowLayout.hpp"

#include "AggregationPhysicalFunctionRegistry.hpp"

namespace NES
{

constexpr static std::string TimeStampField = "timestamp";
constexpr static std::string ValueField = "value";

LastAggregationPhysicalFunction::LastAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , bufferRef(Interface::BufferRef::TupleBufferRef::create(
          8 + this->inputType.getSizeInBytes(), Schema{}.addField(TimeStampField, DataType::Type::UINT64).addField(ValueField, inputType)))
{
}

void LastAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& memoryProvider,
    const Nautilus::Record& record,
    const nautilus::val<Timestamp>& timestamp)
{
    RecordBuffer stateBuffer{aggregationState};

    if (stateBuffer.getNumRecords() == 0)
    {
        nautilus::val<uint64_t> index(0);
        Record newStateRecord{{{TimeStampField, timestamp.value}, {ValueField, inputFunction.execute(record, memoryProvider.arena)}}};
        bufferRef->writeRecord(index, stateBuffer, newStateRecord, memoryProvider.bufferProvider);
        stateBuffer.setNumRecords(1);
        return;
    }

    nautilus::val<uint64_t> index(0);
    auto state = bufferRef->readRecord({TimeStampField, ValueField}, stateBuffer, index);
    auto currentTimestamp = state.read(TimeStampField);
    /// Updating the min value with the new value, if the new value is smaller
    if (timestamp.value > currentTimestamp.cast<nautilus::val<uint64_t>>())
    {
        state.write(TimeStampField, currentTimestamp.cast<nautilus::val<uint64_t>>());
        state.write(ValueField, inputFunction.execute(record, memoryProvider.arena));
        bufferRef->writeRecord(index, stateBuffer, state, memoryProvider.bufferProvider);
    }
}

void LastAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> destination,
    const nautilus::val<AggregationState*> source,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    RecordBuffer destStateBuffer{destination};
    RecordBuffer sourceStateBuffer{source};
    nautilus::val<uint64_t> index(0);
    auto sourceState = bufferRef->readRecord({TimeStampField, ValueField}, sourceStateBuffer, index);

    if (destStateBuffer.getNumRecords() == 0)
    {
        Record newStateRecord{{{TimeStampField, sourceState.read(TimeStampField)}, {ValueField, sourceState.read(ValueField)}}};
        bufferRef->writeRecord(index, destStateBuffer, newStateRecord, pipelineMemoryProvider.bufferProvider);
        destStateBuffer.setNumRecords(1);
        return;
    }

    auto destState = bufferRef->readRecord({TimeStampField, ValueField}, destStateBuffer, index);

    auto destTimestamp = destState.read(TimeStampField);
    auto sourceTimestamp = sourceState.read(TimeStampField);

    if (sourceTimestamp > destTimestamp)
    {
        destState.write(TimeStampField, sourceTimestamp);
        destState.write(ValueField, sourceState.read(ValueField));
        bufferRef->writeRecord(index, destStateBuffer, destState, pipelineMemoryProvider.bufferProvider);
    }
}

Nautilus::Record LastAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    RecordBuffer stateBuffer{aggregationState};
    nautilus::val<uint64_t> index(0);
    auto state = bufferRef->readRecord({TimeStampField, ValueField}, stateBuffer, index);
    Record record;
    record.write(resultFieldIdentifier, state.read(ValueField));
    return record;
}

void LastAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& memoryProvider)
{
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider, AggregationState* aggregationState, size_t inputSize)
        { *reinterpret_cast<TupleBuffer*>(aggregationState) = bufferProvider->getUnpooledBuffer(8 + inputSize).value(); },
        memoryProvider.bufferProvider,
        aggregationState,
        nautilus::val<uint64_t>(inputType.getSizeInBytes()));
}

size_t LastAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(TupleBuffer);
}

void LastAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    invoke(+[](AggregationState* state) { std::destroy_at<TupleBuffer>(reinterpret_cast<TupleBuffer*>(state)); }, aggregationState);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterLastAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<LastAggregationPhysicalFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

}
