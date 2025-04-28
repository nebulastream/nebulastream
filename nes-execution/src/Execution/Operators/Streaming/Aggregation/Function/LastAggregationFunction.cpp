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
#include <Execution/Operators/Streaming/Aggregation/Function/LastAggregationFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Util.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Common.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Aggregation
{

constexpr static std::string TimeStampField = "timestamp";
constexpr static std::string ValueField = "value";

LastAggregationFunction::LastAggregationFunction(
    std::shared_ptr<PhysicalType> inputType,
    std::shared_ptr<PhysicalType> resultType,
    std::unique_ptr<Functions::Function> inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , memoryProvider(std::make_unique<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(Memory::MemoryLayouts::RowLayout::create(
          Schema::create()
              ->addField(TimeStampField, DataTypeProvider::provideBasicType(BasicType::UINT64))
              ->addField(ValueField, this->inputType->type),
          8 + this->inputType->size())))
{
}

void LastAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, ExecutionContext& executionContext, const Nautilus::Record& record)
{
    auto timestampType = DefaultPhysicalTypeFactory().getPhysicalType(DataTypeProvider::provideBasicType(BasicType::UINT64));
    RecordBuffer stateBuffer{aggregationState};

    if (stateBuffer.getNumRecords() == 0)
    {
        nautilus::val<uint64_t> index(0);
        Record newStateRecord{
            {{TimeStampField, executionContext.currentTs.value},
             {ValueField, inputFunction->execute(record, executionContext.pipelineMemoryProvider.arena)}}};
        memoryProvider->writeRecord(index, stateBuffer, newStateRecord);
        stateBuffer.setNumRecords(1);
        return;
    }

    nautilus::val<uint64_t> index(0);
    auto state = memoryProvider->readRecord({TimeStampField, ValueField}, stateBuffer, index);
    auto currentTimestamp = state.read(TimeStampField);
    /// Updating the min value with the new value, if the new value is smaller
    if (executionContext.currentTs > currentTimestamp.cast<nautilus::val<uint64_t>>())
    {
        state.write(TimeStampField, currentTimestamp.cast<nautilus::val<uint64_t>>());
        state.write(ValueField, inputFunction->execute(record, executionContext.pipelineMemoryProvider.arena));
        memoryProvider->writeRecord(index, stateBuffer, state);
    }
}

void LastAggregationFunction::combine(
    const nautilus::val<AggregationState*> destination, const nautilus::val<AggregationState*> source, PipelineMemoryProvider&)
{
    RecordBuffer destStateBuffer{destination};
    RecordBuffer sourceStateBuffer{source};
    nautilus::val<uint64_t> index(0);
    auto sourceState = memoryProvider->readRecord({TimeStampField, ValueField}, sourceStateBuffer, index);

    if (destStateBuffer.getNumRecords() == 0)
    {
        Record newStateRecord{{{TimeStampField, sourceState.read(TimeStampField)}, {ValueField, sourceState.read(ValueField)}}};
        memoryProvider->writeRecord(index, destStateBuffer, newStateRecord);
        destStateBuffer.setNumRecords(1);
        return;
    }

    auto destState = memoryProvider->readRecord({TimeStampField, ValueField}, destStateBuffer, index);

    auto destTimestamp = destState.read(TimeStampField);
    auto sourceTimestamp = sourceState.read(TimeStampField);

    if (sourceTimestamp > destTimestamp)
    {
        destState.write(TimeStampField, sourceTimestamp);
        destState.write(ValueField, sourceState.read(ValueField));
        memoryProvider->writeRecord(index, destStateBuffer, destState);
    }
}

Nautilus::Record LastAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    RecordBuffer stateBuffer{aggregationState};

    invoke(
        +[](AggregationState* USED_IN_DEBUG(aggregationState))
        {
            INVARIANT(
                reinterpret_cast<Memory::TupleBuffer*>(aggregationState)->getNumberOfTuples() == 1,
                "Ey yo, looks like the aggregation does not produce a result. This requires null handling which we do not have at the "
                "moment. I am sorry!");
        },
        aggregationState);

    nautilus::val<uint64_t> index(0);
    auto state = memoryProvider->readRecord({TimeStampField, ValueField}, stateBuffer, index);
    Record record;
    record.write(resultFieldIdentifier, state.read(ValueField));

    invoke(
        +[](AggregationState* state) { std::destroy_at<Memory::TupleBuffer>(reinterpret_cast<Memory::TupleBuffer*>(state)); },
        aggregationState);
    return record;
}

void LastAggregationFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& memoryProvider)
{
    auto timestampType = DefaultPhysicalTypeFactory().getPhysicalType(DataTypeProvider::provideBasicType(BasicType::UINT64));
    nautilus::invoke(
        +[](Memory::AbstractBufferProvider* bufferProvider, AggregationState* aggregationState, size_t inputSize)
        { *reinterpret_cast<Memory::TupleBuffer*>(aggregationState) = bufferProvider->getUnpooledBuffer(8 + inputSize).value(); },
        memoryProvider.bufferProvider,
        aggregationState,
        nautilus::val<uint64_t>(inputType->size()));
}

size_t LastAggregationFunction::getSizeOfStateInBytes() const
{
    return sizeof(Memory::TupleBuffer);
}

}
