/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
*/

#include <Aggregation/Function/LastAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
constexpr uint64_t timestampOffset = 0;
constexpr uint64_t seenOffset = sizeof(uint64_t);
constexpr uint64_t bufferOffset = 2 * sizeof(uint64_t);

nautilus::val<int8_t*> stateMemory(const nautilus::val<AggregationState*>& state)
{
    return static_cast<nautilus::val<int8_t*>>(state);
}
}

LastAggregationPhysicalFunction::LastAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
{
}

void LastAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record,
    const nautilus::val<Timestamp>& timestamp)
{
    const auto memory = stateMemory(aggregationState);
    const auto seen = readValueFromMemRef<bool>(memory + nautilus::val<uint64_t>{seenOffset});
    const auto latestTimestamp = readValueFromMemRef<uint64_t>(memory + nautilus::val<uint64_t>{timestampOffset});
    if (not seen or timestamp.convertToValue() > latestTimestamp)
    {
        PagedVectorRef{BorrowedNautilusBuffer::from(memory + nautilus::val<uint64_t>{bufferOffset}), tupleLayout}
            .pushBack(record, pipelineMemoryProvider.bufferProvider);
        VarVal{timestamp.convertToValue()}.writeToMemory(memory + nautilus::val<uint64_t>{timestampOffset});
        VarVal{nautilus::val<bool>{true}}.writeToMemory(memory + nautilus::val<uint64_t>{seenOffset});
    }
}

void LastAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> destination,
    const nautilus::val<AggregationState*> source,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto destinationMemory = stateMemory(destination);
    const auto sourceMemory = stateMemory(source);
    const auto destinationSeen = readValueFromMemRef<bool>(destinationMemory + nautilus::val<uint64_t>{seenOffset});
    const auto sourceSeen = readValueFromMemRef<bool>(sourceMemory + nautilus::val<uint64_t>{seenOffset});
    const auto destinationTimestamp = readValueFromMemRef<uint64_t>(destinationMemory + nautilus::val<uint64_t>{timestampOffset});
    const auto sourceTimestamp = readValueFromMemRef<uint64_t>(sourceMemory + nautilus::val<uint64_t>{timestampOffset});

    if (sourceSeen and (not destinationSeen or sourceTimestamp > destinationTimestamp))
    {
        auto sourceValues = PagedVectorRef{BorrowedNautilusBuffer::from(sourceMemory + nautilus::val<uint64_t>{bufferOffset}), tupleLayout};
        PagedVectorRef{BorrowedNautilusBuffer::from(destinationMemory + nautilus::val<uint64_t>{bufferOffset}), tupleLayout}
            .pushBack(sourceValues.at(sourceValues.getNumberOfRecords() - nautilus::val<uint64_t>{1}), pipelineMemoryProvider.bufferProvider);
        VarVal{sourceTimestamp}.writeToMemory(destinationMemory + nautilus::val<uint64_t>{timestampOffset});
        VarVal{nautilus::val<bool>{true}}.writeToMemory(destinationMemory + nautilus::val<uint64_t>{seenOffset});
    }
}

Record LastAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto memory = stateMemory(aggregationState);
    auto values = PagedVectorRef{BorrowedNautilusBuffer::from(memory + nautilus::val<uint64_t>{bufferOffset}), tupleLayout};
    auto record = values.at(values.getNumberOfRecords() - nautilus::val<uint64_t>{1});
    Record result;
    result.write(resultFieldIdentifier, inputFunction.execute(record, pipelineMemoryProvider.arena));
    return result;
}

void LastAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto memory = stateMemory(aggregationState);
    VarVal{nautilus::val<bool>{false}}.writeToMemory(memory + nautilus::val<uint64_t>{seenOffset});
    VarVal{nautilus::val<uint64_t>{0}}.writeToMemory(memory + nautilus::val<uint64_t>{timestampOffset});
    const nautilus::val<uint64_t> tupleSize{tupleLayout->getSchema().getSizeInBytes()};
    nautilus::invoke(
        +[](int8_t* pagedVectorMemArea, AbstractBufferProvider* bufferProvider, uint64_t tupleSize)
        {
            auto* pagedVectorBuffer = reinterpret_cast<TupleBuffer*>(pagedVectorMemArea);
            if (auto buffer = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                PagedVector::init(buffer.value(), bufferProvider->getBufferSize(), tupleSize);
                new (pagedVectorBuffer) TupleBuffer(buffer.value());
                return;
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for LAST aggregation paged vector!");
        },
        memory + nautilus::val<uint64_t>{bufferOffset},
        pipelineMemoryProvider.bufferProvider,
        tupleSize);
}

void LastAggregationPhysicalFunction::cleanup(const nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](int8_t* pagedVectorMemArea) { reinterpret_cast<TupleBuffer*>(pagedVectorMemArea)->~TupleBuffer(); },
        stateMemory(aggregationState) + nautilus::val<uint64_t>{bufferOffset});
}

size_t LastAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return bufferOffset + sizeof(TupleBuffer);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterLastAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "Tuple layout paged vector not set");
    return std::make_shared<LastAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.tupleLayout.value());
}
}
