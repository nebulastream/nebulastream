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
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include "Aggregation/Function/AggregationPhysicalFunction.hpp"
#include "DataTypes/DataType.hpp"
#include "Functions/PhysicalFunction.hpp"
#include "Interface/Record.hpp"
#include "Interface/TimestampRef.hpp"
#include "Time/Timestamp.hpp"
#include "Runtime/AbstractBufferProvider.hpp"

namespace NES
{
namespace
{
constexpr uint64_t timestampOffset = 0;
constexpr uint64_t seenOffset = sizeof(uint64_t);
constexpr uint64_t bufferIndexOffset = 2 * sizeof(uint64_t);

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
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record,
    const nautilus::val<Timestamp>& timestamp,
    const AggregationInputBuffer&)
{
    const auto memory = stateMemory(aggregationState);
    const auto seen = readValueFromMemRef<bool>(memory + nautilus::val<uint64_t>{seenOffset});
    const auto latestTimestamp = readValueFromMemRef<uint64_t>(memory + nautilus::val<uint64_t>{timestampOffset});
    if (not seen or timestamp.convertToValue() > latestTimestamp)
    {
        OwnedNautilusBuffer pagedVectorBuffer;
        nautilus::invoke(
            +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
            { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
            parentBuffer,
            pagedVectorBuffer.asArg(),
            static_cast<nautilus::val<uint32_t*>>(memory + nautilus::val<uint64_t>{bufferIndexOffset}));
        PagedVectorRef{BorrowedNautilusBuffer::from(pagedVectorBuffer.asArg()), tupleLayout}
            .pushBack(record, pipelineMemoryProvider.bufferProvider);
        VarVal{timestamp.convertToValue()}.writeToMemory(memory + nautilus::val<uint64_t>{timestampOffset});
        VarVal{nautilus::val<bool>{true}}.writeToMemory(memory + nautilus::val<uint64_t>{seenOffset});
    }
}

void LastAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> destination,
    nautilus::val<TupleBuffer*> destinationParentBuffer,
    const nautilus::val<AggregationState*> source,
    nautilus::val<TupleBuffer*> sourceParentBuffer,
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
        OwnedNautilusBuffer sourceBuffer;
        OwnedNautilusBuffer destinationBuffer;
        nautilus::invoke(
            +[](TupleBuffer* sourceParent,
                TupleBuffer* sourceOut,
                const uint32_t* sourceIndex,
                TupleBuffer* destinationParent,
                TupleBuffer* destinationOut,
                const uint32_t* destinationIndex)
            {
                *sourceOut = sourceParent->loadChildBuffer(ChildBufferIndex{*sourceIndex});
                *destinationOut = destinationParent->loadChildBuffer(ChildBufferIndex{*destinationIndex});
            },
            sourceParentBuffer,
            sourceBuffer.asArg(),
            static_cast<nautilus::val<uint32_t*>>(sourceMemory + nautilus::val<uint64_t>{bufferIndexOffset}),
            destinationParentBuffer,
            destinationBuffer.asArg(),
            static_cast<nautilus::val<uint32_t*>>(destinationMemory + nautilus::val<uint64_t>{bufferIndexOffset}));
        auto sourceValues = PagedVectorRef{BorrowedNautilusBuffer::from(sourceBuffer.asArg()), tupleLayout};
        PagedVectorRef{BorrowedNautilusBuffer::from(destinationBuffer.asArg()), tupleLayout}
            .pushBack(sourceValues.at(sourceValues.getNumberOfRecords() - nautilus::val<uint64_t>{1}), pipelineMemoryProvider.bufferProvider);
        VarVal{sourceTimestamp}.writeToMemory(destinationMemory + nautilus::val<uint64_t>{timestampOffset});
        VarVal{nautilus::val<bool>{true}}.writeToMemory(destinationMemory + nautilus::val<uint64_t>{seenOffset});
    }
}

Record LastAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto memory = stateMemory(aggregationState);
    OwnedNautilusBuffer pagedVectorBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer,
        pagedVectorBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(memory + nautilus::val<uint64_t>{bufferIndexOffset}));
    auto values = PagedVectorRef{BorrowedNautilusBuffer::from(pagedVectorBuffer.asArg()), tupleLayout};
    auto record = values.at(values.getNumberOfRecords() - nautilus::val<uint64_t>{1});
    Record result;
    result.write(resultFieldIdentifier, inputFunction.execute(record, pipelineMemoryProvider.arena));
    return result;
}

void LastAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto memory = stateMemory(aggregationState);
    VarVal{nautilus::val<bool>{false}}.writeToMemory(memory + nautilus::val<uint64_t>{seenOffset});
    VarVal{nautilus::val<uint64_t>{0}}.writeToMemory(memory + nautilus::val<uint64_t>{timestampOffset});
    const nautilus::val<uint64_t> tupleSize{tupleLayout->getSchema().getSizeInBytes()};
    const nautilus::val<uint32_t> childBufferIndex = nautilus::invoke(
        +[](TupleBuffer* parentBuffer, AbstractBufferProvider* bufferProvider, uint64_t tupleSize)
        {
            if (auto buffer = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                PagedVector::init(buffer.value(), bufferProvider->getBufferSize(), tupleSize);
                return parentBuffer->storeChildBuffer(buffer.value()).getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for LAST aggregation paged vector!");
        },
        parentBuffer,
        pipelineMemoryProvider.bufferProvider,
        tupleSize);
    *static_cast<nautilus::val<uint32_t*>>(memory + nautilus::val<uint64_t>{bufferIndexOffset}) = childBufferIndex;
}

void LastAggregationPhysicalFunction::cleanup(const nautilus::val<AggregationState*>)
{
    /// No-op: the paged vector is a child of the parent hash-map buffer.
}

size_t LastAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return bufferIndexOffset + sizeof(uint32_t);
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
