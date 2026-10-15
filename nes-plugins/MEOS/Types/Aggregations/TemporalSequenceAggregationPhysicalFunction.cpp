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

#include <TemporalSequenceAggregationPhysicalFunction.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include "Functions/ConstantValueLogicalFunction.hpp"

#include <DataTypes/StructData.hpp>
#include <DataTypes/VectorData.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::TemporalSequenceHelpers
{
int8_t* sortTsIndicesByValue(int8_t* tsBuffer, const uint64_t tsCount, Arena* arena)
{
    std::span<uint64_t> tsSpan(reinterpret_cast<uint64_t*>(tsBuffer), tsCount);

    /// Allocate memory for a new buffer, holding the sorted indices
    uint64_t* indexBuffer = reinterpret_cast<uint64_t*>(arena->allocateMemory(sizeof(uint64_t) * tsSpan.size()).data());
    const std::span<uint64_t> indexSpan(indexBuffer, tsSpan.size());
    std::iota(indexSpan.begin(), indexSpan.end(), 0);

    /// Sort the indexSpan based on the order of the corrsponding ts values
    std::ranges::sort(indexSpan, [&tsSpan](const uint64_t a, const uint64_t b) { return tsSpan[a] < tsSpan[b]; });

    /// Return pointer to sorted indexVector
    return reinterpret_cast<int8_t*>(indexBuffer);
}
}

namespace NES
{
TemporalSequenceAggregationPhysicalFunction::TemporalSequenceAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
{
}

void TemporalSequenceAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    /// Load the PagedVector, whose index is stored in the aggregation state and push the record into it
    /// We execute the input function in lower.
    /// Load the paged vector buffer from the parent via the stored child index
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    OwnedNautilusBuffer pagedVecBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer.asArg(),
        pagedVecBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(memArea));

    PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
    pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
}

void TemporalSequenceAggregationPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer parentBuffer1,
    nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    auto memArea1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    auto memArea2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);

    /// Write all records from the PagedBuffer of parentBuffer2 into the paged buffer of parentBuffer1
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider,
            TupleBuffer* parent1,
            const uint32_t* indexPtr1,
            TupleBuffer* parent2,
            const uint32_t* indexPtr2) -> void
        {
            const TupleBuffer vec1Buf = parent1->loadChildBuffer(ChildBufferIndex{*indexPtr1});
            const TupleBuffer vec2Buf = parent2->loadChildBuffer(ChildBufferIndex{*indexPtr2});
            auto vector1 = PagedVector::load(vec1Buf);
            const auto vector2 = PagedVector::load(vec2Buf);
            vector1.copyPagesFrom(*bufferProvider, vector2);
        },
        pipelineMemoryProvider.bufferProvider,
        parentBuffer1.asArg(),
        static_cast<nautilus::val<uint32_t*>>(memArea1),
        parentBuffer2.asArg(),
        static_cast<nautilus::val<uint32_t*>>(memArea2));
}

Record TemporalSequenceAggregationPhysicalFunction::lower(
    nautilus::val<AggregationState*> aggregationState, BorrowedNautilusBuffer parentBuffer, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Step 1: Load paged buffer and retrieve number of records stored.
    OwnedNautilusBuffer pagedVecBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer.asArg(),
        pagedVecBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(aggregationState));

    const auto numberOfEntries = invoke(
        +[](const TupleBuffer* pagedVectorBuffer)
        {
            const auto pagedVector = PagedVector::load(*pagedVectorBuffer);
            const auto numberOfEntriesVal = pagedVector.getTotalNumberOfRecords();
            INVARIANT(numberOfEntriesVal > 0, "The number of entries in the paged vector must be greater than 0");
            return numberOfEntriesVal;
        },
        pagedVecBuffer.asArg());

    /// Step 2: Allocate a buffer that is large enough to hold numberOfEntries timestamps.
    const nautilus::val<int8_t*> timestampBuffer = pipelineMemoryProvider.arena.allocateMemory(sizeof(uint64_t) * numberOfEntries);

    /// Step 3: Iterate over every record in the paged buffer, retrieve the TemporalPoint and store the timestamp of every point in the timestamp buffer.
    const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
    const auto pagedVectorEnd = pagedVectorRef.end();
    nautilus::val<uint64_t> recordIndex{0};
    for (auto refIter = pagedVectorRef.begin(); refIter != pagedVectorEnd; ++refIter)
    {
        const auto record = *refIter;
        const VarVal tPointVal = inputFunction.execute(record, pipelineMemoryProvider.arena);
        const StructData tPointData = tPointVal.getRawValueAs<StructData>();
        const VarVal tsVal = tPointData.at("ts");
        tsVal.writeToMemory(timestampBuffer + recordIndex * sizeof(uint64_t));
        ++recordIndex;
    }

    /// Step 4: Invoke helper function which sorts the timestamps and returns memory pointing to the position of every value in the sorted buffer.
    nautilus::val<int8_t*> sortedIndices = nautilus::invoke(
        TemporalSequenceHelpers::sortTsIndicesByValue, timestampBuffer, numberOfEntries, pipelineMemoryProvider.arena.getArena());

    /// Step 5: Allocate memory to hold the whole temporal sequence and write the temporal points in correct order to memory
    const nautilus::val<int8_t*> sequenceBuffer
        = pipelineMemoryProvider.arena.allocateMemory(numberOfEntries * inputType.getSizeInBytesWithoutNull());

    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; ++i)
    {
        /// PagedVector index of the ith temporal point ordered by ts
        const nautilus::val<uint64_t> vecIndex = readValueFromMemRef<uint64_t>(sortedIndices + i * sizeof(uint64_t));
        const auto record = pagedVectorRef.at(vecIndex);
        const VarVal tPointVal = inputFunction.execute(record, pipelineMemoryProvider.arena);
        /// Write tPointVal into the sequence buffer
        tPointVal.writeToMemory(sequenceBuffer + i * inputType.getSizeInBytesWithoutNull());
    }

    /// Step 6: Create VectorData out of the sequence data and write its representation (ptr + size) into the buffer of the StructData we return.
    const VectorData sequenceData{sequenceBuffer, inputType, numberOfEntries * inputType.getSizeInBytesWithoutNull()};
    const nautilus::val<int8_t*> structBuffer = pipelineMemoryProvider.arena.allocateMemory(resultType.getSizeInBytesWithoutNull());
    VarVal{sequenceData, false, false}.writeToMemory(structBuffer);
    const StructData temporalPointSequence{structBuffer, resultType.fields};

    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, VarVal{temporalPointSequence, false, false});
    return resultRecord;
}

void TemporalSequenceAggregationPhysicalFunction::reset(
    nautilus::val<AggregationState*> aggregationState, BorrowedNautilusBuffer parentBuffer, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = getSizeInBytes(tupleLayout->getSchema());
    const nautilus::val<uint32_t> childBufferIndexVal = nautilus::invoke(
        +[](TupleBuffer* parentBuffer, AbstractBufferProvider* bufferProvider, uint64_t tupleSize)
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): aggregation state stores a TupleBuffer at this slot.
            if (auto pagedVectorBufferOpt = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                /// initialize paged vector buffer
                auto pagedVectorBuffer = pagedVectorBufferOpt.value();
                PagedVector::init(pagedVectorBuffer, bufferProvider->getBufferSize(), tupleSize);
                auto childBufferIndex = parentBuffer->storeChildBuffer(pagedVectorBuffer);
                return childBufferIndex.getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for median aggregation paged vector!");
        },
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider,
        tupleSize);
    auto indexMemArea = static_cast<nautilus::val<uint32_t*>>(aggregationState);
    *indexMemArea = childBufferIndexVal;
}

void TemporalSequenceAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
    /// No-op: the paged vector buffer is stored as a child of the parent hash map TupleBuffer and
    /// is released automatically when the parent is released.
}

size_t TemporalSequenceAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Size of a child buffer index
    return sizeof(uint32_t);
}

AggregationPhysicalFunctionRegistryReturnType
TemporalSequenceAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "Tuple layout paged vector not set");
    return std::make_shared<TemporalSequenceAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.tupleLayout.value());
}
}
