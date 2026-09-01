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

#include <Statistics/ReservoirSamplePhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <ranges>
#include <thread>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Statistics/ReservoirMerge.hpp>
#include <Statistics/ReservoirSampleBlob.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{

/// Offsets into the aggregation state: [childBufferIndex: uint32][numberOfSeenTuples: uint64]
constexpr uint64_t NUMBER_OF_SEEN_TUPLES_OFFSET = sizeof(uint32_t);

/// Draws the position an arriving record would take among the numberOfSeenTuples + 1 records seen so far
/// (Algorithm R: the record replaces reservoir slot r iff r < sampleSize).
/// Each worker thread gets its own RNG, seeded by combining the configured seed with the thread id, so
/// concurrent builds do not contend; eviction choices are therefore not reproducible across runs.
uint64_t getRandomSlotProxy(const uint64_t numberOfSeenTuples, const uint64_t seed)
{
    thread_local std::mt19937_64 gen(seed ^ std::hash<std::thread::id>{}(std::this_thread::get_id()));
    std::uniform_int_distribution<uint64_t> dis(0, numberOfSeenTuples);
    return dis(gen);
}

/// Loads the paged vector child buffer referenced by the uint32 index at the beginning of the aggregation state.
OwnedNautilusBuffer
loadPagedVectorBuffer(const nautilus::val<AggregationState*>& aggregationState, const nautilus::val<TupleBuffer*>& parentBuffer)
{
    OwnedNautilusBuffer pagedVectorBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer,
        pagedVectorBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState)));
    return pagedVectorBuffer;
}

/// Allocates a fresh, empty paged vector as a child buffer of the parent and returns its child buffer index.
nautilus::val<uint32_t> allocateFreshPagedVector(
    const nautilus::val<TupleBuffer*>& parentBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<uint64_t>& tupleSize)
{
    return nautilus::invoke(
        +[](TupleBuffer* parentBuffer, AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
        {
            if (auto pagedVectorBufferOpt = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                auto pagedVectorBuffer = pagedVectorBufferOpt.value();
                PagedVector::init(pagedVectorBuffer, bufferProvider->getBufferSize(), tupleSize);
                return parentBuffer->storeChildBuffer(pagedVectorBuffer).getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for the reservoir sample paged vector!");
        },
        parentBuffer,
        bufferProvider,
        tupleSize);
}

uint64_t computeReservoirMergeSelectionProxy(
    const uint64_t seenLeft,
    const uint64_t storedLeft,
    const uint64_t seenRight,
    const uint64_t storedRight,
    const uint64_t capacity,
    const uint64_t seed,
    int8_t* positionsOut)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): arena memory used as a uint64_t position array
    const auto selection = computeReservoirMergeSelection(
        seenLeft, storedLeft, seenRight, storedRight, capacity, seed, reinterpret_cast<uint64_t*>(positionsOut));
    return selection.fromFirst;
}

}

ReservoirSamplePhysicalFunction::ReservoirSamplePhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldIdentifier,
    const uint64_t sampleSize,
    const uint64_t seed)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
    , numberOfSeenTuplesFieldIdentifier(std::move(numberOfSeenTuplesFieldIdentifier))
    , sampleSize(sampleSize)
    , seed(seed)
    , fixedFieldsSizePerTuple(0)
{
    PRECONDITION(this->sampleSize > 0, "The reservoir sample size must be greater than zero");
    for (const auto& field : this->tupleLayout->getSchema())
    {
        PRECONDITION(not field.getDataType().nullable, "Reservoir samples do not support nullable input fields, but got {}", field);
        blobFields.emplace_back(field.getFullyQualifiedName(), field.getDataType());
        if (field.getDataType().type != DataType::Type::VARSIZED)
        {
            fixedFieldsSizePerTuple += field.getDataType().getSizeInBytesWithoutNull();
        }
    }
}

void ReservoirSamplePhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    const auto statePtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto numberOfSeenTuplesRef = statePtr + nautilus::val<uint64_t>{NUMBER_OF_SEEN_TUPLES_OFFSET};
    const auto numberOfSeenTuples = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);

    auto pagedVectorBuffer = loadPagedVectorBuffer(aggregationState, parentBuffer);
    PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVectorBuffer.asArg()), tupleLayout);

    if (numberOfSeenTuples < nautilus::val<uint64_t>{sampleSize})
    {
        pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
    }
    else
    {
        /// Algorithm R: replace a reservoir slot with gradually decreasing probability sampleSize / (seen + 1)
        const auto randomSlot = invoke(getRandomSlotProxy, numberOfSeenTuples, nautilus::val<uint64_t>{seed});
        if (randomSlot < nautilus::val<uint64_t>{sampleSize})
        {
            pagedVectorRef.replaceRecord(record, randomSlot, pipelineMemoryProvider.bufferProvider);
        }
    }

    VarVal{numberOfSeenTuples + nautilus::val<uint64_t>{1}}.writeToMemory(numberOfSeenTuplesRef);
}

void ReservoirSamplePhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<TupleBuffer*> parentBuffer1,
    nautilus::val<AggregationState*> aggregationState2,
    nautilus::val<TupleBuffer*> parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto statePtr1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto statePtr2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const auto numberOfSeenTuplesRef1 = statePtr1 + nautilus::val<uint64_t>{NUMBER_OF_SEEN_TUPLES_OFFSET};
    const auto numberOfSeenTuplesRef2 = statePtr2 + nautilus::val<uint64_t>{NUMBER_OF_SEEN_TUPLES_OFFSET};
    const auto seen1 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef1);
    const auto seen2 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef2);

    auto pagedVectorBuffer1 = loadPagedVectorBuffer(aggregationState1, parentBuffer1);
    auto pagedVectorBuffer2 = loadPagedVectorBuffer(aggregationState2, parentBuffer2);
    const PagedVectorRef pagedVectorRef1(BorrowedNautilusBuffer::from(pagedVectorBuffer1.asArg()), tupleLayout);
    const PagedVectorRef pagedVectorRef2(BorrowedNautilusBuffer::from(pagedVectorBuffer2.asArg()), tupleLayout);
    const auto stored1 = pagedVectorRef1.getNumberOfRecords();
    const auto stored2 = pagedVectorRef2.getNumberOfRecords();

    if (stored1 + stored2 <= nautilus::val<uint64_t>{sampleSize})
    {
        /// Neither reservoir ever evicted (stored == seen), so concatenating is an exact sample of the union.
        nautilus::invoke(
            +[](AbstractBufferProvider* bufferProvider, TupleBuffer* pagedVectorBuffer1, const TupleBuffer* pagedVectorBuffer2) -> void
            {
                auto vector1 = PagedVector::load(*pagedVectorBuffer1);
                const auto vector2 = PagedVector::load(*pagedVectorBuffer2);
                vector1.copyPagesFrom(*bufferProvider, vector2);
            },
            pipelineMemoryProvider.bufferProvider,
            pagedVectorBuffer1.asArg(),
            pagedVectorBuffer2.asArg());
    }
    else
    {
        /// Statistically exact merge: select which tuples survive host-side (see ReservoirMerge.hpp), then
        /// materialize the survivors into a fresh paged vector that replaces the left reservoir.
        const auto positionsMemory = pipelineMemoryProvider.arena.allocateMemory(nautilus::val<uint64_t>{sampleSize * sizeof(uint64_t)});
        const auto fromFirst = invoke(
            computeReservoirMergeSelectionProxy,
            seen1,
            stored1,
            seen2,
            stored2,
            nautilus::val<uint64_t>{sampleSize},
            nautilus::val<uint64_t>{seed},
            positionsMemory);

        const nautilus::val<uint64_t> tupleSize = getSizeInBytes(tupleLayout->getSchema());
        const auto freshIndex = allocateFreshPagedVector(parentBuffer1, pipelineMemoryProvider.bufferProvider, tupleSize);

        OwnedNautilusBuffer freshPagedVectorBuffer;
        nautilus::invoke(
            +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t index) { *out = parent->loadChildBuffer(ChildBufferIndex{index}); },
            parentBuffer1,
            freshPagedVectorBuffer.asArg(),
            freshIndex);
        PagedVectorRef freshPagedVectorRef(BorrowedNautilusBuffer::from(freshPagedVectorBuffer.asArg()), tupleLayout);

        for (nautilus::val<uint64_t> i = 0; i < nautilus::val<uint64_t>{sampleSize}; i = i + 1)
        {
            const auto position = readValueFromMemRef<uint64_t>(positionsMemory + (i * nautilus::val<uint64_t>{sizeof(uint64_t)}));
            if (i < fromFirst)
            {
                const auto record = pagedVectorRef1.at(position);
                freshPagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
            }
            else
            {
                const auto record = pagedVectorRef2.at(position);
                freshPagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
            }
        }

        /// Point the left state at the fresh reservoir. The previous child buffer stays with the parent buffer
        /// until the parent is released.
        VarVal{freshIndex}.writeToMemory(statePtr1);
    }

    VarVal{seen1 + seen2}.writeToMemory(numberOfSeenTuplesRef1);
}

Record ReservoirSamplePhysicalFunction::lower(
    nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto statePtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto numberOfSeenTuples = readValueFromMemRef<uint64_t>(statePtr + nautilus::val<uint64_t>{NUMBER_OF_SEEN_TUPLES_OFFSET});

    auto pagedVectorBuffer = loadPagedVectorBuffer(aggregationState, parentBuffer);
    const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVectorBuffer.asArg()), tupleLayout);
    const auto tupleCount = pagedVectorRef.getNumberOfRecords();

    /// First pass: compute the serialized size of the sample tuples
    nautilus::val<uint64_t> dataSize = tupleCount * nautilus::val<uint64_t>{fixedFieldsSizePerTuple};
    for (const auto& record : pagedVectorRef)
    {
        for (nautilus::static_val<uint64_t> i = 0; i < blobFields.size(); ++i)
        {
            if (blobFields[i].type.type == DataType::Type::VARSIZED)
            {
                const auto varSizedValue = record.read(blobFields[i].name).getRawValueAs<VariableSizedData>();
                dataSize = dataSize + varSizedValue.getSize() + nautilus::val<uint64_t>{ReservoirSampleBlob::VARSIZED_LENGTH_PREFIX_SIZE};
            }
        }
    }

    /// Second pass: serialize the tuples into the blob
    const auto blobSize = nautilus::val<uint64_t>{ReservoirSampleBlob::HEADER_SIZE} + dataSize;
    const auto blobMemory = pipelineMemoryProvider.arena.allocateMemory(blobSize);
    writeReservoirSampleBlobHeader(blobMemory, tupleCount, dataSize);
    auto cursor = getReservoirSampleBlobDataArea(blobMemory);
    for (const auto& record : pagedVectorRef)
    {
        for (nautilus::static_val<uint64_t> i = 0; i < blobFields.size(); ++i)
        {
            const auto& [name, type] = blobFields[i];
            const auto& value = record.read(name);
            if (type.type == DataType::Type::VARSIZED)
            {
                const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
                const auto contentSize = varSizedValue.getSize();
                VarVal{static_cast<nautilus::val<uint32_t>>(contentSize)}.writeToMemory(cursor);
                cursor = cursor + nautilus::val<uint64_t>{ReservoirSampleBlob::VARSIZED_LENGTH_PREFIX_SIZE};
                nautilus::memcpy(cursor, varSizedValue.getContent(), contentSize);
                cursor = cursor + contentSize;
            }
            else if (const auto storeFunction = storeValueFunctionMap.find(type.type); storeFunction != storeValueFunctionMap.end())
            {
                auto dummy = storeFunction->second(value, cursor);
                cursor = cursor + nautilus::val<uint64_t>{type.getSizeInBytesWithoutNull()};
            }
            else
            {
                throw UnknownDataType("Physical Type: {} is currently not supported", type);
            }
        }
    }

    Record resultRecord;
    resultRecord.write(numberOfSeenTuplesFieldIdentifier, VarVal{numberOfSeenTuples});
    resultRecord.write(resultFieldIdentifier, VarVal{VariableSizedData{blobMemory, blobSize}});
    return resultRecord;
}

void ReservoirSamplePhysicalFunction::reset(
    nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = getSizeInBytes(tupleLayout->getSchema());
    const auto childBufferIndex = allocateFreshPagedVector(parentBuffer, pipelineMemoryProvider.bufferProvider, tupleSize);

    const auto statePtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    VarVal{childBufferIndex}.writeToMemory(statePtr);
    VarVal{nautilus::val<uint64_t>{0}}.writeToMemory(statePtr + nautilus::val<uint64_t>{NUMBER_OF_SEEN_TUPLES_OFFSET});
}

void ReservoirSamplePhysicalFunction::cleanup(nautilus::val<AggregationState*> /*aggregationState*/)
{
    /// No-op: the paged vector buffer is stored as a child of the parent hash map TupleBuffer and
    /// is released automatically when the parent is released.
}

size_t ReservoirSamplePhysicalFunction::getSizeOfStateInBytes() const
{
    /// uint32_t child buffer index + uint64_t number of seen tuples
    return sizeof(uint32_t) + sizeof(uint64_t);
}

}
