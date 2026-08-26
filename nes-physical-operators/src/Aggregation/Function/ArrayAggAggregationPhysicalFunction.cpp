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

#include <Aggregation/Function/ArrayAggAggregationPhysicalFunction.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <nautilus/function.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include "Interface/TimestampRef.hpp"
#include "Time/Timestamp.hpp"

namespace NES
{
namespace
{
std::atomic_uint64_t nextArrayAggComparatorIdentifier{0};

Record::RecordFieldIdentifier
getArrayAggFieldIdentifier(const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout, const size_t fieldIndex)
{
    INVARIANT(tupleLayout != nullptr, "ARRAY_AGG PagedVector tuple layout must not be null");
    INVARIANT(tupleLayout->getSchema().size() == 2, "ARRAY_AGG PagedVector layout must contain a timestamp and input field");
    return tupleLayout->getSchema()[fieldIndex]->getFullyQualifiedName();
}
}

ArrayAggAggregationPhysicalFunction::ArrayAggAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    const bool sortByTimestamp)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
    , timestampFieldIdentifier(getArrayAggFieldIdentifier(this->tupleLayout, 0))
    , inputFieldIdentifier(getArrayAggFieldIdentifier(this->tupleLayout, 1))
    , sortByTimestamp(sortByTimestamp)
    , comparatorIdentifier(fmt::format("array_agg_{}", nextArrayAggComparatorIdentifier.fetch_add(1, std::memory_order_relaxed)))
{
    PRECONDITION(this->tupleLayout != nullptr, "ARRAY_AGG PagedVector tuple layout must not be null");
    PRECONDITION(this->inputType.type != DataType::Type::VARSIZED, "ARRAY_AGG requires fixed-size input");
}

void ArrayAggAggregationPhysicalFunction::setup(CompilationContext& compilationContext)
{
    if (not sortByTimestamp)
    {
        return;
    }
    comparatorOwners.emplace_back(PagedVectorRef::registerComparator(
        compilationContext,
        comparatorIdentifier,
        tupleLayout,
        [timestampFieldIdentifier = timestampFieldIdentifier](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
        { return (lhs.read(timestampFieldIdentifier) < rhs.read(timestampFieldIdentifier)).getRawValueAs<nautilus::val<bool>>(); }));
}

void ArrayAggAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record,
    const nautilus::val<Timestamp>& timestamp,
    const AggregationInputBuffer&)
{
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    OwnedNautilusBuffer pagedVectorBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer,
        pagedVectorBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(memArea));
    PagedVectorRef pagedVector(BorrowedNautilusBuffer::from(pagedVectorBuffer.asArg()), tupleLayout);
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (not inputType.nullable or not value.isNull())
    {
        Record arrayAggRecord;
        arrayAggRecord.write(timestampFieldIdentifier, timestamp.convertToValue());
        arrayAggRecord.write(inputFieldIdentifier, value);
        pagedVector.pushBack(arrayAggRecord, pipelineMemoryProvider.bufferProvider);
    }
}

void ArrayAggAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<TupleBuffer*> parentBuffer1,
    const nautilus::val<AggregationState*> aggregationState2,
    nautilus::val<TupleBuffer*> parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider,
            TupleBuffer* parent1,
            const uint32_t* indexPtr1,
            TupleBuffer* parent2,
            const uint32_t* indexPtr2)
        {
            const auto vector1Buffer = parent1->loadChildBuffer(ChildBufferIndex{*indexPtr1});
            const auto vector2Buffer = parent2->loadChildBuffer(ChildBufferIndex{*indexPtr2});
            auto vector1 = PagedVector::load(vector1Buffer);
            const auto vector2 = PagedVector::load(vector2Buffer);
            vector1.copyPagesFrom(*bufferProvider, vector2);
        },
        pipelineMemoryProvider.bufferProvider,
        parentBuffer1,
        static_cast<nautilus::val<uint32_t*>>(aggregationState1),
        parentBuffer2,
        static_cast<nautilus::val<uint32_t*>>(aggregationState2));
}

Record ArrayAggAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    OwnedNautilusBuffer pagedVectorBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer,
        pagedVectorBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(aggregationState));
    PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVectorBuffer.asArg()), tupleLayout);
    if (sortByTimestamp)
    {
        INVARIANT(not comparatorOwners.empty(), "ARRAY_AGG comparator must be registered before lowering");
        pagedVectorRef.sort(comparatorOwners.back(), pipelineMemoryProvider.arena);
    }

    const auto numberOfRecords = pagedVectorRef.getNumberOfRecords();
    const auto fieldSize = nautilus::val<uint64_t>{DataTypeProvider::provideDataType(inputType.type).getSizeInBytesWithNull()};
    const auto payloadSize = numberOfRecords * fieldSize;
    auto payload = pipelineMemoryProvider.arena.allocateVariableSizedData(payloadSize);
    nautilus::val<uint64_t> index = 0;
    for (auto iterator = pagedVectorRef.begin(); iterator != pagedVectorRef.end(); ++iterator)
    {
        const auto record = *iterator;
        record.read(inputFieldIdentifier).writeToMemory(payload.getContent() + (index * fieldSize));
        index = index + 1;
    }

    Record result;
    result.write(resultFieldIdentifier, VarVal{payload});
    return result;
}

void ArrayAggAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = tupleLayout->getSchema().getSizeInBytes();
    const auto childBufferIndex = nautilus::invoke(
        +[](TupleBuffer* parent, AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
        {
            if (auto pagedVectorBuffer = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                PagedVector::init(pagedVectorBuffer.value(), bufferProvider->getBufferSize(), tupleSize);
                return parent->storeChildBuffer(*pagedVectorBuffer).getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for ARRAY_AGG PagedVector");
        },
        parentBuffer,
        pipelineMemoryProvider.bufferProvider,
        tupleSize);
    *static_cast<nautilus::val<uint32_t*>>(aggregationState) = childBufferIndex;
}

void ArrayAggAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
    /// The PagedVector root is a child of the parent hash-map buffer and is released with its parent.
}

size_t ArrayAggAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(ChildBufferIndex::Underlying);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterArrayAggAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "ARRAY_AGG tuple layout not set");
    return std::make_shared<ArrayAggAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        std::move(arguments.inputFunction),
        std::move(arguments.resultFieldIdentifier),
        std::move(arguments.tupleLayout.value()));
}
}
