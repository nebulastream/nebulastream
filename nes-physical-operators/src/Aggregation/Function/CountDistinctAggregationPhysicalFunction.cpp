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

#include <Aggregation/Function/CountDistinctAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <vector>
#include <utility>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <DataTypes/VarVal.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <select.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
/// The paged vector stores only the counted value, so lowering does not read the other fields of the input records.
const Record::RecordFieldIdentifier valueField{Identifier::parse("countDistinctValue")};

std::shared_ptr<PagedVectorTupleLayout> valueLayout(const DataType& valueType)
{
    return std::make_shared<DefaultPagedVectorTupleLayout>(
        std::vector{QualifiedUnboundField{valueField, valueType}} | std::ranges::to<Schema<QualifiedUnboundField, Ordered>>());
}

/// The aggregation state holds only the child-buffer index of the paged vector that stores the values.
OwnedNautilusBuffer loadPagedVectorBuffer(const nautilus::val<AggregationState*>& aggregationState, BorrowedNautilusBuffer parentBuffer)
{
    OwnedNautilusBuffer pagedVectorBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr) { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer.asArg(),
        pagedVectorBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState)));
    return pagedVectorBuffer;
}
}

CountDistinctAggregationPhysicalFunction::CountDistinctAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> /*inputTupleLayout*/)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(valueLayout(this->inputType))
{
}

void CountDistinctAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    /// SQL-standard: NULL is not a value, so COUNT(DISTINCT x) ignores records whose x is NULL.
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (not value.isNull())
    {
        auto pagedVectorBuffer = loadPagedVectorBuffer(aggregationState, parentBuffer);
        PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVectorBuffer.asArg()), tupleLayout);
        Record valueRecord;
        valueRecord.write(valueField, value);
        pagedVectorRef.pushBack(valueRecord, pipelineMemoryProvider.bufferProvider);
    }
}

void CountDistinctAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer parentBuffer1,
    const nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Duplicates across the two states are resolved when lowering.
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider, TupleBuffer* parent1, const uint32_t* indexPtr1, TupleBuffer* parent2, const uint32_t* indexPtr2)
        {
            const TupleBuffer vector1Buffer = parent1->loadChildBuffer(ChildBufferIndex{*indexPtr1});
            const TupleBuffer vector2Buffer = parent2->loadChildBuffer(ChildBufferIndex{*indexPtr2});
            auto vector1 = PagedVector::load(vector1Buffer);
            const auto vector2 = PagedVector::load(vector2Buffer);
            vector1.copyPagesFrom(*bufferProvider, vector2);
        },
        pipelineMemoryProvider.bufferProvider,
        parentBuffer1.asArg(),
        static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState1)),
        parentBuffer2.asArg(),
        static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState2)));
}

Record CountDistinctAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& /*pipelineMemoryProvider*/)
{
    auto pagedVectorBuffer = loadPagedVectorBuffer(aggregationState, parentBuffer);
    const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVectorBuffer.asArg()), tupleLayout);
    const auto pagedVectorEnd = pagedVectorRef.end();

    /// Counts each value at its first occurrence: a candidate is new unless an earlier record holds an equal value.
    nautilus::val<uint64_t> distinctValues = 0;
    nautilus::val<uint64_t> candidatePosition = 0;
    for (auto candidateIt = pagedVectorRef.begin(); candidateIt != pagedVectorEnd; ++candidateIt)
    {
        const auto candidateValue = (*candidateIt).read(valueField);
        nautilus::val<bool> seenBefore = false;
        nautilus::val<uint64_t> earlierPosition = 0;
        for (auto earlierIt = pagedVectorRef.begin(); earlierPosition < candidatePosition and not seenBefore; ++earlierIt)
        {
            const auto earlierValue = (*earlierIt).read(valueField);
            seenBefore = (earlierValue == candidateValue).getRawValueAs<nautilus::val<bool>>();
            earlierPosition = earlierPosition + 1;
        }
        distinctValues = distinctValues + nautilus::select(seenBefore, nautilus::val<uint64_t>{0}, nautilus::val<uint64_t>{1});
        candidatePosition = candidatePosition + 1;
    }

    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, VarVal{distinctValues}.castToType(resultType.type));
    return resultRecord;
}

void CountDistinctAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = getSizeInBytes(tupleLayout->getSchema());
    const nautilus::val<uint32_t> childBufferIndex = nautilus::invoke(
        +[](TupleBuffer* parent, AbstractBufferProvider* bufferProvider, const uint64_t tupleSizeInBytes)
        {
            if (auto pagedVectorBuffer = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                PagedVector::init(pagedVectorBuffer.value(), bufferProvider->getBufferSize(), tupleSizeInBytes);
                return parent->storeChildBuffer(pagedVectorBuffer.value()).getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for the COUNT(DISTINCT) paged vector");
        },
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider,
        tupleSize);
    *static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState)) = childBufferIndex;
}

void CountDistinctAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> /*aggregationState*/)
{
    /// The paged vector is a child buffer of the parent hash map buffer and is released with it.
}

size_t CountDistinctAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(uint32_t);
}

AggregationPhysicalFunctionRegistryReturnType
CountDistinctAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "Tuple layout paged vector not set");
    return std::make_shared<CountDistinctAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.tupleLayout.value());
}

}
