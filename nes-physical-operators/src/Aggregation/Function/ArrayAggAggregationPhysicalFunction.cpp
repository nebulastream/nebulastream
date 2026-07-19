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
#include <Arena.hpp>
#include <CompilationContext.hpp>
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
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
std::atomic_uint64_t nextArrayAggComparatorIdentifier{0};

Record::RecordFieldIdentifier getArrayAggInputFieldIdentifier(const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout)
{
    INVARIANT(tupleLayout != nullptr, "ARRAY_AGG PagedVector tuple layout must not be null");
    INVARIANT(tupleLayout->getSchema().size() > 0, "ARRAY_AGG PagedVector layout must contain an input field");
    return tupleLayout->getSchema()[0]->getFullyQualifiedName();
}
}

ArrayAggAggregationPhysicalFunction::ArrayAggAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    Record::RecordFieldIdentifier orderingFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
    , inputFieldIdentifier(getArrayAggInputFieldIdentifier(this->tupleLayout))
    , orderingFieldIdentifier(std::move(orderingFieldIdentifier))
    , comparatorIdentifier(fmt::format("array_agg_{}", nextArrayAggComparatorIdentifier.fetch_add(1, std::memory_order_relaxed)))
{
    PRECONDITION(this->tupleLayout != nullptr, "ARRAY_AGG PagedVector tuple layout must not be null");
    PRECONDITION(this->inputType.type != DataType::Type::VARSIZED, "ARRAY_AGG requires fixed-size input");
}

void ArrayAggAggregationPhysicalFunction::setup(CompilationContext& compilationContext)
{
    comparatorOwners.emplace_back(PagedVectorRef::registerComparator(
        compilationContext,
        comparatorIdentifier,
        tupleLayout,
        [orderingFieldIdentifier = orderingFieldIdentifier](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
        { return (lhs.read(orderingFieldIdentifier) < rhs.read(orderingFieldIdentifier)).getRawValueAs<nautilus::val<bool>>(); }));
}

void ArrayAggAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    PagedVectorRef pagedVector(BorrowedNautilusBuffer::from(memArea), tupleLayout);
    if (inputType.nullable)
    {
        const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
        if (not value.isNull())
        {
            pagedVector.pushBack(record, pipelineMemoryProvider.bufferProvider);
        }
    }
    else
    {
        pagedVector.pushBack(record, pipelineMemoryProvider.bufferProvider);
    }
}

void ArrayAggAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider, TupleBuffer* vector1Buffer, const TupleBuffer* vector2Buffer)
        {
            auto vector1 = PagedVector::load(*vector1Buffer);
            const auto vector2 = PagedVector::load(*vector2Buffer);
            vector1.copyPagesFrom(*bufferProvider, vector2);
        },
        pipelineMemoryProvider.bufferProvider,
        static_cast<nautilus::val<TupleBuffer*>>(aggregationState1),
        static_cast<nautilus::val<TupleBuffer*>>(aggregationState2));
}

Record ArrayAggAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    INVARIANT(not comparatorOwners.empty(), "ARRAY_AGG comparator must be registered before lowering");
    const auto pagedVectorBuffer = static_cast<nautilus::val<TupleBuffer*>>(aggregationState);
    PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVectorBuffer), tupleLayout);
    pagedVectorRef.sort(comparatorOwners.back(), pipelineMemoryProvider.arena);

    const auto numberOfRecords = pagedVectorRef.getNumberOfRecords();
    const auto fieldSize
        = nautilus::val<uint64_t>{DataTypeProvider::provideDataType(inputType.type).getSizeInBytesWithNull()};
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
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = tupleLayout->getSchema().getSizeInBytes();
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea, AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
        {
            auto* pagedVectorBufferMemArea = reinterpret_cast<TupleBuffer*>( /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                pagedVectorMemArea);
            if (auto pagedVectorBuffer = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                PagedVector::init(pagedVectorBuffer.value(), bufferProvider->getBufferSize(), tupleSize);
                new (pagedVectorBufferMemArea) TupleBuffer(pagedVectorBuffer.value());
                return;
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for ARRAY_AGG PagedVector");
        },
        aggregationState,
        pipelineMemoryProvider.bufferProvider,
        tupleSize);
}

void ArrayAggAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea)
        {
            auto* pagedVectorBuffer = reinterpret_cast<TupleBuffer*>( /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                pagedVectorMemArea);
            pagedVectorBuffer->~TupleBuffer();
        },
        aggregationState);
}

size_t ArrayAggAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(TupleBuffer);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterArrayAggAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "ARRAY_AGG tuple layout not set");
    INVARIANT(arguments.orderingFieldIdentifier.has_value(), "ARRAY_AGG ordering field not set");
    return std::make_shared<ArrayAggAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        std::move(arguments.inputFunction),
        std::move(arguments.resultFieldIdentifier),
        std::move(arguments.tupleLayout.value()),
        std::move(arguments.orderingFieldIdentifier.value()));
}
}
