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

#include <Aggregation/Function/ArrayAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <inline.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

ArrayAggregationPhysicalFunction::ArrayAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> inputBufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , inputBufferRef(std::move(inputBufferRef))
{
    pagedVectorBufferRef = Interface::BufferRef::TupleBufferRef::create(4096, Schema{}.addField("value", inputType));
}

NAUTILUS_INLINE void fixupVectorProxy(Nautilus::Interface::PagedVector* vector1)
{
    vector1->fixupPageNumbers();
}

NAUTILUS_INLINE void mergeVectorProxy(Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2)
{
    vector1->copyFromUnsafe(*vector2);
}

void ArrayAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record,
    const nautilus::val<Timestamp>&)
{
    /// Adding the record to the paged vector. We are storing the full record in the paged vector for now.
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(memArea, pagedVectorBufferRef);
    Record vectorTuple{};
    vectorTuple.write("value", inputFunction.execute(record, pipelineMemoryProvider.arena));
    pagedVectorRef.writeRecord(vectorTuple, pipelineMemoryProvider.bufferProvider);
}

void ArrayAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Getting the paged vectors from the aggregation states
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(mergeVectorProxy, memArea1, memArea2);
}

Nautilus::Record ArrayAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Getting the paged vector from the aggregation state
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    nautilus::invoke(fixupVectorProxy, pagedVectorPtr);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, pagedVectorBufferRef);
    const auto allFieldNames = pagedVectorBufferRef->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {
            const auto numberOfEntriesVal = pagedVector->getTotalNumberOfEntries();
            INVARIANT(numberOfEntriesVal > 0, "The number of entries in the paged vector must be greater than 0");
            return numberOfEntriesVal;
        },
        pagedVectorPtr);

    auto entrySize = pagedVectorBufferRef->getMemoryLayout()->getSchema().getSizeOfSchemaInBytes();

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(numberOfEntries * entrySize);

    /// Copy from paged vector
    const auto endIt = pagedVectorRef.end(allFieldNames);
    auto current = variableSized.getContent();
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;
        auto _ = itemRecord.read("value").customVisit(
            [&]<typename T>(const T& type) -> VarVal
            {
                if constexpr (std::is_same_v<T, VariableSizedData>)
                {
                    throw std::runtime_error("VariableSizedData is not supported in ArrayAggregationPhysicalFunction");
                }
                else
                {
                    *static_cast<nautilus::val<typename T::raw_type*>>(current) = type;
                    current += sizeof(typename T::raw_type);
                }
                return type;
            });
    }

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);

    return resultRecord;
}

void ArrayAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t ArrayAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::PagedVector);
}

void ArrayAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(
                pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterArray_AggAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.bufferRefPagedVector.has_value(), "Memory provider paged vector not set");

    return std::make_shared<ArrayAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.bufferRefPagedVector.value());
}

}
