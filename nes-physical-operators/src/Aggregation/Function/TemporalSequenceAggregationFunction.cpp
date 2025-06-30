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
#include <Aggregation/Function/AggregationFunction.hpp>
#include <Aggregation/Function/TemporalSequenceAggregationFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

TemporalSequenceAggregationFunction::TemporalSequenceAggregationFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    PhysicalFunction lonFunction,
    PhysicalFunction latFunction,
    PhysicalFunction timestampFunction,
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , lonFunction(std::move(lonFunction))
    , latFunction(std::move(latFunction))
    , timestampFunction(std::move(timestampFunction))
    , memProviderPagedVector(std::move(memProviderPagedVector))
{
}

void TemporalSequenceAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    // Store the full record in the paged vector (like MedianAggregationFunction does)
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(memArea, memProviderPagedVector, pipelineMemoryProvider.bufferProvider);
    pagedVectorRef.writeRecord(record, pipelineMemoryProvider.bufferProvider);
}

void TemporalSequenceAggregationFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    // Get the paged vectors from the aggregation states
    const auto memArea1 = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState2);

    // Copy the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](Interface::PagedVector* vector1, const Interface::PagedVector* vector2) -> void { vector1->copyFrom(*vector2); },
        memArea1,
        memArea2);
}

Nautilus::Record
TemporalSequenceAggregationFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    // Get the paged vector from the aggregation state
    const auto pagedVectorPtr = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector, pipelineMemoryProvider.bufferProvider);
    const auto allFieldNames = memProviderPagedVector->getMemoryLayout()->getSchema().getFieldNames();

    // Get number of trajectory points
    const auto numberOfPoints = invoke(
        +[](const Interface::PagedVector* pagedVector)
        {
            const auto numberOfPointsVal = pagedVector->getTotalNumberOfEntries();
            INVARIANT(numberOfPointsVal > 0, "The number of entries in the paged vector must be greater than 0");
            return numberOfPointsVal;
        },
        pagedVectorPtr);

    // Iterate over the array
    



    // Create and return the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, Nautilus::VarVal(varsizedData));
    return resultRecord;
}

void TemporalSequenceAggregationFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            // Allocates a new PagedVector in the memory area
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

void TemporalSequenceAggregationFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            // Calls the destructor of the PagedVector
            auto* pagedVector = reinterpret_cast<Interface::PagedVector*>(pagedVectorMemArea);
            pagedVector->~PagedVector();
        },
        aggregationState);
}

size_t TemporalSequenceAggregationFunction::getSizeOfStateInBytes() const
{
    return sizeof(Interface::PagedVector);
}

} // namespace NES