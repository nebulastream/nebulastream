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

#include <random>
#include <Execution/Operators/Streaming/Aggregation/Function/Synopsis/Sample/ReservoirSampleFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/Synopsis/Sample/SampleFunctionRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>

namespace NES::Runtime::Execution::Aggregation::Synopsis
{

uint64_t getRandomNumberProxy(const uint64_t upperBound)
{
    static std::mt19937 gen(ReservoirSampleFunction::GENERATOR_SEED);
    std::uniform_int_distribution<> dis(0, upperBound);

    return dis(gen);
}

ReservoirSampleFunction::ReservoirSampleFunction(
    std::shared_ptr<PhysicalType> inputType,
    std::shared_ptr<PhysicalType> resultType,
    std::unique_ptr<Functions::Function> inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector,
    const uint64_t reservoirSize)
    : SampleFunction(
          std::move(inputType),
          std::move(resultType),
          std::move(inputFunction),
          std::move(resultFieldIdentifier),
          std::move(memProviderPagedVector),
          reservoirSize)
    , currRecordIdx(0)
{
    PRECONDITION(reservoirSize != 0, "Reservoir size cannot be zero");
}

void ReservoirSampleFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector, pipelineMemoryProvider.bufferProvider);

    if (currRecordIdx < sampleSize)
    {
        pagedVectorRef.writeRecord(record);
        sampleDataSize += getRecordDataSize(record);
    }
    else
    {
        /// Replace records in the sample with gradually decreasing probability
        auto randomNumber = invoke(getRandomNumberProxy, currRecordIdx);
        if (randomNumber < sampleSize)
        {
            const auto oldRecord = pagedVectorRef.replaceRecord(record, randomNumber);
            sampleDataSize += getRecordDataSize(record) - getRecordDataSize(oldRecord);
        }
    }

    currRecordIdx += 1;
}

void ReservoirSampleFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    const auto pagedVectorPtr1 = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState1);
    const auto pagedVectorPtr2 = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState2);

    invoke(
        +[](Interface::PagedVector* vector1, const Interface::PagedVector* vector2) -> void { vector1->copyFrom(*vector2); },
        pagedVectorPtr1,
        pagedVectorPtr2);
}

Record ReservoirSampleFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector, pipelineMemoryProvider.bufferProvider);
    const auto allFieldNames = memProviderPagedVector->getMemoryLayout()->getSchema()->getFieldNames();
    auto sampleRef = SampleFunctionRef(
        pipelineMemoryProvider.arena, memProviderPagedVector->getMemoryLayout()->getSchema(), sampleSize, sampleDataSize);

    /// Store all records in the sample as PagedVector contains only sample elements
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != pagedVectorRef.end(allFieldNames); ++candidateIt)
    {
        sampleRef.writeRecord(*candidateIt);
    }

    /// Add the reservoir to the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, sampleRef.getSample());
    return resultRecord;
}

void ReservoirSampleFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Allocate a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVectorPtr = reinterpret_cast<Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVectorPtr) Interface::PagedVector();
        },
        aggregationState);
}

size_t ReservoirSampleFunction::getSizeOfStateInBytes() const
{
    return sizeof(Interface::PagedVector);
}

}
