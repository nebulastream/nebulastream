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

#include <Aggregation/Function/Synopsis/Sample/ReservoirSamplePhysicalFunction.hpp>

#include <random>
#include <Aggregation/Function/Synopsis/Sample/SamplePhysicalFunction.hpp>
#include <Aggregation/Function/Synopsis/SynopsisFunctionRef.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>

namespace NES
{

uint64_t getRandomNumberProxy(const uint64_t upperBound)
{
    static std::mt19937 gen(ReservoirSamplePhysicalFunction::GENERATOR_SEED);
    std::uniform_int_distribution<> dis(0, upperBound);

    return dis(gen);
}

ReservoirSamplePhysicalFunction::ReservoirSamplePhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector,
    const uint64_t reservoirSize)
    : SamplePhysicalFunction(
          std::move(inputType),
          std::move(resultType),
          std::move(inputFunction),
          std::move(resultFieldIdentifier),
          std::move(memProviderPagedVector),
          reservoirSize)
{
    PRECONDITION(reservoirSize != 0, "Reservoir size cannot be zero");
}

void ReservoirSamplePhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector);

    const auto curRecordIdxPtr = pagedVectorPtr + nautilus::val<uint64_t>(sizeof(Interface::PagedVector));
    const auto sampleDataSizePtr = curRecordIdxPtr + nautilus::val<uint64_t>(sizeof(uint64_t));
    auto currRecordIdx = Nautilus::Util::readValueFromMemRef<uint64_t>(curRecordIdxPtr);
    auto sampleDataSize = Nautilus::Util::readValueFromMemRef<uint64_t>(sampleDataSizePtr);

    if (currRecordIdx < sampleSize)
    {
        pagedVectorRef.writeRecord(record, pipelineMemoryProvider.bufferProvider);
        sampleDataSize = sampleDataSize + getRecordDataSize(record);
    }
    else
    {
        /// Replace records in the sample with gradually decreasing probability
        auto randomNumber = invoke(getRandomNumberProxy, currRecordIdx);
        if (randomNumber < sampleSize)
        {
            const auto oldRecord = pagedVectorRef.replaceRecord(record, randomNumber, pipelineMemoryProvider.bufferProvider);
            sampleDataSize = sampleDataSize + getRecordDataSize(record) - getRecordDataSize(oldRecord);
        }
    }
    currRecordIdx = currRecordIdx + nautilus::val<uint64_t>(1);

    VarVal(currRecordIdx).writeToMemory(curRecordIdxPtr);
    VarVal(sampleDataSize).writeToMemory(sampleDataSizePtr);
}

void ReservoirSamplePhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    // TODO Why is this not used?
    (void)pipelineMemoryProvider;
    const auto pagedVectorPtr1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto pagedVectorPtr2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);

    const auto curRecordIdxPtr1 = pagedVectorPtr1 + nautilus::val<uint64_t>(sizeof(Interface::PagedVector));
    const auto sampleDataSizePtr1 = curRecordIdxPtr1 + nautilus::val<uint64_t>(sizeof(uint64_t));
    auto currRecordIdx1 = Nautilus::Util::readValueFromMemRef<uint64_t>(curRecordIdxPtr1);
    auto sampleDataSize1 = Nautilus::Util::readValueFromMemRef<uint64_t>(sampleDataSizePtr1);

    const auto curRecordIdxPtr2 = pagedVectorPtr2 + nautilus::val<uint64_t>(sizeof(Interface::PagedVector));
    const auto sampleDataSizePtr2 = curRecordIdxPtr2 + nautilus::val<uint64_t>(sizeof(uint64_t));
    auto currRecordIdx2 = Nautilus::Util::readValueFromMemRef<uint64_t>(curRecordIdxPtr2);
    auto sampleDataSize2 = Nautilus::Util::readValueFromMemRef<uint64_t>(sampleDataSizePtr2);

    const auto curRecordIdx = currRecordIdx1 + currRecordIdx2;
    VarVal(curRecordIdx).writeToMemory(curRecordIdxPtr1);
    const auto sampleDataSize = sampleDataSize1 + sampleDataSize2;
    VarVal(sampleDataSize).writeToMemory(sampleDataSizePtr1);

    invoke(
        +[](Interface::PagedVector* vector1, const Interface::PagedVector* vector2) -> void { vector1->copyFrom(*vector2); },
        pagedVectorPtr1,
        pagedVectorPtr2);
}

Record ReservoirSamplePhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector);
    const auto allFieldNames = memProviderPagedVector->getMemoryLayout()->getSchema().getFieldNames();
    auto sampleRef = SynopsisFunctionRef(memProviderPagedVector->getMemoryLayout()->getSchema());

    const auto curRecordIdxPtr = pagedVectorPtr + nautilus::val<uint64_t>(sizeof(Interface::PagedVector));
    const auto sampleDataSizePtr = curRecordIdxPtr + nautilus::val<uint64_t>(sizeof(uint64_t));
    auto currRecordIdx = Nautilus::Util::readValueFromMemRef<uint64_t>(curRecordIdxPtr);
    auto sampleDataSize = Nautilus::Util::readValueFromMemRef<uint64_t>(sampleDataSizePtr);

    // sample might not contain sampleSize number of tuples if there were not enough tuples in the window
    const auto actualSampleSize = currRecordIdx > sampleSize ? sampleSize : currRecordIdx;
    sampleRef.initializeForWriting(
        pipelineMemoryProvider.arena, sampleDataSize, nautilus::val<uint32_t>(sizeof(uint64_t)), actualSampleSize);

    /// Store all records in the sample as PagedVector contains only sample elements
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != pagedVectorRef.end(allFieldNames); ++candidateIt)
    {
        sampleRef.writeRecord(*candidateIt);
    }

    /// Add the reservoir to the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, sampleRef.getSynopsis());
    return resultRecord;
}

void ReservoirSamplePhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVector = reinterpret_cast<Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Interface::PagedVector();

            /// MemSet the two uint64_t values to 0. One for the sampleDataSize and one for the currentRecordIndex
            std::memset(reinterpret_cast<int8_t*>(pagedVector) + sizeof(Interface::PagedVector), 0, sizeof(uint64_t) * 2);
        },
        aggregationState);
}

void ReservoirSamplePhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector
                = reinterpret_cast<Interface::PagedVector*>(pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}

size_t ReservoirSamplePhysicalFunction::getSizeOfStateInBytes() const
{
    // TODO(nikla44): we should use the SampleFunction::getSizeOfStateInBytes() method
    return sizeof(Interface::PagedVector) + sizeof(uint64_t) + sizeof(uint64_t);
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterReservoirSampleAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    /// TODO Get reservoir size
    uint64_t reservoirSize = 5;

    return std::make_shared<ReservoirSamplePhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.memProviderPagedVector.value(),
        reservoirSize);
}


}
