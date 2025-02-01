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
#include <random>
#include <utility>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/Synopsis/SampleWithoutReplacementFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>

static uint64_t getRandomNumberProxy(const uint64_t upperBound)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, upperBound);

    return dis(gen);
}

namespace NES::Runtime::Execution::Synopsis
{

SampleWithoutReplacementFunction::SampleWithoutReplacementFunction(
    PhysicalTypePtr inputType,
    PhysicalTypePtr resultType,
    std::unique_ptr<Functions::Function> inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector,
    uint64_t sampleSize)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , memProviderPagedVector(std::move(memProviderPagedVector)), sampleSize(sampleSize)
{
}

void SampleWithoutReplacementFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider,
    const Nautilus::Record& record)
{
    /// Adding the record to the paged vector. We are storing the full record in the paged vector for now.
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(memArea, memProviderPagedVector, bufferProvider);
    pagedVectorRef.writeRecord(record);
}

void SampleWithoutReplacementFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    const nautilus::val<Memory::AbstractBufferProvider*>&)
{
    /// Getting the paged vectors from the aggregation states
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2) -> void
        { vector1->copyFrom(*vector2); },
        memArea1,
        memArea2);
}

Nautilus::Record SampleWithoutReplacementFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
{
    /// Getting the paged vector from the aggregation state
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    nautilus::val<uint64_t*> alreadySeen = nautilus::invoke(+[](const uint64_t sampleSize){ return new uint64_t[sampleSize]; }, sampleSize);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector, bufferProvider);
    const auto allFieldNames = memProviderPagedVector->getMemoryLayoutPtr()->getSchema()->getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {
            const auto numberOfEntriesVal = pagedVector->getTotalNumberOfEntries();
            INVARIANT(numberOfEntriesVal > 0, "The number of entries in the paged vector must be greater than 0");
            return numberOfEntriesVal;
        },
        pagedVectorPtr);

    /// Create a new memory provider for our result
    auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    auto layout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(schema, 128);
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> const resultMemoryProvider = std::make_shared<Nautilus::Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(layout);

    nautilus::val<int8_t*> resultStorage = nautilus::invoke(+[](Memory::AbstractBufferProvider* provider, uint64_t sampleSize)
    {
        auto tuplebuffer = provider->getUnpooledBuffer(100).value();

        /// write string into buffer
        auto buffer = tuplebuffer.getBuffer<uint32_t>();
        *buffer = sampleSize;

        /// will leak
        tuplebuffer.retain();
        return tuplebuffer.getBuffer<uint64_t>();
    }, resultMemoryProvider, sampleSize);
    auto resultBuffer = Nautilus::RecordBuffer(resultStorage);

    /// Picking a random candidate and adding it if we haven't seen it yet
    nautilus::val<uint64_t> seenIndex = 0;
    while (seenIndex < sampleSize)
    {
        nautilus::val<bool> randomUnseen = true;
        nautilus::val<uint64_t> randomIndex = nautilus::invoke(getRandomNumberProxy, numberOfEntries);
        for (nautilus::val<uint64_t> i = 0; i < seenIndex; ++i)
        {
            const nautilus::val<uint64_t> currentValue = alreadySeen[i];
            if (currentValue == randomIndex)
            {
                randomUnseen = false;
                break;
            }
        }
        if (randomUnseen)
        {
            alreadySeen[seenIndex] = randomIndex;
            const auto candidateRecord = *pagedVectorRef.at(allFieldNames, randomIndex);
            const auto candidateValue = inputFunction->execute(candidateRecord);
            /// TODO Save candidateValue
            resultMemoryProvider->writeRecord(seenIndex, resultBuffer, candidateRecord);
            ++seenIndex;
        }

    }

    Nautilus::VariableSizedData resultStorageVarSized(resultStorage);
    const Nautilus::VarVal varSizedData{resultStorageVarSized};
    Nautilus::Record resultRecord;
    resultRecord.write("SamplingWoR", resultStorageVarSized);

    nautilus::invoke(+[](uint64_t* alreadySeen){ delete[] alreadySeen; }, alreadySeen);

    return resultRecord;
}

void SampleWithoutReplacementFunction::reset(
    const nautilus::val<AggregationState*> aggregationState, const nautilus::val<Memory::AbstractBufferProvider*>&)
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

size_t SampleWithoutReplacementFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::PagedVector);
}

}
