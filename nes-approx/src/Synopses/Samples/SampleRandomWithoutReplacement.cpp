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

#include <API/AttributeField.hpp>
#include <Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Synopses/Samples/SampleRandomWithoutReplacement.hpp>
#include <Util/UtilityFunctions.hpp>
#include <random>

namespace NES::ASP{

void SampleRandomWithoutReplacement::addToSynopsis(Nautilus::Record record) {
    // Here we just store all records and then perform the sampling in getApproximate()

    if (storedRecords.empty()) {
        storedRecords.emplace_back(bufferManager->getBufferBlocking());
    }

    auto lastTupleBuffer = storedRecords[storedRecords.size() - 1];
    auto dynamicTupleBuffer = ASP::Util::createDynamicTupleBuffer(lastTupleBuffer, inputSchema);

    auto recordIndex = dynamicTupleBuffer.getNumberOfTuples();

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto baseAddress = Nautilus::MemRef((int8_t*) (dynamicTupleBuffer.getBuffer().getBuffer() +
                                       recordIndex * inputSchema->getSchemaSizeInBytes()));
    Nautilus::Value<Nautilus::MemRef> entryMemRef(baseAddress);
    for (auto& field : inputSchema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
    ++numberOfTuples;

    dynamicTupleBuffer.setNumberOfTuples(dynamicTupleBuffer.getNumberOfTuples() + 1);
    if (dynamicTupleBuffer.getNumberOfTuples() >= dynamicTupleBuffer.getCapacity()) {
        storedRecords.emplace_back(bufferManager->getBufferBlocking());
    }
}

std::vector<Runtime::TupleBuffer>
SampleRandomWithoutReplacement::getApproximate(Runtime::BufferManagerPtr bufferManager) {
    // First, we have to pick our sample
    std::vector<Runtime::TupleBuffer> sample;
    std::mt19937 generator(GENERATOR_SEED_DEFAULT);
    std::uniform_int_distribution<uint64_t> uniformIntDistribution(0, numberOfTuples - 1);
    auto numberOfTuplesInSample = 0UL;
    std::vector<uint64_t> alreadyDrawn;
    while (numberOfTuplesInSample < sampleSize && numberOfTuplesInSample < numberOfTuples) {
        uint64_t pos = uniformIntDistribution(generator);
        while (std::find(alreadyDrawn.begin(), alreadyDrawn.end(), pos) != alreadyDrawn.end()) {
            pos = uniformIntDistribution(generator);
        }
        alreadyDrawn.emplace_back(pos);
        NES_DEBUG("pos: " << pos);

        auto numberOfTuplesPerBuffer = bufferManager->getBufferSize() / inputSchema->getSchemaSizeInBytes();
        auto tupleBuffer = pos / numberOfTuplesPerBuffer;
        auto tuplePosition = pos % numberOfTuplesPerBuffer;


        if (sample.empty() || sample[sample.size() - 1].getNumberOfTuples() >= numberOfTuplesPerBuffer) {
            sample.emplace_back(bufferManager->getBufferBlocking());
        }
        auto sampleBuffer = sample[sample.size() - 1];

        auto srcMem = storedRecords[tupleBuffer].getBuffer() + tuplePosition * inputSchema->getSchemaSizeInBytes();
        auto destMem = sampleBuffer.getBuffer() + sampleBuffer.getNumberOfTuples() * inputSchema->getSchemaSizeInBytes();
        std::memcpy(destMem, srcMem, inputSchema->getSchemaSizeInBytes());
        sampleBuffer.setNumberOfTuples(sampleBuffer.getNumberOfTuples() + 1);
        ++numberOfTuplesInSample;
    }

    // Approximate over the sample and write the approximation into record
    auto aggregationValueMemRef = Nautilus::MemRef((int8_t*)aggregationValue.get());
    aggregationFunction->reset(aggregationValueMemRef);
    for (auto& buffer : sample) {
        NES_DEBUG("buffer in SampleRandomWithoutReplacement::getApproximate: " << NES::Util::printTupleBufferAsCSV(buffer, inputSchema));
        auto bufferAddress = Nautilus::Value<Nautilus::MemRef>((int8_t*) buffer.getBuffer());
        auto numberOfRecords = buffer.getNumberOfTuples();
        for (Nautilus::Value<Nautilus::UInt64> i = (uint64_t) 0; i < numberOfRecords; i = i + (uint64_t) 1) {
            auto memoryProvider = ASP::Util::createMemoryProvider(bufferManager->getBufferSize(), inputSchema);
            auto tmpRecord = memoryProvider->read({}, bufferAddress, i);
            auto tmpValue = tmpRecord.read(fieldNameAggregation);
            aggregationFunction->lift(aggregationValueMemRef, tmpValue);
        }
    }

    // Lower the aggregation
    Nautilus::Record record;
    auto approximatedValue = aggregationFunction->lower(aggregationValueMemRef);
    record.write(fieldNameApproximate, approximatedValue);

    // Create an output buffer and write the approximation into it
    auto memoryProvider = ASP::Util::createMemoryProvider(bufferManager->getBufferSize(), outputSchema);
    auto outputBuffer = bufferManager->getBufferBlocking();
    auto outputRecordBuffer = Runtime::Execution::RecordBuffer(Nautilus::Value<Nautilus::MemRef>((int8_t*) std::addressof(outputBuffer)));

    Nautilus::Value<Nautilus::UInt64> recordIndex(0UL);
    Nautilus::Value<Nautilus::UInt64> numRecords(1UL);
    auto bufferAddress = outputRecordBuffer.getBuffer();
    memoryProvider->write(recordIndex, bufferAddress, record);
    outputRecordBuffer.setNumRecords(numRecords);

    return {outputBuffer};
}

SampleRandomWithoutReplacement::SampleRandomWithoutReplacement(size_t sampleSize) : sampleSize(sampleSize) {}

void SampleRandomWithoutReplacement::initialize() {
    auto aggregationValueMemRef = Nautilus::MemRef((int8_t*)aggregationValue.get());
    storedRecords.clear();
    numberOfTuples = 0;
}

} // namespace NES::ASP