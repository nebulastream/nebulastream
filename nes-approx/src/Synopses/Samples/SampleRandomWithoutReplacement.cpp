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

#include <Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Synopses/Samples/SampleRandomWithoutReplacement.hpp>
#include <random>

namespace NES::ASP{

void SampleRandomWithoutReplacement::addToSynopsis(Nautilus::Record record) {
    // Here we just store all records and then perform the sampling in getApproximate()
    storedRecords.emplace_back(record);
}

std::vector<Runtime::TupleBuffer>
SampleRandomWithoutReplacement::getApproximate(Runtime::BufferManagerPtr bufferManager) {
    // First, we have to pick our sample
    std::vector<Nautilus::Record> sample;
    std::mt19937 generator(GENERATOR_SEED_DEFAULT);
    std::uniform_int_distribution<uint64_t> uniformIntDistribution(0, storedRecords.size());
    while (sample.size() < sampleSize) {
        uint64_t pos = uniformIntDistribution(generator);
        sample.emplace_back(storedRecords[pos]);
    }

    // Approximate over the sample and write the approximation into record
    Nautilus::Record record;
    auto aggregationValueMemRef = Nautilus::MemRef((int8_t*)aggregationValue.get());
    aggregationFunction->reset(aggregationValueMemRef);
    for (auto& item : sample) {
        aggregationFunction->lift(aggregationValueMemRef, item.read(fieldNameAggregation));
    }
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
    storedRecords.clear();
}

} // namespace NES::ASP