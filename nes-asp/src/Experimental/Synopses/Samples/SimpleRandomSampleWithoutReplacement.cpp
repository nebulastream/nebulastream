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
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Experimental/Synopses/Samples/SimpleRandomSampleWithoutReplacement.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/UtilityFunctions.hpp>
#include <random>

namespace NES::ASP{

SimpleRandomSampleWithoutReplacement::SimpleRandomSampleWithoutReplacement(
    Parsing::SynopsisAggregationConfig& aggregationConfig, size_t sampleSize):
                         AbstractSynopsis(aggregationConfig), sampleSize(sampleSize), recordSize(inputSchema->getSchemaSizeInBytes()) {
    SimpleRandomSampleWithoutReplacement::initialize();
}

void SimpleRandomSampleWithoutReplacement::addToSynopsis(Nautilus::Record record) {

    Nautilus::Interface::StackRef stackRef = Nautilus::Interface::StackRef(Nautilus::Value<Nautilus::MemRef>((int8_t*) stack.get()), recordSize);
    auto entryMemRef = stackRef.allocateEntry();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto& field : inputSchema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

std::vector<Runtime::TupleBuffer> SimpleRandomSampleWithoutReplacement::getApproximate(Runtime::BufferManagerPtr bufferManager) {
    Nautilus::Interface::StackRef stackRef = Nautilus::Interface::StackRef(Nautilus::Value<Nautilus::MemRef>((int8_t*) stack.get()), recordSize);
    auto numberOfTuples = stackRef.getNumberOfEntries();

    // First, we have to pick our sample and store it in a vector of Nautilus::Records
    hier weiter machen!!!


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
        NES_TRACE2("buffer in SampleRandomWithoutReplacement::getApproximate: {}", NES::Util::printTupleBufferAsCSV(buffer, inputSchema));
        auto bufferAddress = Nautilus::Value<Nautilus::MemRef>((int8_t*) buffer.getBuffer());
        auto numberOfRecords = buffer.getNumberOfTuples();
        for (Nautilus::Value<Nautilus::UInt64> i = (uint64_t) 0; i < numberOfRecords; i = i + (uint64_t) 1) {
            auto memoryProvider = Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(), inputSchema);
            auto tmpRecord = memoryProvider->read({}, bufferAddress, i);
            auto tmpValue = tmpRecord.read(fieldNameAggregation);
            aggregationFunction->lift(aggregationValueMemRef, tmpValue);
        }
    }

    // Lower the aggregation
    Nautilus::Record record;
    auto approximatedValue = aggregationFunction->lower(aggregationValueMemRef);
    auto scalingFactor = Nautilus::Value<Nautilus::Double>(getScalingFactor());
    approximatedValue = multiplyWithScalingFactor(approximatedValue, scalingFactor);
    record.write(fieldNameApproximate, approximatedValue);

    // Create an output buffer and write the approximation into it
    auto memoryProvider = Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(), outputSchema);
    auto outputBuffer = bufferManager->getBufferBlocking();
    auto outputRecordBuffer = Runtime::Execution::RecordBuffer(Nautilus::Value<Nautilus::MemRef>((int8_t*) std::addressof(outputBuffer)));

    Nautilus::Value<Nautilus::UInt64> recordIndex(0UL);
    Nautilus::Value<Nautilus::UInt64> numRecords(1UL);
    auto bufferAddress = outputRecordBuffer.getBuffer();
    memoryProvider->write(recordIndex, bufferAddress, record);
    outputRecordBuffer.setNumRecords(numRecords);

    return {outputBuffer};
}

void SimpleRandomSampleWithoutReplacement::initialize() {
    auto aggregationValueMemRef = Nautilus::MemRef((int8_t*)aggregationValue.get());

    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = inputSchema->getSchemaSizeInBytes();
    stack = std::make_unique<Nautilus::Interface::Stack>(std::move(allocator), entrySize);
}

double SimpleRandomSampleWithoutReplacement::getScalingFactor(){
    double retValue = 1;

    if (std::dynamic_pointer_cast<Runtime::Execution::Aggregation::CountAggregationFunction>(aggregationFunction) ||
        std::dynamic_pointer_cast<Runtime::Execution::Aggregation::SumAggregationFunction>(aggregationFunction)) {
        double numberOfTuplesInWindow = NES::Util::getNumberOfTuples(storedRecords);
        double minSize = std::min((double) sampleSize, numberOfTuplesInWindow);
        retValue = ((double) numberOfTuplesInWindow / minSize);
    }

    return retValue;
}

Nautilus::Value<>
SimpleRandomSampleWithoutReplacement::multiplyWithScalingFactor(Nautilus::Value<> approximatedValue,
                                                                Nautilus::Value<Nautilus::Double> scalingFactor) {
    auto tmpValue = Nautilus::Value<>(approximatedValue * scalingFactor);
    double value = tmpValue.getValue().staticCast<Nautilus::Double>().getValue();

    if (approximatedValue->isType<Nautilus::Int8>()) {
        return Nautilus::Value<Nautilus::Int8>((int8_t)value);
    } else if (approximatedValue->isType<Nautilus::Int16>()) {
        return Nautilus::Value<Nautilus::Int16>((int16_t)value);
    } else if (approximatedValue->isType<Nautilus::Int32>()) {
        return Nautilus::Value<Nautilus::Int32>((int32_t)value);
    } else if (approximatedValue->isType<Nautilus::Int64>()) {
        return Nautilus::Value<Nautilus::Int64>((int64_t)value);
    } else if (approximatedValue->isType<Nautilus::UInt8>()) {
        return Nautilus::Value<Nautilus::UInt8>((uint8_t)value);
    } else if (approximatedValue->isType<Nautilus::UInt16>()) {
        return Nautilus::Value<Nautilus::UInt16>((uint16_t)value);
    } else if (approximatedValue->isType<Nautilus::UInt32>()) {
        return Nautilus::Value<Nautilus::UInt32>((uint32_t)value);
    } else if (approximatedValue->isType<Nautilus::UInt64>()) {
        return Nautilus::Value<Nautilus::UInt64>((uint64_t)value);
    } else if (approximatedValue->isType<Nautilus::Float>()) {
        return Nautilus::Value<Nautilus::Float>((float)value);
    } else if (approximatedValue->isType<Nautilus::Double>()) {
        return Nautilus::Value<Nautilus::Double>((double)value);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

} // namespace NES::ASP