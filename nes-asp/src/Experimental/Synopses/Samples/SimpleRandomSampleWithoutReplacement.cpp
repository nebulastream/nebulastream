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
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/UtilityFunctions.hpp>
#include <random>

namespace NES::ASP{


uint64_t createSampleProxy(void* stackPtr, uint64_t sampleSize) {
    auto* stack = (Nautilus::Interface::Stack*) stackPtr;
    const auto numberOfTuples = stack->getNumberOfEntries();
    std::mt19937 generator(GENERATOR_SEED_DEFAULT);
    std::uniform_int_distribution<uint64_t> distribution(0, numberOfTuples - 1);
    auto numberOfRecordsInSample = std::min(sampleSize, numberOfTuples);

    // Creating as many random positions as necessary
    std::vector<uint64_t> allRandomPositions;
    for (auto i = 0UL; i < numberOfRecordsInSample; ++i) {
        auto randomPos = distribution(generator);
        while(std::find(allRandomPositions.begin(), allRandomPositions.end(), randomPos) != allRandomPositions.end()) {
            randomPos = distribution(generator);
        }
        allRandomPositions.emplace_back(randomPos);
    }

    // Sorting the random positions so that we can iterate through the vector and move all the records at the random positions to the front
    auto cnt = 0UL;
    std::sort(allRandomPositions.begin(), allRandomPositions.end());
    for (const auto randomPos : allRandomPositions) {
        stack->moveTo(randomPos, cnt++);
    }

    return numberOfRecordsInSample;
}

SimpleRandomSampleWithoutReplacement::SimpleRandomSampleWithoutReplacement(
    Parsing::SynopsisAggregationConfig& aggregationConfig, size_t sampleSize):
                         AbstractSynopsis(aggregationConfig), sampleSize(sampleSize), recordSize(inputSchema->getSchemaSizeInBytes()) {
}

void SimpleRandomSampleWithoutReplacement::addToSynopsis(Nautilus::Record record) {

    auto stackRef = Nautilus::Interface::StackRef(Nautilus::Value<Nautilus::MemRef>((int8_t*) stack.get()), recordSize);
    auto entryMemRef = stackRef.allocateEntry();
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : inputSchema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

std::vector<Runtime::TupleBuffer> SimpleRandomSampleWithoutReplacement::getApproximate(Runtime::BufferManagerPtr bufferManager) {
    Nautilus::Value<Nautilus::MemRef> stackMemRef((int8_t*) stack.get());
    auto stackRef = Nautilus::Interface::StackRef(stackMemRef, recordSize);

    auto numberOfRecordsInSample = Nautilus::FunctionCall("createSampleProxy", createSampleProxy, stackMemRef,
                                                          Nautilus::Value<Nautilus::UInt64>((uint64_t)sampleSize));

    // Approximate over the sample and write the approximation into record
    auto aggregationValueMemRef = Nautilus::MemRef((int8_t*)aggregationValue.get());
    aggregationFunction->reset(aggregationValueMemRef);

    auto memoryProvider = Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(),
                                                                                                   inputSchema);

    std::vector<Nautilus::Value<Nautilus::UInt64>> alreadyDrawn;
    auto entryMemRef = stackRef.getEntry(0UL);
    for (Nautilus::Value<Nautilus::UInt64> curTuple(0UL); curTuple < numberOfRecordsInSample; curTuple = curTuple + 1) {
        auto tmpRecord = memoryProvider->read({}, entryMemRef, curTuple);
        auto tmpValue = tmpRecord.read(fieldNameAggregation);
        aggregationFunction->lift(aggregationValueMemRef, tmpValue);
    }

    // Lower the aggregation
    Nautilus::Record record;
    auto approximatedValue = aggregationFunction->lower(aggregationValueMemRef);
    auto scalingFactor = Nautilus::Value<Nautilus::Double>(getScalingFactor(stackRef));
    approximatedValue = multiplyWithScalingFactor(approximatedValue, scalingFactor);
    record.write(fieldNameApproximate, approximatedValue);

    // Create an output buffer and write the approximation into it
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
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto entrySize = inputSchema->getSchemaSizeInBytes();
    stack = std::make_unique<Nautilus::Interface::Stack>(std::move(allocator), entrySize);
}

double SimpleRandomSampleWithoutReplacement::getScalingFactor(Nautilus::Interface::StackRef stackRef){
    double retValue = 1;

    if (std::dynamic_pointer_cast<Runtime::Execution::Aggregation::CountAggregationFunction>(aggregationFunction) ||
        std::dynamic_pointer_cast<Runtime::Execution::Aggregation::SumAggregationFunction>(aggregationFunction)) {
        double numberOfTuplesInWindow = stackRef.getTotalNumberOfEntries();
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