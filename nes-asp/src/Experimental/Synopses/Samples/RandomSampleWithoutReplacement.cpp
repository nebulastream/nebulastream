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
#include <Experimental/Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Experimental/Synopses/Samples/RandomSampleWithoutReplacement.hpp>
#include <Experimental/Synopses/Samples/RandomSampleWithoutReplacementOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <random>

namespace NES::ASP {


uint64_t createSampleProxy(void* pagedVectorPtr, uint64_t sampleSize) {
    auto* pagedVector = (Nautilus::Interface::PagedVector*) pagedVectorPtr;
    const auto numberOfTuples = pagedVector->getNumberOfEntries();

    // Generating a uniform random distribution
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
    for (const auto& randomPos : allRandomPositions) {
        if (randomPos != cnt) {
            pagedVector->moveFromTo(randomPos, cnt);
        }
        cnt += 1;
    }

    return numberOfRecordsInSample;
}

void* getPagedVectorRefProxy(void* opHandlerPtr) {
    auto* opHandler = (RandomSampleWithoutReplacementOperatorHandler*) opHandlerPtr;
    return opHandler->getPagedVectorRef();
}

void setupOpHandlerProxy(void* opHandlerPtr, uint64_t entrySize) {
    auto* opHandler = (RandomSampleWithoutReplacementOperatorHandler*) opHandlerPtr;
    opHandler->setup(entrySize);
}

RandomSampleWithoutReplacement::RandomSampleWithoutReplacement(Parsing::SynopsisAggregationConfig& aggregationConfig,
                                                               size_t sampleSize, const std::string& keyFieldName):
                         AbstractSynopsis(aggregationConfig), sampleSize(sampleSize),
                         recordSize(inputSchema->getSchemaSizeInBytes()), keyFieldName(keyFieldName) {
}

void RandomSampleWithoutReplacement::addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx,
                                                   Nautilus::Record record,
                                                   Runtime::Execution::Operators::OperatorState*) {

    // TODO this can be pulled out of this function and into the open() #3743
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    auto pagedVectorMemRef = Nautilus::FunctionCall("getPagedVectorRefProxy", getPagedVectorRefProxy, opHandlerMemRef);
    auto pagedVectorRef = Nautilus::Interface::PagedVectorRef(pagedVectorMemRef, recordSize);

    auto entryMemRef = pagedVectorRef.allocateEntry();
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (const auto& field : inputSchema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

std::vector<Runtime::TupleBuffer> RandomSampleWithoutReplacement::getApproximate(uint64_t handlerIndex,
                                                                                 Runtime::Execution::ExecutionContext& ctx,
                                                                                 std::vector<Nautilus::Value<>>& keyValues,
                                                                                 Runtime::BufferManagerPtr bufferManager) {
    using namespace Runtime::Execution;

    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    auto pagedVectorMemRef = Nautilus::FunctionCall("getPagedVectorRefProxy", getPagedVectorRefProxy, opHandlerMemRef);
    auto pagedVectorRef = Nautilus::Interface::PagedVectorRef(pagedVectorMemRef, recordSize);
    auto numberOfRecordsInSample = Nautilus::FunctionCall("createSampleProxy", createSampleProxy, pagedVectorMemRef,
                                                          Nautilus::Value<Nautilus::UInt64>((uint64_t)sampleSize));


    auto memoryProviderInput = MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(),
                                                                                    inputSchema);
    auto memoryProviderOutput = MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(),
                                                                                     outputSchema);

    auto buffer = bufferManager->getBufferBlocking();
    auto maxRecordsPerBuffer = bufferManager->getBufferSize() / outputSchema->getSchemaSizeInBytes();
    std::vector<Runtime::TupleBuffer> retTupleBuffers;
    Nautilus::Value<Nautilus::UInt64> recordIndex((uint64_t) 0);
    for (auto& key : keyValues) {
        // Approximate over the sample and write the approximation into record
        auto aggregationValueMemRef = pagedVectorRef.allocateEntry();
        aggregationFunction->reset(aggregationValueMemRef);

        // Iterating over the sample and creating the aggregation for all keys
        Nautilus::Value<Nautilus::UInt64> zeroValue(0UL);
        for (auto it = pagedVectorRef.begin(); it != pagedVectorRef.at(numberOfRecordsInSample); ++it) {
            auto entryMemRef = *it;
            auto tmpRecord = memoryProviderInput->read({}, entryMemRef, zeroValue);
            if (readKeyExpression->execute(tmpRecord) == key) {
                aggregationFunction->lift(aggregationValueMemRef, tmpRecord);
            }
        }

        // Lower the aggregation
        Nautilus::Record record;
        aggregationFunction->lower(aggregationValueMemRef, record);
        auto scalingFactor = Nautilus::Value<Nautilus::Double>(getScalingFactor(pagedVectorRef));
        auto approximatedValue = multiplyWithScalingFactor(record.read(fieldNameApproximate), scalingFactor);

        // Writing the approximate to the record as well as the key

        record.write(fieldNameApproximate, approximatedValue);
        record.write(keyFieldName, key);


        // Writing the values to the buffer
        auto recordBuffer = RecordBuffer(Nautilus::Value<Nautilus::MemRef>((int8_t*) std::addressof(buffer)));
        auto bufferAddress = recordBuffer.getBuffer();
        memoryProviderOutput->write(recordIndex, bufferAddress, record);
        recordIndex = recordIndex + 1;
        recordBuffer.setNumRecords(recordIndex);

        if (recordIndex >= maxRecordsPerBuffer) {
            retTupleBuffers.emplace_back(buffer);
            buffer = bufferManager->getBufferBlocking();
            recordIndex = (uint64_t) 0;
        }
    }

    if (recordIndex > 0) {
        retTupleBuffers.emplace_back(buffer);
    }

    return retTupleBuffers;
}

void RandomSampleWithoutReplacement::setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext& ctx) {
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    Nautilus::FunctionCall("setupOpHandlerProxy", setupOpHandlerProxy, opHandlerMemRef,
                           Nautilus::Value<Nautilus::UInt64>(inputSchema->getSchemaSizeInBytes()));
}

double RandomSampleWithoutReplacement::getScalingFactor(Nautilus::Interface::PagedVectorRef& pagedVecRef){
    double retValue = 1;

    if ((aggregationType == Parsing::Aggregation_Type::COUNT) || (aggregationType == Parsing::Aggregation_Type::SUM)) {
        // As we use one entry in the PagedVector for calculating the approximate, we have to subtract it here
        auto numberOfTuplesInWindow = pagedVecRef.getTotalNumberOfEntries().getValue().getRawInt() - 1;
        auto minSize = std::min(sampleSize, (uint64_t) numberOfTuplesInWindow);
        retValue = ((double) numberOfTuplesInWindow / minSize);
    }

    return retValue;
}

Nautilus::Value<> RandomSampleWithoutReplacement::multiplyWithScalingFactor(Nautilus::Value<> approximatedValue,
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

bool RandomSampleWithoutReplacement::storeLocalOperatorState(uint64_t, const Runtime::Execution::Operators::Operator*,
                                                             Runtime::Execution::ExecutionContext&,
                                                             Runtime::Execution::RecordBuffer) {
    // TODO this will be used in issue #3743
    return false;
}
} // namespace NES::ASP