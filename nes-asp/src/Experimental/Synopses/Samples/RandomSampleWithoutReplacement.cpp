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


void createSampleProxy(void* pagedVectorPtr, uint64_t sampleSize) {
    auto* pagedVector = (Nautilus::Interface::PagedVector*) pagedVectorPtr;
    const uint64_t numberOfTuples = pagedVector->getNumberOfEntries();

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
}

void* getPagedVectorRefProxy(void* opHandlerPtr) {
    auto* opHandler = (RandomSampleWithoutReplacementOperatorHandler*) opHandlerPtr;
    return opHandler->getPagedVectorRef();
}

void setupSampleOpHandlerProxy(void* opHandlerPtr, uint64_t entrySize, uint64_t pageSize) {
    auto* opHandler = (RandomSampleWithoutReplacementOperatorHandler*) opHandlerPtr;
    opHandler->setup(entrySize, pageSize);
}

RandomSampleWithoutReplacement::RandomSampleWithoutReplacement(Parsing::SynopsisAggregationConfig& aggregationConfig,
                                                               uint64_t sampleSize, uint64_t entrySize, uint64_t pageSize):
    AbstractSynopsis(aggregationConfig), sampleSize(sampleSize), entrySize(entrySize), pageSize(pageSize) {
}

void RandomSampleWithoutReplacement::addToSynopsis(const uint64_t, Runtime::Execution::ExecutionContext &,
                                                   Nautilus::Record record,
                                                   const Runtime::Execution::Operators::OperatorState* pState) {

    // Retrieving the reference to the sampleStorage
    NES_ASSERT2_FMT(pState != nullptr, "Local state was null, but we expected it not to be null!");
    auto localState = dynamic_cast<const LocalRandomSampleOperatorState*>(pState);
    auto& sampleStorage = const_cast<LocalRandomSampleOperatorState*>(localState)->sampleStorage;

    auto entryMemRef = sampleStorage.allocateEntry();
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (const auto& field : inputSchema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

void RandomSampleWithoutReplacement::getApproximateRecord(const uint64_t handlerIndex,
                                                          Runtime::Execution::ExecutionContext& ctx,
                                                          const Nautilus::Value<>& key,
                                                          Nautilus::Record& outputRecord) {
    using namespace Runtime::Execution;

    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    auto pagedVectorMemRef = Nautilus::FunctionCall("getPagedVectorRefProxy", getPagedVectorRefProxy, opHandlerMemRef);
    auto pagedVectorRef = Nautilus::Interface::PagedVectorRef(pagedVectorMemRef, entrySize, pageSize);

    // The buffer size does not matter to us, as the record always lie in a row
    auto memoryProviderInput = MemoryProvider::MemoryProvider::createMemoryProvider(inputSchema->getSchemaSizeInBytes(),
                                                                                    inputSchema);

    // We use the pagedVector to create a new memref for storing the aggregation value
    if (!firstRunGetApproximate) {
        firstRunGetApproximate = true;
        Nautilus::FunctionCall("createSampleProxy", createSampleProxy, pagedVectorMemRef,
                                                              Nautilus::Value<Nautilus::UInt64>((uint64_t)sampleSize));

        pagedVectorRef.allocateEntry();
    }

    auto numberOfTuplesInWindow = pagedVectorRef.getTotalNumberOfEntries() - 1;
    auto numberOfRecordsInSample = numberOfTuplesInWindow.as<Nautilus::UInt64>();
    if (sampleSize < numberOfRecordsInSample) {
        numberOfRecordsInSample = Nautilus::Value<>(sampleSize).as<Nautilus::UInt64>();
    }


    // Iterating over the sample and creating the aggregation for all keys
    Nautilus::Value<Nautilus::MemRef> aggregationValueMemRef = pagedVectorRef.getEntry(numberOfTuplesInWindow.as<Nautilus::UInt64>());
    aggregationFunction->reset(aggregationValueMemRef);
    Nautilus::Value<Nautilus::UInt64> zeroValue((uint64_t) 0);
    for (auto it = pagedVectorRef.begin(); it != pagedVectorRef.at(numberOfRecordsInSample); ++it) {
        auto entryMemRef = *it;
        auto tmpRecord = memoryProviderInput->read({}, entryMemRef, zeroValue);
        auto readKey = readKeyExpression->execute(tmpRecord);
        if (readKey == key) {
            aggregationFunction->lift(aggregationValueMemRef, tmpRecord);
        }
    }

    // Lower the aggregation
    aggregationFunction->lower(aggregationValueMemRef, outputRecord);
    auto scalingFactor = getScalingFactor(numberOfTuplesInWindow);
    auto approximatedValue = outputRecord.read(fieldNameApproximate) * scalingFactor;
    NES_DEBUG2("approximatedValue {}", approximatedValue.getValue().toString());

    // Writing the approximate to the record as well as the key
    outputRecord.write(fieldNameApproximate, approximatedValue);
    outputRecord.write(fieldNameKey, key);
}

void RandomSampleWithoutReplacement::setup(const uint64_t handlerIndex, Runtime::Execution::ExecutionContext& ctx) {
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    Nautilus::FunctionCall("setupSampleOpHandlerProxy", setupSampleOpHandlerProxy, opHandlerMemRef,
                           Nautilus::Value<Nautilus::UInt64>(entrySize),
                           Nautilus::Value<Nautilus::UInt64>(pageSize));
}

Nautilus::Value<> RandomSampleWithoutReplacement::getScalingFactor(Nautilus::Value<>& numberOfTuplesInWindow){
    Nautilus::Value<Nautilus::Double> retValue((double_t)1);

    if ((aggregationType == Parsing::Aggregation_Type::COUNT) || (aggregationType == Parsing::Aggregation_Type::SUM)) {
        // As we use one entry in the PagedVector for calculating the approximate, we have to subtract it here
        Nautilus::Value<Nautilus::Double> minSize((double_t)sampleSize);
        retValue = (numberOfTuplesInWindow / minSize);
    }

    NES_DEBUG2("Scaling factor is {}", retValue.getValue().toString());
    if (retValue == 1.0) {
        return Nautilus::Value<Nautilus::UInt64>((uint64_t) 1);
    } else {
        return retValue;
    };
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

bool RandomSampleWithoutReplacement::storeLocalOperatorState(const uint64_t handlerIndex,
                                                             const Runtime::Execution::Operators::Operator* op,
                                                             Runtime::Execution::ExecutionContext& ctx) {

    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    auto pagedVectorMemRef = Nautilus::FunctionCall("getPagedVectorRefProxy", getPagedVectorRefProxy, opHandlerMemRef);
    auto pagedVectorRef = Nautilus::Interface::PagedVectorRef(pagedVectorMemRef, entrySize, pageSize);

    ctx.setLocalOperatorState(op, std::make_unique<LocalRandomSampleOperatorState>(pagedVectorRef));
    return true;
}
} // namespace NES::ASP