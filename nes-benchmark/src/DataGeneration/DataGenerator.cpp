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
#include <API/Schema.hpp>
#include <DataGeneration/DataGenerator.hpp>
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <DataGeneration/LightSaber/ClusterMonitoringDataGenerator.hpp>
#include <DataGeneration/LightSaber/LinarRoadDataGenerator.hpp>
#include <DataGeneration/LightSaber/ManufacturingEquipmentDataGenerator.hpp>
#include <DataGeneration/LightSaber/SmartGridDataGenerator.hpp>
#include <DataGeneration/Nextmark/NEAuctionDataGenerator.hpp>
#include <DataGeneration/Nextmark/NEBitDataGenerator.hpp>
#include <DataGeneration/YSBDataGenerator.hpp>
#include <DataGeneration/ZipfianDataGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Benchmark::DataGeneration {

Runtime::MemoryLayouts::MemoryLayoutPtr DataGenerator::getMemoryLayout(size_t bufferSize) {

    auto schema = this->getSchema();
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        return Runtime::MemoryLayouts::RowLayout::create(schema, bufferSize);
    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        return Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferSize);
    }

    return nullptr;
}

NES::Runtime::TupleBuffer DataGenerator::allocateBuffer() { return bufferManager->getBufferBlocking(); }

DataGeneratorPtr DataGenerator::createGeneratorByName(std::string type, Yaml::Node generatorNode) {

    NES_INFO("DataGenerator created from type: {}", type)

    if (type.empty() || type == "Default") {
        return std::make_unique<DefaultDataGenerator>(/* minValue */ 0, /* maxValue */ 1000);
    } else if (type == "Uniform") {
        if (generatorNode["minValue"].IsNone() || generatorNode["maxValue"].IsNone()) {
            NES_THROW_RUNTIME_ERROR("Alpha, minValue and maxValue are necessary for a Uniform Datagenerator!");
        }

        auto minValue = generatorNode["minValue"].As<uint64_t>();
        auto maxValue = generatorNode["maxValue"].As<uint64_t>();
        return std::make_unique<DefaultDataGenerator>(minValue, maxValue);
    } else if (type == "NEBit") {
        return std::make_unique<NEBitDataGenerator>();
    } else if (type == "NEAuction") {
        return std::make_unique<NEAuctionDataGenerator>();
    } else if (type == "Zipfian") {
        if (generatorNode["alpha"].IsNone() || generatorNode["minValue"].IsNone() || generatorNode["maxValue"].IsNone()) {
            NES_THROW_RUNTIME_ERROR("Alpha, minValue and maxValue are necessary for a Zipfian Datagenerator!");
        }

        auto alpha = generatorNode["alpha"].As<double>();
        auto minValue = generatorNode["minValue"].As<uint64_t>();
        auto maxValue = generatorNode["maxValue"].As<uint64_t>();
        return std::make_unique<ZipfianDataGenerator>(alpha, minValue, maxValue);

    } else if (NES::Util::toUpperCase(type) == "YSB" || type == "YSBKafka") {
        return std::make_unique<YSBDataGenerator>();
    } else if (type == "SmartGrid") {
        return std::make_unique<SmartGridDataGenerator>();
    } else if (type == "LinearRoad") {
        return std::make_unique<LinearRoadDataGenerator>();
    } else if (type == "ClusterMonitoring") {
        return std::make_unique<ClusterMonitoringDataGenerator>();
    } else if (type == "ManufacturingEquipment") {
        return std::make_unique<ManufacturingEquipmentDataGenerator>();
    } else {
        NES_THROW_RUNTIME_ERROR("DataGenerator " << type << " could not been parsed!");
    }
}

void DataGenerator::setBufferManager(Runtime::BufferManagerPtr newBufferManager) {
    DataGenerator::bufferManager = newBufferManager;
}
void DataGenerator::insertTimestamps(std::vector<NES::Runtime::TupleBuffer>& buffers,
                                     size_t bufferSize,
                                     size_t startingTimestamp,
                                     size_t stepSize,
                                     uint8_t stepChanceInPercent,
                                     bool allowPartialSteps) {
    if (!hasTimestampField()) {
        NES_WARNING("{} does not support timestamps", getName());
        return;
    }

    NES_ASSERT(stepChanceInPercent <= 100, "Step chance required to be less or equal to 100");

    size_t currentTimestamp = startingTimestamp;

    for (auto& tupleBuffer : buffers) {
        Runtime::MemoryLayouts::DynamicTupleBuffer buffer(getMemoryLayout(bufferSize), tupleBuffer);
        for (auto tuple : buffer) {
            getTimestampField(tuple).write<uint64_t>(currentTimestamp);

            if (stepChanceInPercent == 100) {
                currentTimestamp += stepSize;
            }

            auto roll = rand() % 100;
            if (allowPartialSteps) {
                currentTimestamp += static_cast<size_t>(static_cast<float>(roll) / 100.0f * static_cast<float>(stepSize));
            } else if (roll >= stepChanceInPercent) {
                currentTimestamp += stepSize;
            }
        }
    }
}
}// namespace NES::Benchmark::DataGeneration