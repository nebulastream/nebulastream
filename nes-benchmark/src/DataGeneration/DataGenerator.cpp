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
#include <DataGeneration/Nextmark/NEAuctionDataGenerator.hpp>
#include <DataGeneration/Nextmark/NEBitDataGenerator.hpp>
#include <DataGeneration/YSBDataGenerator.hpp>
#include <DataGeneration/ZipfianDataGenerator.hpp>
#include <DataGeneration/LightSaber/SmartGridDataGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
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
    } else {
        NES_THROW_RUNTIME_ERROR("DataGenerator " << type << " could not been parsed!");
    }
}

void DataGenerator::setBufferManager(Runtime::BufferManagerPtr newBufferManager) {
    DataGenerator::bufferManager = newBufferManager;
}
}// namespace NES::Benchmark::DataGeneration