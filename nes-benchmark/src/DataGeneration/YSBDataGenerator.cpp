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
#include <Configurations/Coordinator/SchemaType.hpp>
#include <DataGeneration/YSBDataGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>

namespace NES::Benchmark::DataGeneration {

std::string YSBDataGenerator::getName() { return "YSB"; }

std::vector<Runtime::TupleBuffer> YSBDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    NES_INFO("YSB Mode source mode");

    std::vector<Runtime::TupleBuffer> createdBuffers;
    createdBuffers.reserve(numberOfBuffers);

    auto memoryLayout = getMemoryLayout(bufferSize);
    auto ts = 0UL;
    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < dynamicBuffer.getCapacity(); currentRecord++, ts++) {
            auto campaign_id = rand() % 1000;
            auto event_type = currentRecord % 3;
            dynamicBuffer[currentRecord]["user_id"].write<uint64_t>(1);
            dynamicBuffer[currentRecord]["page_id"].write<uint64_t>(0);
            dynamicBuffer[currentRecord]["campaign_id"].write<uint64_t>(campaign_id);
            dynamicBuffer[currentRecord]["ad_type"].write<uint64_t>(0);
            dynamicBuffer[currentRecord]["event_type"].write<uint64_t>(event_type);
            dynamicBuffer[currentRecord]["current_ms"].write<uint64_t>(ts);
            dynamicBuffer[currentRecord]["ip"].write<uint64_t>(0x01020304);
            dynamicBuffer[currentRecord]["d1"].write<uint64_t>(1);
            dynamicBuffer[currentRecord]["d2"].write<uint64_t>(1);
            dynamicBuffer[currentRecord]["d3"].write<uint32_t>(1);
            dynamicBuffer[currentRecord]["d4"].write<uint16_t>(1);
        }
        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(buffer);
    }
    NES_INFO("Created all buffers!");
    return createdBuffers;
}

std::string YSBDataGenerator::toString() {
    std::ostringstream oss;

    oss << getName();
    return oss.str();
}

SchemaPtr YSBDataGenerator::getSchema() {
    return Schema::create()
        ->addField("user_id", BasicType::UINT64)
        ->addField("page_id", BasicType::UINT64)
        ->addField("campaign_id", BasicType::UINT64)
        ->addField("ad_type", BasicType::UINT64)
        ->addField("event_type", BasicType::UINT64)
        ->addField("current_ms", BasicType::UINT64)
        ->addField("ip", BasicType::UINT64)
        ->addField("d1", BasicType::UINT64)
        ->addField("d2", BasicType::UINT64)
        ->addField("d3", BasicType::UINT32)
        ->addField("d4", BasicType::UINT16);
}

Configurations::SchemaTypePtr YSBDataGenerator::getSchemaType() {
    const char* length = "0";
    const char* dataType = "UINT64";
    std::vector<Configurations::SchemaFieldDetail> schemaFiledDetails;
    schemaFiledDetails.emplace_back("user_id", dataType, length);
    schemaFiledDetails.emplace_back("page_id", dataType, length);
    schemaFiledDetails.emplace_back("campaign_id", dataType, length);
    schemaFiledDetails.emplace_back("ad_type", dataType, length);
    schemaFiledDetails.emplace_back("event_type", dataType, length);
    schemaFiledDetails.emplace_back("current_ms", dataType, length);
    schemaFiledDetails.emplace_back("ip", dataType, length);
    schemaFiledDetails.emplace_back("d1", dataType, length);
    schemaFiledDetails.emplace_back("d2", dataType, length);
    schemaFiledDetails.emplace_back("d3", dataType, length);
    schemaFiledDetails.emplace_back("d4", dataType, length);
    return Configurations::SchemaType::create(schemaFiledDetails);
}

}// namespace NES::Benchmark::DataGeneration