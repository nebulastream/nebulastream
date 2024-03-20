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
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
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
        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < testBuffer.getCapacity(); currentRecord++, ts++) {
            auto campaign_id = rand() % 1000;
            auto event_type = currentRecord % 3;
            testBuffer[currentRecord]["user_id"].write<uint64_t>(1);
            testBuffer[currentRecord]["page_id"].write<uint64_t>(0);
            testBuffer[currentRecord]["campaign_id"].write<uint64_t>(campaign_id);
            testBuffer[currentRecord]["ad_type"].write<uint64_t>(0);
            testBuffer[currentRecord]["event_type"].write<uint64_t>(event_type);
            testBuffer[currentRecord]["current_ms"].write<uint64_t>(ts);
            testBuffer[currentRecord]["ip"].write<uint64_t>(0x01020304);
            testBuffer[currentRecord]["d1"].write<uint64_t>(1);
            testBuffer[currentRecord]["d2"].write<uint64_t>(1);
            testBuffer[currentRecord]["d3"].write<uint32_t>(1);
            testBuffer[currentRecord]["d4"].write<uint16_t>(1);
        }
        testBuffer.setNumberOfTuples(testBuffer.getCapacity());
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
    const char* dataType = "UINT64";
    std::vector<Configurations::SchemaFieldDetail> schemaFieldDetails;
    schemaFieldDetails.emplace_back("user_id", dataType);
    schemaFieldDetails.emplace_back("page_id", dataType);
    schemaFieldDetails.emplace_back("campaign_id", dataType);
    schemaFieldDetails.emplace_back("ad_type", dataType);
    schemaFieldDetails.emplace_back("event_type", dataType);
    schemaFieldDetails.emplace_back("current_ms", dataType);
    schemaFieldDetails.emplace_back("ip", dataType);
    schemaFieldDetails.emplace_back("d1", dataType);
    schemaFieldDetails.emplace_back("d2", dataType);
    schemaFieldDetails.emplace_back("d3", dataType);
    schemaFieldDetails.emplace_back("d4", dataType);
    return Configurations::SchemaType::create(schemaFieldDetails);
}

}// namespace NES::Benchmark::DataGeneration