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
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <DataGeneration/YSBDataGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <random>

namespace NES::Benchmark::DataGeneration {

using YSBTuple = std::tuple<uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t,
                            uint32_t, uint16_t>;

std::string YSBDataGenerator::getName() { return "YSB"; }

std::vector<Runtime::TupleBuffer> YSBDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    NES_INFO("YSB Mode source mode");

    std::vector<Runtime::TupleBuffer> createdBuffers;
    createdBuffers.reserve(numberOfBuffers);

    auto memoryLayout = getMemoryLayout(bufferSize);
    auto ts = 0UL;

    // using seed to generate a predictable sequence of values for deterministic behavior
    std::mt19937 generator(GENERATOR_SEED_DEFAULT);
    std::uniform_int_distribution<uint64_t> uniformIntDistribution(0, 1000);

    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer();

        if (memoryLayout->getSchema()->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
            auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(memoryLayout->getSchema(), bufferSize);
            auto rowLayoutDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, buffer);
            for (uint64_t currentRecord = 0; currentRecord < rowLayoutDynamicBuffer.getCapacity(); ++currentRecord, ++ts) {
                auto campaign_id = uniformIntDistribution(generator);
                auto event_type = currentRecord % 3;
                rowLayoutDynamicBuffer.pushRecordToBuffer(YSBTuple(1, 0, campaign_id, 0, event_type, ts,
                                                                   0x01020304, 1, 1, 1, 1));
            }
            rowLayoutDynamicBuffer.setNumberOfTuples(rowLayoutDynamicBuffer.getCapacity());
        } else {
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t currentRecord = 0; currentRecord < dynamicBuffer.getCapacity(); currentRecord++, ts++) {
                auto campaign_id = uniformIntDistribution(generator);
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
        }
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

}// namespace NES::Benchmark::DataGeneration