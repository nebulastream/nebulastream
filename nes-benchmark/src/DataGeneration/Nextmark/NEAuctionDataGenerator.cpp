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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <DataGeneration/Nextmark/NEAuctionDataGenerator.hpp>
#include <DataGeneration/Nextmark/NexmarkCommon.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <algorithm>
#include <fstream>
#include <math.h>
#include <utility>

namespace NES::Benchmark::DataGeneration {

NEAuctionDataGenerator::NEAuctionDataGenerator() : DataGenerator() {}

std::string NEAuctionDataGenerator::getName() { return "NEAuction"; }
std::vector<Runtime::TupleBuffer> NEAuctionDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> buffers;
    buffers.reserve(numberOfBuffers);

    auto memoryLayout = getMemoryLayout(bufferSize);

    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < dynamicBuffer.getCapacity(); currentRecord++) {
            long epoch = currentRecord / NexmarkCommon::TOTAL_EVENT_RATIO;
            long offset = currentRecord % NexmarkCommon::TOTAL_EVENT_RATIO;
            if (offset < NexmarkCommon::PERSON_EVENT_RATIO) {
                epoch--;
                offset = NexmarkCommon::AUCTION_EVENT_RATIO - 1;
            } else {
                offset = NexmarkCommon::AUCTION_EVENT_RATIO - 1;
            }
            long auctionId = epoch * NexmarkCommon::AUCTION_EVENT_RATIO + offset;//r.nextLong(minAuctionId, maxAuctionId);

            epoch = currentRecord / NexmarkCommon::TOTAL_EVENT_RATIO;
            offset = currentRecord % NexmarkCommon::TOTAL_EVENT_RATIO;

            if (offset >= NexmarkCommon::PERSON_EVENT_RATIO) {
                offset = NexmarkCommon::PERSON_EVENT_RATIO - 1;
            }
            long matchingPerson;
            if (rand() % 100 > 85) {
                long personId = epoch * NexmarkCommon::PERSON_EVENT_RATIO + offset;
                matchingPerson = (personId / NexmarkCommon::HOT_SELLER_RATIO) * NexmarkCommon::HOT_SELLER_RATIO;
            } else {
                long personId = epoch * NexmarkCommon::PERSON_EVENT_RATIO + offset + 1;
                long activePersons = std::min(personId, 20000L);
                long n = rand() % (activePersons + 100);
                matchingPerson = personId + activePersons - n;
            }
            dynamicBuffer[currentRecord]["id"].write<uint64_t>(auctionId);
            dynamicBuffer[currentRecord]["seller"].write<uint64_t>(matchingPerson);
        }
        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        buffers.emplace_back(buffer);
    }
    return buffers;
}
SchemaPtr NEAuctionDataGenerator::getSchema() {
    return Schema::create()
        ->addField("id", BasicType::UINT64)
        ->addField("itemName", BasicType::UINT64)
        ->addField("itemNamePad", BasicType::UINT64)
        ->addField("description", BasicType::UINT64)
        ->addField("descriptionPad", BasicType::UINT64)
        ->addField("initialBit", BasicType::UINT64)
        ->addField("reserve", BasicType::UINT64)
        ->addField("dateTime", BasicType::UINT64)
        ->addField("seller", BasicType::UINT64)
        ->addField("expires", BasicType::UINT64)
        ->addField("category", BasicType::UINT64);
}

std::string NEAuctionDataGenerator::toString() {
    std::ostringstream oss;
    oss << getName();
    return oss.str();
}
Configurations::SchemaTypePtr NEAuctionDataGenerator::getSchemaType() {
    const char* dataTypeUI64 = "UINT64";
    std::vector<Configurations::SchemaFieldDetail> schemaFieldDetails;
    schemaFieldDetails.emplace_back("id", dataTypeUI64);
    schemaFieldDetails.emplace_back("itemName", dataTypeUI64);
    schemaFieldDetails.emplace_back("itemNamePad", dataTypeUI64);
    schemaFieldDetails.emplace_back("description", dataTypeUI64);
    schemaFieldDetails.emplace_back("descriptionPad", dataTypeUI64);
    schemaFieldDetails.emplace_back("initialBit", dataTypeUI64);
    schemaFieldDetails.emplace_back("reserve", dataTypeUI64);
    schemaFieldDetails.emplace_back("dateTime", dataTypeUI64);
    schemaFieldDetails.emplace_back("seller", dataTypeUI64);
    schemaFieldDetails.emplace_back("expires", dataTypeUI64);
    schemaFieldDetails.emplace_back("category", dataTypeUI64);
    return Configurations::SchemaType::create(schemaFieldDetails);
}

}// namespace NES::Benchmark::DataGeneration