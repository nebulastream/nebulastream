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
#include <DataGeneration/Nextmark/NEBitDataGenerator.hpp>
#include <DataGeneration/Nextmark/NexmarkCommon.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Util/TestTupleBuffer.hpp>

#include <Configurations/Coordinator/SchemaType.hpp>
#include <algorithm>
#include <fstream>
#include <math.h>
#include <utility>

namespace NES::Benchmark::DataGeneration {

NEBitDataGenerator::NEBitDataGenerator() : DataGenerator() {}

std::string NEBitDataGenerator::getName() { return "NEBit"; }

std::vector<Runtime::TupleBuffer> NEBitDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> buffers;
    buffers.reserve(numberOfBuffers);

    auto memoryLayout = getMemoryLayout(bufferSize);

    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer();
        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < testBuffer.getCapacity(); currentRecord++) {
            long auction, bidder;

            long epoch = currentRecord / NexmarkCommon::TOTAL_EVENT_RATIO;

            if (rand() % 100 > NexmarkCommon::HOT_AUCTIONS_PROB) {
                auction = (((epoch * NexmarkCommon::AUCTION_EVENT_RATIO + NexmarkCommon::AUCTION_EVENT_RATIO - 1)
                            / NexmarkCommon::HOT_AUCTION_RATIO)
                           * NexmarkCommon::HOT_AUCTION_RATIO);
            } else {
                long a =
                    std::max(0L, epoch * NexmarkCommon::AUCTION_EVENT_RATIO + NexmarkCommon::AUCTION_EVENT_RATIO - 1 - 20000);
                long b = epoch * NexmarkCommon::AUCTION_EVENT_RATIO + NexmarkCommon::AUCTION_EVENT_RATIO - 1;
                auction = a + rand() % (b - a + 1 + 100);
            }

            if (rand() % 100 > 85) {
                long personId = epoch * NexmarkCommon::PERSON_EVENT_RATIO + NexmarkCommon::PERSON_EVENT_RATIO - 1;
                bidder = (personId / NexmarkCommon::HOT_SELLER_RATIO) * NexmarkCommon::HOT_SELLER_RATIO;
            } else {
                long personId = epoch * NexmarkCommon::PERSON_EVENT_RATIO + NexmarkCommon::PERSON_EVENT_RATIO - 1;
                long activePersons = std::min(personId, 60000L);
                long n = rand() % (activePersons + 100);
                bidder = personId + activePersons - n;
            }

            testBuffer[currentRecord]["auctionId"].write<uint64_t>(auction);
            testBuffer[currentRecord]["bidderId"].write<uint64_t>(bidder);
            testBuffer[currentRecord]["price"].write<double>(0.1);
        }
        testBuffer.setNumberOfTuples(testBuffer.getCapacity());
        buffers.emplace_back(buffer);
    }
    return buffers;
}
SchemaPtr NEBitDataGenerator::getSchema() {
    return Schema::create()
        ->addField("creationTS", BasicType::UINT64)
        ->addField("timestamp", BasicType::UINT64)
        ->addField("auctionId", BasicType::UINT64)
        ->addField("bidderId", BasicType::UINT64)
        ->addField("price", BasicType::FLOAT64);
}

Configurations::SchemaTypePtr NEBitDataGenerator::getSchemaType() {
    const char* dataTypeUI64 = "UINT64";
    const char* dataTypeF64 = "FLOAT64";
    std::vector<Configurations::SchemaFieldDetail> schemaFieldDetails;
    schemaFieldDetails.emplace_back("creationTS", dataTypeUI64);
    schemaFieldDetails.emplace_back("timestamp", dataTypeUI64);
    schemaFieldDetails.emplace_back("auctionId", dataTypeUI64);
    schemaFieldDetails.emplace_back("bidderId", dataTypeUI64);
    schemaFieldDetails.emplace_back("price", dataTypeF64);
    return Configurations::SchemaType::create(schemaFieldDetails);
}

std::string NEBitDataGenerator::toString() {
    std::ostringstream oss;
    oss << getName();
    return oss.str();
}

}// namespace NES::Benchmark::DataGeneration
