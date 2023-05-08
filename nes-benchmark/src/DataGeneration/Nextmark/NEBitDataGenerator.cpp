#include <API/Schema.hpp>
#include <DataGeneration/Nextmark/NEBitDataGenerator.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <DataGeneration/Nextmark/NexmarkCommon.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>

#include <fstream>
#include <math.h>
#include <algorithm>
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
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < dynamicBuffer.getCapacity(); currentRecord++) {
            long auction, bidder;

            long epoch = currentRecord / NexmarkCommon::TOTAL_EVENT_RATIO;

            if (rand() % 100 > NexmarkCommon::HOT_AUCTIONS_PROB) {
                auction = (((epoch * NexmarkCommon::AUCTION_EVENT_RATIO + NexmarkCommon::AUCTION_EVENT_RATIO - 1) / NexmarkCommon::HOT_AUCTION_RATIO) * NexmarkCommon::HOT_AUCTION_RATIO);
            } else {
                long a = std::max(0L, epoch * NexmarkCommon::AUCTION_EVENT_RATIO + NexmarkCommon::AUCTION_EVENT_RATIO - 1 - 20000);
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

            dynamicBuffer[currentRecord]["auctionId"].write<uint64_t>(auction);
            dynamicBuffer[currentRecord]["bidderId"].write<uint64_t>(bidder);
            dynamicBuffer[currentRecord]["price"].write<double>(0.1);
        }
        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
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

std::string NEBitDataGenerator::toString() {
    std::ostringstream oss;
    oss << getName();
    return oss.str();
}

}// namespace NES