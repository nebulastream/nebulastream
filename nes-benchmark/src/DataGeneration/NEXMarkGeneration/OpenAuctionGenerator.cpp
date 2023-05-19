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

#include <DataGeneration/NEXMarkGeneration/OpenAuctionGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

OpenAuctionGenerator::OpenAuctionGenerator(uint64_t numberOfRecords)
    : DataGenerator(), numberOfRecords(numberOfRecords), dependencyGeneratorInstance(DependencyGenerator::getInstance(numberOfRecords)) {}

std::vector<Runtime::TupleBuffer> OpenAuctionGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> createdBuffers;
    createdBuffers.reserve(numberOfBuffers);

    auto memoryLayout = this->getMemoryLayout(bufferSize);

    std::random_device rndDevice;
    std::mt19937 generator(rndDevice());
    std::uniform_int_distribution<uint64_t> uniformReserveDistribution(1000, 2000);
    std::uniform_int_distribution<uint64_t> uniformBooleanDistribution(0, 1);
    std::uniform_int_distribution<uint16_t> uniformCategoryDistribution(0, 302);
    std::uniform_int_distribution<uint8_t> uniformQuantityDistribution(1, 9);

    auto auctions = dependencyGeneratorInstance.getAuctions();
    auto processedAuctions = 0UL;

    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

        // TODO add designated branch for RowLayout to make it faster (cmp. DefaultDataGenerator.cpp)
        for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
            // TODO what if numberOfRecords is smaller than the number of records we can fit in the buffers? Timestamp would be reset
            // TODO what if numberOfRecords is larger than the number of records we can fit in the buffers? We might get bids that reference auction id's or bidder id's that haven't been created yet
            auto auctionsIndex = processedAuctions++ % auctions.size();

            auto reserve = 0UL;
            if (uniformBooleanDistribution(generator)) reserve = (uint64_t) round(std::get<1>(auctions[auctionsIndex]) * (1.2 + uniformReserveDistribution(generator) / 1000.0));
            auto privacy = false;
            if (uniformBooleanDistribution(generator)) privacy = true;
            auto category = uniformCategoryDistribution(generator);
            auto quantity = uniformQuantityDistribution(generator);
            std::ostringstream oss;
            if (uniformBooleanDistribution(generator)) oss << "Regular";
            else oss << "Featured";
            if (quantity > 1 && uniformBooleanDistribution(generator)) oss << ", Dutch";
            auto type = oss.str();

            dynamicBuffer[curRecord]["id"].write<uint64_t>(processedAuctions);
            dynamicBuffer[curRecord]["reserve"].write<uint64_t>(reserve);
            dynamicBuffer[curRecord]["privacy"].write<bool>(privacy);
            dynamicBuffer[curRecord]["sellerId"].write<uint64_t>(std::get<0>(auctions[auctionsIndex]));
            dynamicBuffer[curRecord]["category"].write<uint16_t>(category);
            dynamicBuffer[curRecord]["quantity"].write<uint8_t>(quantity);
            dynamicBuffer[curRecord]["type"].write<std::string>(type);
            dynamicBuffer[curRecord]["startTime"].write<uint64_t>(std::get<2>(auctions[auctionsIndex]));
            dynamicBuffer[curRecord]["endTime"].write<uint64_t>(std::get<3>(auctions[auctionsIndex]));
        }

        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    return createdBuffers;
}

SchemaPtr OpenAuctionGenerator::getSchema() {
    return Schema::create()
        ->addField(createField("id", BasicType::UINT64))
        ->addField(createField("reserve", BasicType::UINT64))
        ->addField(createField("privacy", BasicType::BOOLEAN))
        ->addField(createField("sellerId", BasicType::UINT64))
        ->addField(createField("category", BasicType::UINT16))
        ->addField(createField("quantity", BasicType::UINT8))
        ->addField(createField("type", BasicType::TEXT))
        ->addField(createField("startTime", BasicType::UINT64))
        ->addField(createField("endTime", BasicType::UINT64));
}

std::string OpenAuctionGenerator::getName() { return "NEXMarkOpenAuction"; }

std::string OpenAuctionGenerator::toString() {
    std::ostringstream oss;

    oss << getName() << " (" << numberOfRecords << ")";

    return oss.str();
}

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
