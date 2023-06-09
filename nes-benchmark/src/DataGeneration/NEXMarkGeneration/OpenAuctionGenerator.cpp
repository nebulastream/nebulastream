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
#include <Util/Core.hpp>
#include <cmath>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

std::vector<Runtime::TupleBuffer> OpenAuctionGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    auto& dependencyGeneratorInstance = DependencyGenerator::getInstance(numberOfBuffers, bufferSize);
    auto auctions = dependencyGeneratorInstance.getAuctions();
    auto numberOfAuctions = auctions.size();
    auto numberOfRecords = dependencyGeneratorInstance.getNumberOfRecords();
    auto auctionsToProcess = (recordsInit + numberOfRecords) < numberOfAuctions ? (recordsInit + numberOfRecords) : numberOfAuctions;

    std::vector<Runtime::TupleBuffer> createdBuffers;
    uint64_t numberOfBuffersToCreate = std::ceil(auctionsToProcess * getSchema()->getSchemaSizeInBytes() / (bufferSize * 1.0));
    createdBuffers.reserve(numberOfBuffersToCreate);
    NES_INFO("auctionsToProcess: " << auctionsToProcess << "\tnumberOfAuctionsBuffersToCreate: " << numberOfBuffersToCreate);

    auto memoryLayout = this->getMemoryLayout(bufferSize);
    auto processedAuctions = 0UL;

    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffersToCreate; ++curBuffer) {
        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

        for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity() && processedAuctions < auctionsToProcess; ++curRecord) {
            auto auctionsIndex = processedAuctions++;
            auto record = generateOpenAuctionRecord(auctions, auctionsIndex, dynamicBuffer);

            dynamicBuffer[curRecord]["id"].write<uint64_t>(auctionsIndex);
            dynamicBuffer[curRecord]["reserve"].write<uint64_t>(record.reserve);
            dynamicBuffer[curRecord]["privacy"].write<bool>(record.privacy);
            dynamicBuffer[curRecord]["sellerId"].write<uint64_t>(record.sellerId);
            dynamicBuffer[curRecord]["category"].write<uint16_t>(record.category);
            dynamicBuffer[curRecord]["quantity"].write<uint8_t>(record.quantity);
            dynamicBuffer[curRecord]["type"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(record.type);
            dynamicBuffer[curRecord]["startTime"].write<uint64_t>(record.startTime);
            dynamicBuffer[curRecord]["endTime"].write<uint64_t>(record.endTime);
        }

        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    return createdBuffers;
}

OpenAuctionRecord OpenAuctionGenerator::generateOpenAuctionRecord(std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>& auctions,
                                                                  uint64_t auctionsIndex, Runtime::MemoryLayouts::DynamicTupleBuffer dynamicBuffer) {
    static std::mt19937 generator(42);
    static std::uniform_int_distribution<uint16_t> uniformReserveDistribution(1000, 2000);
    static std::uniform_int_distribution<uint8_t> uniformBooleanDistribution(0, 1);
    static std::uniform_int_distribution<uint16_t> uniformCategoryDistribution(0, 302);
    static std::uniform_int_distribution<uint8_t> uniformQuantityDistribution(1, 10);

    OpenAuctionRecord record;

    // create random data
    record.reserve = 0UL;
    if (uniformBooleanDistribution(generator)) {
        record.reserve = (uint64_t) round(std::get<1>(auctions[auctionsIndex]) * (1.2 + uniformReserveDistribution(generator) / 1000.0));
    }
    record.privacy = false;
    if (uniformBooleanDistribution(generator)) {
        record.privacy = true;
    }
    record.category = uniformCategoryDistribution(generator);
    record.quantity = uniformQuantityDistribution(generator);

    std::ostringstream oss;
    if (uniformBooleanDistribution(generator)) {
        oss << "Regular";
    }
    else {
        oss << "Featured";
    }
    if (record.quantity > 1 && uniformBooleanDistribution(generator)) {
        oss << ", Dutch";
    }
    // write type string to childBuffer in order to store it in TupleBuffer
    record.type = Util::writeStringToTupleBuffer(dynamicBuffer.getBuffer(), allocateBuffer(), oss.str());

    // write auction dependencies to record
    record.sellerId = std::get<0>(auctions[auctionsIndex]);
    record.startTime = std::get<2>(auctions[auctionsIndex]);
    record.endTime = std::get<3>(auctions[auctionsIndex]);

    return record;
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

    oss << getName() << "()";

    return oss.str();
}

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
