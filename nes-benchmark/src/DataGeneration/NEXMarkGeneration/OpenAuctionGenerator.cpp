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
#include <DataGeneration/NEXMarkGeneration/UniformIntDistributions.hpp>
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
            dynamicBuffer.pushRecordToBuffer(record);
        }

        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    return createdBuffers;
}

OpenAuctionRecord OpenAuctionGenerator::generateOpenAuctionRecord(std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>& auctions,
                                                                  uint64_t auctionsIndex, Runtime::MemoryLayouts::DynamicTupleBuffer dynamicBuffer) {
    static auto uniformIntDistributions = UniformIntDistributions();

    // create random reserve
    auto reserve = 0UL;
    if (uniformIntDistributions.generateRandomBoolean()) {
        reserve = (uint64_t) round(std::get<1>(auctions[auctionsIndex]) * (1.2 + uniformIntDistributions.generateRandomReserve() / 1000.0));
    }

    // create random privacy
    auto privacy = false;
    if (uniformIntDistributions.generateRandomBoolean()) {
        privacy = true;
    }

    // create random category and quantity
    auto category = uniformIntDistributions.generateRandomCategory();
    auto quantity = uniformIntDistributions.generateRandomQuantity();

    // create random type
    std::ostringstream oss;
    if (uniformIntDistributions.generateRandomBoolean()) {
        oss << "Regular";
    }
    else {
        oss << "Featured";
    }
    if (quantity > 1 && uniformIntDistributions.generateRandomBoolean()) {
        oss << "; Dutch";
    }
    auto type = Util::writeStringToTupleBuffer(dynamicBuffer.getBuffer(), allocateBuffer(), oss.str());

    // get auction dependencies
    auto sellerId = std::get<0>(auctions[auctionsIndex]);
    auto startTime = std::get<2>(auctions[auctionsIndex]);
    auto endTime = std::get<3>(auctions[auctionsIndex]);

    return std::make_tuple(auctionsIndex, reserve, privacy, sellerId, category, quantity, type, startTime, endTime);
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
