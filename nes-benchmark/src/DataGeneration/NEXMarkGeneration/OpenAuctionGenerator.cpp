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

OpenAuctionGenerator::OpenAuctionGenerator()
    : DataGenerator() {}

std::vector<Runtime::TupleBuffer> OpenAuctionGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::random_device rndDevice;
    std::mt19937 generator(rndDevice());
    std::uniform_int_distribution<uint64_t> uniformReserveDistribution(1000, 2000);
    std::uniform_int_distribution<uint64_t> uniformBooleanDistribution(0, 1);
    std::uniform_int_distribution<uint16_t> uniformCategoryDistribution(0, 302);
    std::uniform_int_distribution<uint8_t> uniformQuantityDistribution(1, 9);

    auto& dependencyGeneratorInstance = DependencyGenerator::getInstance(numberOfBuffers, bufferSize);
    auto auctions = dependencyGeneratorInstance.getAuctions();
    auto numberOfAuctions = auctions.size();
    auto numberOfRecords = dependencyGeneratorInstance.getNumberOfRecords();
    auto auctionsToProcess = numberOfRecords < numberOfAuctions ? numberOfRecords : numberOfAuctions;

    std::vector<Runtime::TupleBuffer> createdBuffers;
    uint64_t numberOfBuffersToCreate = 1 + auctionsToProcess * getSchema()->getSchemaSizeInBytes() / bufferSize;
    createdBuffers.reserve(numberOfBuffersToCreate);

    auto memoryLayout = this->getMemoryLayout(bufferSize);
    auto processedAuctions = 0UL;

    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffersToCreate; ++curBuffer) {
        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

        // TODO add designated branch for RowLayout to make it faster (cmp. DefaultDataGenerator.cpp)
        for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity() && processedAuctions < auctionsToProcess; ++curRecord) {
            auto auctionsIndex = processedAuctions++;

            // create random data
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

            // write type string to childBuffer in order to store it in TupleBuffer
            auto childTupleBuffer = allocateBuffer();
            auto sizeOfInputField = type.size();
            (*childTupleBuffer.getBuffer<uint32_t>()) = sizeOfInputField;
            std::memcpy(childTupleBuffer.getBuffer() + sizeof(uint32_t), type.c_str(), sizeOfInputField);
            auto childIdx = dynamicBuffer.getBuffer().storeChildBuffer(childTupleBuffer);

            dynamicBuffer[curRecord]["id"].write<uint64_t>(processedAuctions);
            dynamicBuffer[curRecord]["reserve"].write<uint64_t>(reserve);
            dynamicBuffer[curRecord]["privacy"].write<bool>(privacy);
            dynamicBuffer[curRecord]["sellerId"].write<uint64_t>(std::get<0>(auctions[auctionsIndex]));
            dynamicBuffer[curRecord]["category"].write<uint16_t>(category);
            dynamicBuffer[curRecord]["quantity"].write<uint8_t>(quantity);
            dynamicBuffer[curRecord]["type"].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx);
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

    oss << getName() << "()";

    return oss.str();
}

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
