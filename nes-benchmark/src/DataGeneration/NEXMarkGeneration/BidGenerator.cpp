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

#include <DataGeneration/NEXMarkGeneration/BidGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <cmath>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

std::vector<Runtime::TupleBuffer> BidGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    auto& dependencyGeneratorInstance = DependencyGenerator::getInstance(numberOfBuffers, bufferSize);
    auto bids = dependencyGeneratorInstance.getBids();
    auto numberOfBids = bids.size();
    auto numberOfRecords = dependencyGeneratorInstance.getNumberOfRecords();
    auto bidsToProcess = (numberOfRecords * 10) < numberOfBids ? (numberOfRecords * 10) : numberOfBids;

    std::vector<Runtime::TupleBuffer> createdBuffers;
    uint64_t numberOfBuffersToCreate = std::ceil(bidsToProcess * getSchema()->getSchemaSizeInBytes() / (bufferSize * 1.0));
    createdBuffers.reserve(numberOfBuffersToCreate);
    NES_INFO("bidsToProcess: " << bidsToProcess << "\tnumberOfBidsBuffersToCreate: " << numberOfBuffersToCreate);

    auto memoryLayout = this->getMemoryLayout(bufferSize);
    auto processedBids = 0UL;

    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffersToCreate; ++curBuffer) {
        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

        // TODO add designated branch for RowLayout to make it faster (cmp. DefaultDataGenerator.cpp)
        for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity() && processedBids < numberOfBids; ++curRecord) {
            auto bidsIndex = processedBids++;

            dynamicBuffer[curRecord]["auctionId"].write<uint64_t>(std::get<0>(bids[bidsIndex]));
            dynamicBuffer[curRecord]["bidderId"].write<uint64_t>(std::get<1>(bids[bidsIndex]));
            dynamicBuffer[curRecord]["price"].write<uint64_t>(std::get<2>(bids[bidsIndex]));
            dynamicBuffer[curRecord]["timestamp"].write<uint64_t>(std::get<3>(bids[bidsIndex]));
        }

        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    return createdBuffers;
}

SchemaPtr BidGenerator::getSchema() {
    return Schema::create()
        ->addField(createField("auctionId", BasicType::UINT64))
        ->addField(createField("bidderId", BasicType::UINT64))
        ->addField(createField("price", BasicType::UINT64))
        ->addField(createField("timestamp", BasicType::UINT64));
}

std::string BidGenerator::getName() { return "NEXMarkBid"; }

std::string BidGenerator::toString() {
    std::ostringstream oss;

    oss << getName() << "()";

    return oss.str();
}

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
