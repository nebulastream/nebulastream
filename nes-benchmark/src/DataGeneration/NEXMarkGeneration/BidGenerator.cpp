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
#include <Runtime/TupleBuffer.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

BidGenerator::BidGenerator(uint64_t numberOfRecords)
    : DataGenerator(), numberOfRecords(numberOfRecords), dependencyGeneratorInstance(DependencyGenerator::getInstance(numberOfRecords)) {}

std::vector<Runtime::TupleBuffer> BidGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> createdBuffers;
    createdBuffers.reserve(numberOfBuffers);

    auto memoryLayout = this->getMemoryLayout(bufferSize);

    auto bids = dependencyGeneratorInstance.getBids();
    auto processedBids = 0UL;

    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

        // TODO add designated branch for RowLayout to make it faster (cmp. DefaultDataGenerator.cpp)
        for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
            // TODO what if numberOfRecords is smaller than the number of records we can fit in the buffers? Timestamp would be reset
            // TODO what if numberOfRecords is larger than the number of records we can fit in the buffers? We might get bids that reference auction id's or bidder id's that haven't been created yet
            auto bidsIndex = processedBids++ % bids.size();

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

    oss << getName() << " (" << numberOfRecords << ")";

    return oss.str();
}

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
