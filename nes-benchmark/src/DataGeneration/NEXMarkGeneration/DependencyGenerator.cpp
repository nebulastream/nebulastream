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

#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <DataGeneration/NEXMarkGeneration/BidGenerator.hpp>
#include <DataGeneration/NEXMarkGeneration/DependencyGenerator.hpp>
#include <DataGeneration/NEXMarkGeneration/OpenAuctionGenerator.hpp>
#include <DataGeneration/NEXMarkGeneration/PersonGenerator.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

DependencyGenerator::DependencyGenerator(size_t numberOfBuffers, size_t bufferSize) {
    // using seed to generate a predictable sequence of values for deterministic behavior
    // note that this particular seed is from the original java files
    std::mt19937 initGenerator(dependencyGeneratorSeed);
    std::uniform_int_distribution<uint64_t> uniformPersonGenerationDistribution(0, 9);
    std::uniform_int_distribution<uint64_t> uniformOpenAuctionGenerationDistribution(0, 2);
    std::uniform_int_distribution<uint64_t> uniformBidGenerationDistribution(0, 20);

    // calculate how many records to create
    // note that a person buffer will have 12 additional text fields and an auction buffer will have one
    auto personSchemaSize = PersonGenerator().getSchema()->getSchemaSizeInBytes() + 12 * bufferSize;
    auto auctionSchemaSize = OpenAuctionGenerator().getSchema()->getSchemaSizeInBytes() + bufferSize;
    auto bidSchemaSize = BidGenerator().getSchema()->getSchemaSizeInBytes();
    auto totalBufferSizeInBytes = numberOfBuffers * bufferSize;
    numberOfRecords = (totalBufferSizeInBytes - recordsInit * (personSchemaSize + auctionSchemaSize)) / (personSchemaSize / 10.0 + auctionSchemaSize + 10.0 * bidSchemaSize);
    NES_ASSERT(numberOfRecords > 0, "numberOfPreAllocatedBuffer or bufferSizeInBytes is too small!");

    auto timeInSec = 0_u64;

    // first generate some persons and open auctions that can be bid on
    for (auto i = 0; i < recordsInit; ++i) {
        generatePersonDependencies(timeInSec, 1);
    }
    for (auto i = 0; i < recordsInit; ++i) {
        generateOpenAuctionDependencies(timeInSec, 1);
    }

    // now generate approximately numberOfRecords many auctions as well as corresponding persons and bids
    for (auto i = 0UL; i < numberOfRecords; ++i) {
        // create on average one person per ten auctions
        if (uniformPersonGenerationDistribution(initGenerator) == 0) {
            generatePersonDependencies(timeInSec, 1);
        }

        // create on average one open auction
        auto numOpenAuctions = uniformOpenAuctionGenerationDistribution(initGenerator);
        generateOpenAuctionDependencies(timeInSec, numOpenAuctions);

        // create on average ten bids per auction
        auto numBids = uniformBidGenerationDistribution(initGenerator);
        generateBidDependencies(timeInSec, numBids);
    }
}

DependencyGenerator& DependencyGenerator::getInstance(size_t numberOfBuffers, size_t bufferSize) {
    // C++11 guarantees that static local variables are initialized in a thread-safe manner
    static DependencyGenerator instance(numberOfBuffers, bufferSize);
    return instance;
}

void DependencyGenerator::generatePersonDependencies(uint64_t& curTime, uint64_t numPersons) {
    curTime = incrementTime(curTime);
    auto copyOfCurTime = curTime;

    for (uint64_t i = 0; i < numPersons; ++i) {
        // create a new element for the persons vector
        persons.emplace_back(copyOfCurTime);
    }
}

void DependencyGenerator::generateOpenAuctionDependencies(uint64_t& curTime, uint64_t numOpenAuctions) {
    // using seed to generate a predictable sequence of values for deterministic behavior
    static std::mt19937 generator(42);
    // generate a random seller id from the pool of persons
    std::uniform_int_distribution<uint64_t> uniformPersonIdDistribution(0, persons.size() - 1);
    // generate a random initial price that is between one and two-hundred dollars
    static std::uniform_int_distribution<uint64_t> uniformCurPriceDistribution(1, 200);
    // generate a random end time that is between two and twenty-six hours after the start time
    static std::uniform_int_distribution<uint64_t> uniformEndTimeDistribution(2 * 60 * 60, 26 * 60 * 60 - 1);

    curTime = incrementTime(curTime);
    auto copyOfCurTime = curTime;

    for (uint64_t i = 0; i < numOpenAuctions; ++i) {
        auto sellerId = uniformPersonIdDistribution(generator);
        auto curPrice = uniformCurPriceDistribution(generator);
        auto endTime = curTime + uniformEndTimeDistribution(generator);
        // create a new element for the auctions vector
        std::tuple<uint64_t, uint64_t, uint64_t, uint64_t> auctionRecord = std::make_tuple(sellerId, curPrice, copyOfCurTime, endTime);
        auctions.emplace_back(auctionRecord);
    }
}

void DependencyGenerator::generateBidDependencies(uint64_t& curTime, uint64_t numBids) {
    // using seed to generate a predictable sequence of values for deterministic behavior
    static std::mt19937 generator(42);
    // generate a random auction id from the pool of auctions
    std::uniform_int_distribution<uint64_t> uniformAuctionIdDistribution(0, auctions.size() - 1);
    // generate a random seller id from the pool of persons
    std::uniform_int_distribution<uint64_t> uniformPersonIdDistribution(0, persons.size() - 1);
    // generate a random bid that increases the current price between one and twenty-five dollars
    static std::uniform_int_distribution<uint64_t> uniformNewBidDistribution(1, 25);

    curTime = incrementTime(curTime);
    auto copyOfCurTime = curTime;

    for (uint64_t i = 0; i < numBids; ++i) {
        auto auctionId = uniformAuctionIdDistribution(generator);
        auto bidderId = uniformPersonIdDistribution(generator);
        // get latest bid on the auction, generate a higher bid and update the auction record in auctions vector
        auto& curPrice = std::get<1>(auctions[auctionId]);
        auto newBid = curPrice + uniformNewBidDistribution(generator);
        curPrice = newBid;
        // create a new element for the bids vector
        std::tuple<uint64_t, uint64_t, uint64_t, uint64_t> bidRecord = std::make_tuple(auctionId, bidderId, newBid, copyOfCurTime);
        bids.emplace_back(bidRecord);
    }
}

uint64_t DependencyGenerator::incrementTime(uint64_t curTimeInSec) {
    // using seed to generate a predictable sequence of values for deterministic behavior
    static std::mt19937 generator(42);
    static std::uniform_int_distribution<uint64_t> uniformTimeDistribution(0, 59);

    return curTimeInSec + uniformTimeDistribution(generator);
}

std::vector<uint64_t>& DependencyGenerator::getPersons() { return persons; }

std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>& DependencyGenerator::getAuctions() { return auctions; }

std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>& DependencyGenerator::getBids() { return bids; }

uint64_t DependencyGenerator::getNumberOfRecords(){ return numberOfRecords; }

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
