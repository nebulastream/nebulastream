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

#include <DataGeneration/NEXMarkGeneration/DependencyGenerator.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

DependencyGenerator::DependencyGenerator(uint64_t numberOfRecords) {
    // using a seed to generate a predictable sequence of values for deterministic behavior
    std::mt19937 generator(103984);
    std::uniform_int_distribution<uint64_t> uniformPersonGenerationDistribution(0, 9);
    std::uniform_int_distribution<uint64_t> uniformOpenAuctionGenerationDistribution(0, 2);
    std::uniform_int_distribution<uint64_t> uniformBidGenerationDistribution(0, 20);

    auto timeInSec = 0UL;

    // first generate some persons and open auctions that can be bid on
    for (auto i = 0; i < 50; ++i) {
        generatePersonDependencies(timeInSec, 1);
    }
    for (auto i = 0; i < 50; ++i) {
        generateOpenAuctionDependencies(timeInSec, 1);
    }

    // now generate approximately numberOfRecords many auctions as well as corresponding persons and bids
    for (auto i = 0UL; i < numberOfRecords; ++i) {
        // create on average one person per ten auctions
        if (uniformPersonGenerationDistribution(generator) == 0) {
            generatePersonDependencies(timeInSec, 1);
        }

        // create on average one open auction
        auto numOpenAuctions = uniformOpenAuctionGenerationDistribution(generator);
        generateOpenAuctionDependencies(timeInSec, numOpenAuctions);

        // create on average ten bids per auction
        auto numBids = uniformBidGenerationDistribution(generator);
        generateBidDependencies(timeInSec, numBids);
    }
}

DependencyGenerator& DependencyGenerator::getInstance(uint64_t numberOfRecords) {
    // C++11 guarantees that static local variables are initialized in a thread-safe manner
    static DependencyGenerator instance(numberOfRecords);
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
    curTime = incrementTime(curTime);
    auto copyOfCurTime = curTime;
    std::random_device rndDevice;
    std::mt19937 generator(rndDevice());

    // generate a random seller id from the pool of persons
    auto maxSellerId = persons.size() - 1;
    std::uniform_int_distribution<uint64_t> uniformPersonIdDistribution(0, maxSellerId);

    // generate a random initial price that is between one and two-hundred dollars
    std::uniform_int_distribution<uint64_t> uniformCurPriceDistribution(1, 200);

    // generate a random end time that is between two and twenty-six hours after the start time
    std::uniform_int_distribution<uint64_t> uniformEndTimeDistribution(2 * 60 * 60, 26 * 60 * 60 - 1);

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
    curTime = incrementTime(curTime);
    auto copyOfCurTime = curTime;
    std::random_device rndDevice;
    std::mt19937 generator(rndDevice());

    // generate a random auction id from the pool of auctions
    auto maxAuctionId = auctions.size() - 1;
    std::uniform_int_distribution<uint64_t> uniformAuctionIdDistribution(0, maxAuctionId);

    // generate a random seller id from the pool of persons
    auto maxBidderId = persons.size() - 1;
    std::uniform_int_distribution<uint64_t> uniformPersonIdDistribution(0, maxBidderId);

    // generate a random bid that increases the current price between one and twenty-five dollars
    std::uniform_int_distribution<uint64_t> uniformNewBidDistribution(1, 25);

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
    // generate a random new time that is at most one minute in the future
    std::random_device rndDevice;
    std::mt19937 generator(rndDevice());
    std::uniform_int_distribution<uint64_t> uniformTimeDistribution(0, 59);

    return curTimeInSec + uniformTimeDistribution(generator);
}

std::vector<uint64_t>& DependencyGenerator::getPersons() { return persons; }

std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>& DependencyGenerator::getAuctions() { return auctions; }

std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>& DependencyGenerator::getBids() { return bids; }

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
