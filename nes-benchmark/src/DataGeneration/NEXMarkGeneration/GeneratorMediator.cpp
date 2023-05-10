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

#include <DataGeneration/NEXMarkGeneration/GeneratorMediator.hpp>
#include <DataGeneration/NEXMarkGeneration/OpenAuctionGenerator.hpp>
#include <API/Schema.hpp>
#include <random>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

GeneratorMediator::GeneratorMediator(size_t numberOfBuffers, size_t bufferSize, uint64_t numGenCalls) {
    auto timeInSec = 0UL;
    std::mt19937 generator(103984);
    std::uniform_int_distribution<uint64_t> uniformPersonDistribution(0, 9);
    std::uniform_int_distribution<uint64_t> uniformOpenAuctionDistribution(0, 2);
    std::uniform_int_distribution<uint64_t> uniformBidDistribution(0, 20);

    // first generate some persons and open auctions that can be bid on
    for (auto i = 0; i < 50; ++i) {
        createPersonTimestamps(&timeInSec, 1);
    }
    for (auto i = 0; i < 50; ++i) {
        createOpenAuctionTimestamps(&timeInSec, 1);
    }

    // now generate the data stream
    for (auto i = 0UL; i < numGenCalls; ++i) {
        // create on average one person per ten auctions
        if (uniformPersonDistribution(generator) == 0) {
            createPersonTimestamps(&timeInSec, 1);
        }

        // create on average one open auction
        auto numOpenAuctions = uniformOpenAuctionDistribution(generator);
        createOpenAuctionTimestamps(&timeInSec, numOpenAuctions);

        // create on average ten bids per auction
        auto numBids = uniformBidDistribution(generator);
        createBidTimestamps(&timeInSec, numBids);
    }
}

GeneratorMediator& GeneratorMediator::getInstance(size_t numberOfBuffers, size_t bufferSize, uint64_t numGenCalls) {
    // C++11 guarantees that static local variables are initialized in a thread-safe manner
    static GeneratorMediator instance(numberOfBuffers, bufferSize, numGenCalls);
    return instance;
}

void GeneratorMediator::createPersonTimestamps(uint64_t* curTime, uint64_t numPersons) {
    for (uint64_t i = 0; i < numPersons; ++i) {
        std::tuple<uint64_t, uint64_t> personRecord;
        persons.emplace_back(personRecord);
    }
}

void GeneratorMediator::createOpenAuctionTimestamps(uint64_t* curTime, uint64_t numOpenAuctions) {
    for (uint64_t i = 0; i < numOpenAuctions; ++i) {
        std::tuple<uint64_t, uint64_t, uint64_t> auctionRecord;
        auctions.emplace_back(auctionRecord);
    }
}

void GeneratorMediator::createBidTimestamps(uint64_t* curTime, uint64_t numBids) {
    for (uint64_t i = 0; i < numBids; ++i) {
        std::tuple<uint64_t, uint64_t> bidRecord;
        bids.emplace_back(bidRecord);
    }
}

uint64_t GeneratorMediator::incrementTime(uint64_t curTime) {
    std::mt19937 generator(103984);
    static std::uniform_int_distribution<uint64_t> uniformTimeDistribution(0, 59);

    return curTime + uniformTimeDistribution(generator);
}

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
