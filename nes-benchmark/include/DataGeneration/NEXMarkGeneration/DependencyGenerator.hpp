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

#ifndef NES_DEPENDENCYGENERATOR_HPP
#define NES_DEPENDENCYGENERATOR_HPP

#include <cstdint>
#include <vector>
#include <API/Schema.hpp>
#include <random>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

auto constexpr dependencyGeneratorSeed = 103984;
auto constexpr recordsInit = 50;

/**
 * @brief This class is a helper class for the NEXMark generators. As the data streams in NEXMark depend on each other,
 * we need a centralised unit to coordinate the each data stream with the others. It is responsible for keeping track
 * of all the creation timestamps and making sure that the data is consistent.
 */
class DependencyGenerator {
  public:
    /**
     * @brief returns the single instance of the class. Calls the constructor if needed
     * @param numberOfBuffers
     * @param bufferSize
     * @return DependencyGenerator instance
     */
    static DependencyGenerator& getInstance(size_t numberOfBuffers, size_t bufferSize);

    /**
     * @brief getter for persons vector
     * @return persons vector
     */
    std::vector<uint64_t>& getPersons();

    /**
     * @brief getter for auctions vector
     * @return auctions vector
     */
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>& getAuctions();

    /**
     * @brief getter for bids vector
     * @return bids vector
     */
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>& getBids();

    /**
     * @brief getter for numberOfRecords
     * @return numberOfRecords
     */
    uint64_t getNumberOfRecords();

  private:
    /**
     * @brief constructor of a DependencyGenerator
     * @param numberOfBuffers
     * @param bufferSize
     */
    explicit DependencyGenerator(size_t numberOfBuffers, size_t bufferSize);

    /**
     * @brief copy constructor and move assignment operator are deleted to prevent creation of multiple instances of the DependencyGenerator.
     * Note that the move constructor and move assignment operator are deleted implicitly by the compiler.
     */
    DependencyGenerator(const DependencyGenerator&) = delete;
    DependencyGenerator& operator=(const DependencyGenerator&) = delete;

    /**
     * @brief creates a timestamp for every person and adds it to persons vector
     * @param curTime
     * @param numPersons
     */
    void generatePersonDependencies(uint64_t& curTime, uint64_t numPersons);

    /**
     * @brief creates start and end timestamps as well as a random seller id and an initial price and adds the element to auctions vector
     * @param curTime
     * @param numOpenAuctions
     */
    void generateOpenAuctionDependencies(uint64_t& curTime, uint64_t numOpenAuctions);

    /**
     * @brief creates a timestamp as well as a random auction id, bidder id and a new bid and adds the element to bids vector
     * @param curTime
     * @param numBids
     */
    void generateBidDependencies(uint64_t& curTime, uint64_t numBids);

    /**
     * @brief increments the current time up to a minute
     * @param curTimeInSec
     * @return incremented time
     */
    static uint64_t incrementTime(uint64_t curTimeInSec);

    /**
     * @brief a persons element is a timestamp
     */
    std::vector<uint64_t> persons;
    /**
     * @brief an auctions element consists of seller id, current price, start timestamp and end timestamp
     */
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>> auctions;
    /**
     * @brief a bids element consists of auction id, bidder id, new bid and timestamp
     */
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>> bids;

    uint64_t numberOfRecords;
};
} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration

#endif//NES_DEPENDENCYGENERATOR_HPP
