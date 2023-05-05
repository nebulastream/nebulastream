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

#ifndef NES_GENERATORMEDIATOR_HPP
#define NES_GENERATORMEDIATOR_HPP

#include <cstdint>
#include <vector>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

class GeneratorMediator {
  public:
    /**
     * @brief
     * @param size
     * @return
     */
    static GeneratorMediator& getInstance(size_t numberOfBuffers, size_t bufferSize);

    uint64_t getPersonTimestamp (uint64_t personId);

    std::tuple<uint64_t, uint64_t> getAuctionTimestamps (uint64_t auctionId);

    uint64_t getAuctionId (uint64_t timestamp);

    uint64_t getPersonId (uint64_t timestamp);

  private:
    /**
     * @brief constructor of a GeneratorMediator
     * @param numberOfBuffers
     * @param bufferSize
     */
    GeneratorMediator(size_t numberOfBuffers, size_t bufferSize);

    /**
     * @brief copy constructor and move assignment operator are deleted to prevent creation of multiple instances of the GeneratorMediator.
     * Note that the move constructor and move assignment operator are deleted implicitly by the compiler.
     */
    GeneratorMediator(const GeneratorMediator&) = delete;
    GeneratorMediator& operator=(const GeneratorMediator&) = delete;

    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> auctions;
    std::vector<std::tuple<uint64_t, uint64_t>> persons;
};
} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration

#endif//NES_GENERATORMEDIATOR_HPP
