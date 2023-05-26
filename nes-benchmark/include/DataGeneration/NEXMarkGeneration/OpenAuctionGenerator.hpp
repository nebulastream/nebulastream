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

#ifndef NES_OPENAUCTIONGENERATOR_HPP
#define NES_OPENAUCTIONGENERATOR_HPP

#include <DataGeneration/DataGenerator.hpp>
#include <DataGeneration/NEXMarkGeneration/DependencyGenerator.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

class OpenAuctionGenerator : public DataGenerator {
  public:
    /**
     * @brief creates a OpenAuctionGenerator
     */
    explicit OpenAuctionGenerator();

    /**
     * @brief creates data with the schema "id, reserve, privacy, sellerId, category, quantity, type, startTime, endTime"
     * from the auctions vector of dependencyGeneratorInstance. All values except sellerId, startTime and endTime are drawn
     * randomly from uniform distributions in a predefined range
     * @param numberOfBuffers
     * @param bufferSize
     * @return the TupleBuffer
     */
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;

    /**
     * @brief overrides the schema from the abstract parent class
     * @return schema of the OpenAuctionGenerator
     */
    SchemaPtr getSchema() override;

    /**
     * @brief overrides the name from the abstract parent class
     * @return name of the OpenAuctionGenerator
     */
    std::string getName() override;

    /**
     * @brief overrides the string representation of the parent class
     * @return the string representation of the OpenAuctionGenerator
     */
    std::string toString() override;
};
} //namespace NES::Benchmark::DataGeneration::NEXMarkGeneration

#endif//NES_OPENAUCTIONGENERATOR_HPP
