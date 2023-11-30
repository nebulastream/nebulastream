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

#ifndef NES_BENCHMARK_INCLUDE_DATAGENERATION_DEFAULTDATAGENERATOR_HPP_
#define NES_BENCHMARK_INCLUDE_DATAGENERATION_DEFAULTDATAGENERATOR_HPP_

#include <DataGeneration/DataGenerator.hpp>

namespace NES::Benchmark::DataGeneration {
auto constexpr GENERATOR_SEED_DEFAULT = 848566;

class DefaultDataGenerator : public DataGenerator {
  public:
    /**
     * @brief creates a DefaultDataGenerator
     * @param minValue
     * @param maxValue
     */
    explicit DefaultDataGenerator(uint64_t minValue, uint64_t maxValue);

    /**
     * @brief creates uniformed data with the schema "id, value, payload, timestamp"
     * the id, payload, and timestamp are just counters that increment whereas the value gets drawn
     * randomly from a uniform distribution in the range [minValue, maxValue]
     * @param numberOfBuffers
     * @param bufferSize
     * @return success
     */
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;

    /**
     * @brief overrides the schema from the abstract parent class
     * @return schema from a DefaultDataGenerator
     */
    SchemaPtr getSchema() override;

    /**
     * @brief overrides the schema from the abstract parent class
     * @return schema from a DefaultDataGenerator
     */
    Configurations::SchemaTypePtr getSchemaType() override;

    /**
     * @brief overrides the name from the abstract parent class
     * @return name
     */
    std::string getName() override;

    /**
     * @brief overrides the string representation of the parent class
     * @return
     */
    std::string toString() override;

  private:
    uint64_t minValue;
    uint64_t maxValue;
};
}// namespace NES::Benchmark::DataGeneration
#endif  // NES_BENCHMARK_INCLUDE_DATAGENERATION_DEFAULTDATAGENERATOR_HPP_
