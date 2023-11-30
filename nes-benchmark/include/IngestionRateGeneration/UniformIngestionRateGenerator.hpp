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

#ifndef NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_UNIFORMINGESTIONRATEGENERATOR_HPP_
#define NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_UNIFORMINGESTIONRATEGENERATOR_HPP_

#include <IngestionRateGeneration/IngestionRateGenerator.hpp>

namespace NES::Benchmark::IngestionRateGeneration {
/**
 * @brief This class inherits from IngestionRateGenerator and allows for the generation of uniform distributed ingestion rates.
 */
class UniformIngestionRateGenerator : public IngestionRateGenerator {
  public:
    /**
     * @brief constructor for a uniform ingestion rate generator
     * @param ingestionRateInBuffers
     * @param ingestionRateCount
     */
    explicit UniformIngestionRateGenerator(uint64_t ingestionRateInBuffers, uint64_t ingestionRateCount);

    /**
     * @brief creates a vector of length ingestionRateCount and fills it with value ingestionRateInBuffers
     * @return predefinedIngestionRates
     */
    std::vector<std::uint64_t> generateIngestionRates() override;

  private:
    uint64_t ingestionRateInBuffers;
    std::vector<uint64_t> predefinedIngestionRates;
};
}// namespace NES::Benchmark::IngestionRateGeneration

#endif// NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_UNIFORMINGESTIONRATEGENERATOR_HPP_
