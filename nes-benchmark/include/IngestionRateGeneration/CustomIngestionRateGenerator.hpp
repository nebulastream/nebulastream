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

#ifndef NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_CUSTOMINGESTIONRATEGENERATOR_HPP_
#define NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_CUSTOMINGESTIONRATEGENERATOR_HPP_

#include <IngestionRateGeneration/IngestionRateGenerator.hpp>

namespace NES::Benchmark::IngestionRateGeneration {
/**
 * @brief This class inherits from IngestionRateGenerator and allows for the generation of M1, M2, D1 and D2 distributed ingestion rates.
 */
class CustomIngestionRateGenerator : public IngestionRateGenerator {
  public:
    /**
     * @brief constructor for a uniform ingestion rate generator
     * @param ingestionRateDistribution
     * @param ingestionRateCount
     */
    explicit CustomIngestionRateGenerator(uint64_t ingestionRateCount, std::vector<uint64_t>& customValues);

    /**
      * @brief creates a vector of length ingestionRateCount and fills it with values of m1Values, m2Values, d1Values or d2Values
      * @return predefinedIngestionRates
      */
    std::vector<std::uint64_t> generateIngestionRates() override;

  private:
    std::vector<uint64_t> customValues;
    std::vector<uint64_t> predefinedIngestionRates;
};
}// namespace NES::Benchmark::IngestionRateGeneration

#endif // NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_CUSTOMINGESTIONRATEGENERATOR_HPP_
