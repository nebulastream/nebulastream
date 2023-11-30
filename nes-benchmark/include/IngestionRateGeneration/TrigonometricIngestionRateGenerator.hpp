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

#ifndef NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_TRIGONOMETRICINGESTIONRATEGENERATOR_HPP_
#define NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_TRIGONOMETRICINGESTIONRATEGENERATOR_HPP_

#include <IngestionRateGeneration/IngestionRateGenerator.hpp>

namespace NES::Benchmark::IngestionRateGeneration {
/**
 * @brief As we still build on with libstdc++, this is how we get the numerical value of pi
 * @return pi
 */
constexpr double PI() { return 3.14159265358979323846; }

/**
 * @brief This class inherits from IngestionRateGenerator and allows for the generation of sine and cosine distributed ingestion rates.
 */
class TrigonometricIngestionRateGenerator : public IngestionRateGenerator {
  public:
    /**
     * @brief constructor for a uniform ingestion rate generator
     * @param ingestionRateInBuffers
     * @param ingestionRateCount
     */
    explicit TrigonometricIngestionRateGenerator(IngestionRateDistribution ingestionRateDistribution,
                                                 uint64_t ingestionRateInBuffers,
                                                 uint64_t ingestionRateCount,
                                                 uint64_t numberOfPeriods);

    /**
      * @brief creates a vector of length ingestionRateCount and fills it with values that are distributed according to ingestionRateDistribution
      * @return predefinedIngestionRates
      */
    std::vector<std::uint64_t> generateIngestionRates() override;

  private:
    /**
     * @brief calculates the sine of x. Sine has a period length of ingestionRateInBuffers divided by numberOfPeriods
     * @param xValue
     * @return value
     */
    double getSinValue(uint64_t xValue);

    /**
     * @brief calculates the cosine of x. Cosine has a period length of ingestionRateInBuffers divided by numberOfPeriods
     * @param xValue
     * @return value
     */
    double getCosValue(uint64_t xValue);

    IngestionRateDistribution ingestionRateDistribution;
    uint64_t ingestionRateInBuffers;
    uint64_t numberOfPeriods;
    std::vector<uint64_t> predefinedIngestionRates;
};
}// namespace NES::Benchmark::IngestionRateGeneration

#endif  // NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_TRIGONOMETRICINGESTIONRATEGENERATOR_HPP_
