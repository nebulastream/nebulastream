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

#ifndef NES_TRIGONOMETRICINGESTIONRATEGENERATOR_HPP
#define NES_TRIGONOMETRICINGESTIONRATEGENERATOR_HPP

#include <IngestionRateGeneration/IngestionRateGenerator.hpp>

namespace NES::Benchmark::IngestionRateGeneration {
/**
 * @brief This class inherits from IngestionRateGenerator and allows for the generation of sine and cosine distributed ingestion rates.
 */
class TrigonometricIngestionRateGenerator : public IngestionRateGenerator {

    // As we still build on with libstdc++, we have to use this way of getting PI
    constexpr double PI() { return std::atan(1)*4; }

  public:
    /**
     * @brief constructor for a uniform ingestion rate generator
     * @param ingestionRateInBuffers
     * @param ingestionRateCnt
     */
    explicit TrigonometricIngestionRateGenerator(IngestionRateDistribution ingestionRateDistribution, uint64_t ingestionRateInBuffers, uint64_t ingestionRateCnt, uint64_t numberOfPeriods);

    /**
      * @brief creates a vector of length ingestionRateCnt and fills it with values that are distributed according to ingestionRateDistribution
      * @return predefinedIngestionRates
      */
    std::vector<std::uint64_t> generateIngestionRates() override;

  private:
    /**
     * @brief calculates the sine of x. Sine has a period length of ingestionRateInBuffers/numberOfPeriods
     * @param x
     * @return value
     */
    double getSinValue(uint64_t x);

    /**
     * @brief calculates the cosine of x. Cosine has a period length of ingestionRateInBuffers/numberOfPeriods
     * @param x
     * @return value
     */
    double getCosValue(uint64_t x);

    IngestionRateDistribution ingestionRateDistribution;
    uint64_t ingestionRateInBuffers;
    uint64_t ingestionRateCnt;
    uint64_t numberOfPeriods;
    std::vector<uint64_t> predefinedIngestionRates;
};
}// namespace NES::Benchmark::IngestionRateGeneration

#endif//NES_TRIGONOMETRICINGESTIONRATEGENERATOR_HPP
