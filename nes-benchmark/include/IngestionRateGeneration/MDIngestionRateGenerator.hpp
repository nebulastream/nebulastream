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

#ifndef NES_MDINGESTIONRATEGENERATOR_HPP
#define NES_MDINGESTIONRATEGENERATOR_HPP

#include <IngestionRateGeneration/IngestionRateGenerator.hpp>

namespace NES::Benchmark::IngestionRateGeneration {

// TODO turn M1,M2,D1,D2 into functions
// TODO use vector instead of array -> this may be resolved by the TODO above
constexpr uint64_t m1Values[] = {35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000};
constexpr uint64_t m2Values[] = {10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000,50000,40000,30000,20000};
constexpr uint64_t d1Values[] = {10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000,50000,40000,30000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000};
constexpr uint64_t d2Values[] = {100000,90000,80000,70000,60000,50000,40000,30000,20000,10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000,50000,40000,30000,20000,30000,40000,50000};

class MDIngestionRateGenerator : public IngestionRateGenerator {
  public:
    /**
     * @brief constructor for a uniform ingestion rate generator
     * @param ingestionRateDistribution
     * @param ingestionRateCnt
     */
    explicit MDIngestionRateGenerator(std::string ingestionRateDistribution, uint64_t ingestionRateCnt);

    /**
      * @brief creates a vector of length ingestionRateCnt and fills it with values of m1Values, m2Values, d1Values or d2Values
      * @return predefinedIngestionRates
      */
    std::vector<std::uint64_t> generateIngestionRates() override;

  private:
    IngestionRateDistribution ingestionRateDistribution;
    uint64_t ingestionRateCnt;
    std::vector<uint64_t> predefinedIngestionRates;
};
}// namespace NES::Benchmark::IngestionRateGeneration

#endif//NES_MDINGESTIONRATEGENERATOR_HPP
