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

#ifndef NES_INGESTIONRATEGENERATOR_HPP
#define NES_INGESTIONRATEGENERATOR_HPP

#include <Util/Logger/Logger.hpp>
#include <E2E/Configurations/E2EBenchmarkConfig.hpp>
#include <cstdint>
#include <vector>
#include <string>

namespace NES::Benchmark::IngestionRateGeneration {

enum class IngestionRateDistribution : uint8_t {
    UNIFORM,
    SINUS,
    COSINUS,
    M1,
    M2,
    D1,
    D2,
    UNDEFINED
};

class IngestionRateGenerator;
using IngestionRateGeneratorPtr = std::unique_ptr<IngestionRateGenerator>;

class IngestionRateGenerator {
  public:
    /**
     * @brief constructor for an ingestion rate generator
     */
    explicit IngestionRateGenerator() = default;

    /**
     * @brief virtual destructor
     */
    virtual ~IngestionRateGenerator() = default;

    /**
     * @brief creates a vector of ingestion rates
     * @return vector of predefined ingestion rates
     */
    virtual std::vector<uint64_t> generateIngestionRates() = 0;

    /**
     * @brief
     * @return
     */
    static IngestionRateGeneratorPtr createIngestionRateGenerator(E2EBenchmarkConfigOverAllRuns& configOverAllRuns);

  private:
    /**
     * @brief determines whether the given ingestion rate distribution is supported
     * @param ingestionRateDistribution
     * @return IngestionRateDistribution
     */
    static IngestionRateDistribution getDistributionFromString(std::string& ingestionRateDistribution);
};
}// namespace NES::Benchmark::IngestionRateGeneration

#endif//NES_INGESTIONRATEGENERATOR_HPP
