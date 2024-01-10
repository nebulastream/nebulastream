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

#ifndef NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_INGESTIONRATEGENERATOR_HPP_
#define NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_INGESTIONRATEGENERATOR_HPP_

#include <E2E/Configurations/E2EBenchmarkConfig.hpp>
#include <cstdint>
#include <string>
#include <vector>

namespace NES::Benchmark::IngestionRateGeneration {

/**
 * @brief This class defines the different types of supported ingestion rate generators.
 */
enum class IngestionRateDistribution : uint8_t { UNIFORM, SINUS, COSINUS, CUSTOM, UNDEFINED };

class IngestionRateGenerator;
using IngestionRateGeneratorPtr = std::unique_ptr<IngestionRateGenerator>;

/**
 * @brief This class is used by the ExternalProvider to create a vector of potentially different ingestion rates in order to support dynamic ingestion rates.
 */
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
      * @brief creates a specific type of ingestion rate generator based on the given ingestion rate distribution
      * @param configOverAllRuns
      * @return pointer to a type of ingestion rate generator
      */
    static IngestionRateGeneratorPtr createIngestionRateGenerator(E2EBenchmarkConfigOverAllRuns& configOverAllRuns);

  protected:
    uint64_t ingestionRateCount = 0;

  private:
    /**
     * @brief determines whether the given ingestion rate distribution is supported
     * @param ingestionRateDistribution
     * @return IngestionRateDistribution
     */
    static IngestionRateDistribution getDistributionFromString(std::string& ingestionRateDistribution);
};
}// namespace NES::Benchmark::IngestionRateGeneration

#endif // NES_BENCHMARK_INCLUDE_INGESTIONRATEGENERATION_INGESTIONRATEGENERATOR_HPP_
