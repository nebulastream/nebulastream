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

#ifndef NES_BENCHMARK_INCLUDE_E2E_CONFIGURATIONS_E2EBENCHMARKCONFIGOVERALLRUNS_HPP_
#define NES_BENCHMARK_INCLUDE_E2E_CONFIGURATIONS_E2EBENCHMARKCONFIGOVERALLRUNS_HPP_

#include <Configurations/ConfigurationOption.hpp>
#include <DataGeneration/DataGenerator.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>

namespace NES::Benchmark {

// Default values for config options
constexpr auto defaultCustomDelayInSeconds = 0;

/**
 * @brief all configurations that are constant over all runs
 */
class E2EBenchmarkConfigOverAllRuns {

  public:
    /**
     * @brief encapsulates the query and the custom delay for query config
     */
    class E2EBenchmarkQueryConfig {
      public:
        E2EBenchmarkQueryConfig(std::string queryString, uint32_t customDelayInSeconds)
            : queryString(std::move(queryString)), customDelayInSeconds(customDelayInSeconds) {}
        friend std::ostream& operator<<(std::ostream& os, const E2EBenchmarkQueryConfig& config);

        E2EBenchmarkQueryConfig(const E2EBenchmarkQueryConfig& other)
            : queryString(other.queryString), customDelayInSeconds(other.customDelayInSeconds) {}

        E2EBenchmarkQueryConfig(E2EBenchmarkQueryConfig&& other) noexcept
            : queryString(std::move(other.queryString)), customDelayInSeconds(other.customDelayInSeconds) {}

        E2EBenchmarkQueryConfig& operator=(const E2EBenchmarkQueryConfig& other);
        E2EBenchmarkQueryConfig& operator=(E2EBenchmarkQueryConfig&& other);

        [[nodiscard]] const std::string& getQueryString() const;
        [[nodiscard]] uint32_t getCustomDelayInSeconds() const;

      private:
        std::string queryString;
        uint32_t customDelayInSeconds;
    };

    /**
     * @brief creates a E2EBenchmarkConfigPerRun object and sets the default values
     */
    explicit E2EBenchmarkConfigOverAllRuns();

    /**
     * @brief creates a string representation of this object
     * @return the string representation
     */
    [[nodiscard]] std::string toString() const;

    /**
     * @brief parses and generates the config for the parameters constant over all
     * runs by parsing the yamlConfig
     * @param yamlConfig
     * @return
     */
    static E2EBenchmarkConfigOverAllRuns generateConfigOverAllRuns(Yaml::Node yamlConfig);

    /**
     * @brief calculates the total schema size for this run
     * @return total schema size for all logical sources
     */
    size_t getTotalSchemaSize();

    /**
     * @brief creates a string representation of mapLogicalSrcNameToDataGenerator
     * @return string representation
     */
    [[nodiscard]] std::string getStrLogicalSrcDataGenerators() const;

    /**
     * @brief creates a string representation of queries vector
     * @return string representation
     */
    [[nodiscard]] std::string getStrQueries() const;

  public:
    Configurations::IntConfigOption startupSleepIntervalInSeconds;
    Configurations::IntConfigOption numMeasurementsToCollect;
    Configurations::IntConfigOption experimentMeasureIntervalInSeconds;
    Configurations::IntConfigOption numberOfPreAllocatedBuffer;
    Configurations::IntConfigOption numberOfBuffersToProduce;
    Configurations::IntConfigOption batchSize;
    Configurations::IntConfigOption ingestionRateInBuffers;
    Configurations::IntConfigOption ingestionRateCount;
    Configurations::IntConfigOption numberOfPeriods;
    Configurations::StringConfigOption ingestionRateDistribution;
    Configurations::StringConfigOption customValues;
    Configurations::StringConfigOption dataProvider;
    Configurations::StringConfigOption outputFile;
    Configurations::StringConfigOption benchmarkName;
    Configurations::StringConfigOption inputType;
    Configurations::StringConfigOption sourceSharing;
    std::vector<E2EBenchmarkQueryConfig> queries;
    Configurations::StringConfigOption dataProviderMode;
    std::map<std::string, DataGeneration::DataGeneratorPtr> sourceNameToDataGenerator;
    Configurations::StringConfigOption connectionString;
    Configurations::StringConfigOption joinStrategy;
};
}// namespace NES::Benchmark

#endif// NES_BENCHMARK_INCLUDE_E2E_CONFIGURATIONS_E2EBENCHMARKCONFIGOVERALLRUNS_HPP_
