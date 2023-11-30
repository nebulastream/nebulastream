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
class E2EBenchmarkConfigOverAllRuns {

  public:
    /**
     * @brief creates a E2EBenchmarkConfigPerRun object and sets the default values
     */
    explicit E2EBenchmarkConfigOverAllRuns();

    /**
     * @brief creates a string representation of this object
     * @return the string representation
     */
    std::string toString();

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
    std::string getStrLogicalSrcDataGenerators();

    /**
     * @brief all configurations that are constant over all runs
     */
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
    Configurations::StringConfigOption query;
    Configurations::StringConfigOption dataProviderMode;
    std::map<std::string, DataGeneration::DataGeneratorPtr> sourceNameToDataGenerator;
    Configurations::StringConfigOption connectionString;
    Configurations::StringConfigOption joinStrategy;
};
}// namespace NES::Benchmark

#endif  // NES_BENCHMARK_INCLUDE_E2E_CONFIGURATIONS_E2EBENCHMARKCONFIGOVERALLRUNS_HPP_
