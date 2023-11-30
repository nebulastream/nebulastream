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

#ifndef NES_BENCHMARK_INCLUDE_E2E_CONFIGURATIONS_E2EBENCHMARKCONFIG_HPP_
#define NES_BENCHMARK_INCLUDE_E2E_CONFIGURATIONS_E2EBENCHMARKCONFIG_HPP_

#include <Configurations/ConfigurationOption.hpp>
#include <E2E/Configurations/E2EBenchmarkConfigOverAllRuns.hpp>
#include <E2E/Configurations/E2EBenchmarkConfigPerRun.hpp>
#include <Util/yaml/Yaml.hpp>
#include <memory>
#include <vector>

namespace NES::Benchmark {

/**
     * @brief this class saves all the configuration parameters and
     * creates benchmarks from a provided yaml file
     */
class E2EBenchmarkConfig {

  public:
    /**
         * @brief creates benchmarks from the yaml file. Expects that yamlConfigFile exists
         * @param configPath
         * @return
         */
    static E2EBenchmarkConfig createBenchmarks(const std::string& yamlConfigFile);

    /**
         * @brief reads the logging level from the config file. Expects that yamlConfigFile exists
         * @param yamlConfigFile
         * @return loglevel
         */
    static NES::LogLevel getLogLevel(const std::string& yamlConfigFile, NES::LogLevel defaultLogLevel = NES::LogLevel::LOG_DEBUG);

    /**
         * @brief creates a string representation of this object
         * @return the string representation
         */
    std::string toString();

    [[nodiscard]] std::vector<E2EBenchmarkConfigPerRun>& getAllConfigPerRuns() { return allConfigPerRuns; }
    E2EBenchmarkConfigOverAllRuns& getConfigOverAllRuns() { return configOverAllRuns; }

  private:
    std::vector<E2EBenchmarkConfigPerRun> allConfigPerRuns;
    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
};
}// namespace NES::Benchmark

#endif  // NES_BENCHMARK_INCLUDE_E2E_CONFIGURATIONS_E2EBENCHMARKCONFIG_HPP_
