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

#ifndef NES_BENCHMARK_INCLUDE_E2E_E2ERUNNER_HPP_
#define NES_BENCHMARK_INCLUDE_E2E_E2ERUNNER_HPP_

#include <E2E/E2ESingleRun.hpp>

namespace NES::Benchmark {
/**
 * @brief parses and returns an E2EBenchmarkConfig. Also
 * @param configPath
 * @param logPath
 * @return E2EBenchmarkConfig
 */
E2EBenchmarkConfig parseYamlConfig(std::string configPath, std::string logPath);

/**
 * @brief Runs a single experiment with the given rpcPort and restPort for the nesCoordinator
 * @param configPerRun
 * @param configOverallRuns
 * @param rpcPort
 * @param restPort
 */
void executeSingleRun(E2EBenchmarkConfigPerRun& configPerRun,
                      E2EBenchmarkConfigOverAllRuns& configOverallRuns,
                      int rpcPort,
                      int restPort);

/**
 * @brief Writes the header to the output csv file
 * @param e2EBenchmarkConfig
 */
void writeHeaderToCsvFile(E2EBenchmarkConfigOverAllRuns& configOverAllRuns);

}// namespace NES::Benchmark
#endif// NES_BENCHMARK_INCLUDE_E2E_E2ERUNNER_HPP_
