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

#ifndef NES_E2EBENCHMARKCONFIGPERRUN_HPP
#define NES_E2EBENCHMARKCONFIGPERRUN_HPP

#include <Configurations/ConfigurationOption.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/yaml/Yaml.hpp>
#include <vector>

namespace NES::Benchmark {

class E2EBenchmarkConfigPerRun {

  public:
    /**
     * @brief creates a E2EBenchmarkConfigPerRun object and sets the default values
     */
    explicit E2EBenchmarkConfigPerRun();

    /**
     * @brief creates a string representation of this object
     * @return the string representation
     */
    std::string toString();

    /**
     * @brief parses and generates the config for the parameters changing per run
     * runs by parsing the yamlConfig
     * @param yamlConfig
     * @return
     */
    static std::vector<E2EBenchmarkConfigPerRun> generateAllConfigsPerRun(Yaml::Node yamlConfig);

    /**
     * @brief Creates string representation of the number of physical sources
     * @return string representation
     */
    std::string getStringLogicalSourceToNumberOfPhysicalSources() const;

    /**
     * @brief Creates a vector of maps for the number of physical sources for each logical source
     * @param yamlConfig
     * @return vector of created experiments
     */
    static std::vector<std::map<std::string, uint64_t>> generateMapsLogicalSrcToNumberOfPhysicalSources(Yaml::Node yamlConfig);

    Configurations::IntConfigOption numberOfWorkerThreads;
    std::map<std::string, uint64_t> logicalSrcToNoPhysicalSrc;
    Configurations::IntConfigOption bufferSizeInBytes;
    Configurations::IntConfigOption numberOfBuffersInGlobalBufferManager;
    Configurations::IntConfigOption numberOfBuffersInSourceLocalBufferPool;
    Configurations::IntConfigOption numberOfQueriesToDeploy;
    Configurations::IntConfigOption pageSize;
    Configurations::IntConfigOption preAllocPageCnt;
    Configurations::IntConfigOption numberOfPartitions;
    Configurations::LongConfigOption maxHashTableSize;
    Configurations::ConfigurationOptionPtr<QueryCompilation::QueryCompilerOptions::NautilusBackend> nautilusBackend;
};
}// namespace NES::Benchmark

#endif//NES_E2EBENCHMARKCONFIGPERRUN_HPP
