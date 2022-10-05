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

#include <E2E/E2EBenchmarkConfig.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/yaml/Yaml.hpp>

NES::LogLevel NES::Benchmark::E2EBenchmarkConfig::getLogLevel(const std::string& yamlConfigFile,
                                                              NES::LogLevel defaultLogLevel) {

    NES::LogLevel retLogLevel = defaultLogLevel;
    try {
        Yaml::Node configFile = *(new Yaml::Node());
        Yaml::Parse(configFile, yamlConfigFile.c_str());

        auto logLevelString = configFile["logLevel"].As<std::string>();
        auto logLevelMagicEnum = magic_enum::enum_cast<NES::LogLevel>(logLevelString);
        if (logLevelMagicEnum.has_value()) {
            retLogLevel = logLevelMagicEnum.value();
        }
    } catch (std::exception& e) {
        std::cerr << "Error while reading the log level. Setting the loglevel to" << NES::getLogName(defaultLogLevel) << std::endl;
        retLogLevel = defaultLogLevel;
    }

    std::cout << "Loglevel: " << NES::getLogName(retLogLevel) << std::endl;
    return retLogLevel;
}

NES::Benchmark::E2EBenchmarkConfig NES::Benchmark::E2EBenchmarkConfig::createBenchmarks(const std::string& yamlConfigFile) {

    E2EBenchmarkConfig e2EBenchmarkConfig;

    try {
        Yaml::Node configFile = *(new Yaml::Node());
        Yaml::Parse(configFile, yamlConfigFile.c_str());

        NES_INFO("Generating configOverAllRuns...");
        auto configOverAllRuns = E2EBenchmarkConfigOverAllRuns::generateConfigOverAllRuns(configFile);

        NES_INFO("Generating configsPerRun...");
        auto allConfigPerRuns = E2EBenchmarkConfigPerRun::generateAllConfigsPerRun(configFile);

        e2EBenchmarkConfig.configOverAllRuns = configOverAllRuns;
        e2EBenchmarkConfig.allConfigPerRuns = allConfigPerRuns;

    } catch (std::exception& e) {
        std::cerr << "Error while trying to create the benchmarks" << std::endl;
        throw;
    }

    return e2EBenchmarkConfig;
}

std::string NES::Benchmark::E2EBenchmarkConfig::toString() {
    std::stringstream oss;
    oss << "\n###############################################\n"
        << "Parameters over all Runs:\n" << configOverAllRuns.toString() << "\n";

    for (size_t experiment = 0; experiment < allConfigPerRuns.size(); ++experiment) {
        oss << "Experiment " << experiment << ":" << std::endl;
        oss << allConfigPerRuns[experiment].toString() << std::endl;
    }

    return oss.str();
}

