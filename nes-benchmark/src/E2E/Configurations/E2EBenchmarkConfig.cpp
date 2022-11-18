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

#include <E2E/Configurations/E2EBenchmarkConfig.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES::Benchmark {

LogLevel Benchmark::E2EBenchmarkConfig::getLogLevel(const std::string& yamlConfigFile, LogLevel defaultLogLevel) {

    LogLevel retLogLevel = defaultLogLevel;
    try {
        Yaml::Node configFile;
        Yaml::Parse(configFile, yamlConfigFile.c_str());
        if (configFile.IsNone()) {
            std::cerr << "Error while reading the log level. Setting the loglevel to " << getLogName(defaultLogLevel)
                      << std::endl;
            return defaultLogLevel;
        }

        auto logLevelString = configFile["logLevel"].As<std::string>();
        auto logLevelMagicEnum = magic_enum::enum_cast<LogLevel>(logLevelString);
        if (logLevelMagicEnum.has_value()) {
            retLogLevel = logLevelMagicEnum.value();
        }
    } catch (std::exception& e) {
        std::cerr << "Error while reading the log level. Setting the loglevel to " << getLogName(defaultLogLevel) << std::endl;
        retLogLevel = defaultLogLevel;
    }

    std::cout << "Loglevel: " << getLogName(retLogLevel) << std::endl;
    return retLogLevel;
}

Benchmark::E2EBenchmarkConfig Benchmark::E2EBenchmarkConfig::createBenchmarks(const std::string& yamlConfigFile) {

    E2EBenchmarkConfig e2EBenchmarkConfig;

    try {
        Yaml::Node configFile;
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

std::string Benchmark::E2EBenchmarkConfig::toString() {
    std::stringstream oss;
    oss << "\n###############################################\n"
        << "Parameters over all Runs:\n"
        << configOverAllRuns.toString() << "\n";

    for (size_t experiment = 0; experiment < allConfigPerRuns.size(); ++experiment) {
        oss << "Experiment " << experiment << ":" << std::endl;
        oss << allConfigPerRuns[experiment].toString() << std::endl;
    }

    return oss.str();
}
}// namespace NES::Benchmark
