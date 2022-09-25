/*
Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <E2EBenchmarkConfig.hpp>
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

        generateConfigOverAllRuns(e2EBenchmarkConfig, configFile);
        generateAllConfigsPerRun(e2EBenchmarkConfig, configFile);

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
void NES::Benchmark::E2EBenchmarkConfig::generateAllConfigsPerRun(E2EBenchmarkConfig& e2EBenchmarkConfig,
                                                                  Yaml::Node yamlConfig) {
    /* Getting all parameters per experiment run in vectors */
    auto numWorkerThreads = Util::splitWithStringDelimiter<uint32_t>(yamlConfig["numberOfWorkerThreads"].As<std::string>(), ",");
    auto numSources = Util::splitWithStringDelimiter<uint32_t>(yamlConfig["numberOfSources"].As<std::string>(), ",");
    auto numBuffersToProduce = Util::splitWithStringDelimiter<uint32_t>(yamlConfig["numberOfBuffersToProduce"].As<std::string>(), ",");
    auto bufferSizeInBytes = Util::splitWithStringDelimiter<uint32_t>(yamlConfig["bufferSizeInBytes"].As<std::string>(), ",");

    size_t totalBenchmarkRuns = numWorkerThreads.size();
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numSources.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numBuffersToProduce.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, bufferSizeInBytes.size());

    Util::padVectorToSize<uint32_t>(numWorkerThreads, totalBenchmarkRuns, numWorkerThreads.back());
    Util::padVectorToSize<uint32_t>(numSources, totalBenchmarkRuns, numSources.back());
    Util::padVectorToSize<uint32_t>(numBuffersToProduce, totalBenchmarkRuns, numBuffersToProduce.back());
    Util::padVectorToSize<uint32_t>(bufferSizeInBytes, totalBenchmarkRuns, bufferSizeInBytes.back());

    e2EBenchmarkConfig.allConfigPerRuns.reserve(totalBenchmarkRuns);

    for (size_t i = 0; i < numBuffersToProduce.size(); ++i) {
        E2EBenchmarkConfigPerRun e2EBenchmarkConfigPerRun;
        e2EBenchmarkConfigPerRun.numWorkerThreads->setValue(numWorkerThreads[i]);
        e2EBenchmarkConfigPerRun.numBuffersToProduce->setValue(numBuffersToProduce[i]);
        e2EBenchmarkConfigPerRun.numSources->setValue(numSources[i]);
        e2EBenchmarkConfigPerRun.bufferSizeInBytes->setValue(bufferSizeInBytes[i]);

        e2EBenchmarkConfig.allConfigPerRuns.push_back(e2EBenchmarkConfigPerRun);
    }
}
void NES::Benchmark::E2EBenchmarkConfig::generateConfigOverAllRuns(E2EBenchmarkConfig& e2EBenchmarkConfig,
                                                                   Yaml::Node yamlConfig) {
    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
    configOverAllRuns.startupSleepIntervalInSeconds->setValue(yamlConfig["startupSleepIntervalInSeconds"].As<uint32_t>());
    configOverAllRuns.numMeasurementsToCollect->setValue(yamlConfig["numMeasurementsToCollect"].As<uint32_t>());
    configOverAllRuns.experimentMeasureIntervalInSeconds->setValue(yamlConfig["experimentMeasureIntervalInSeconds"].As<uint32_t>());
    configOverAllRuns.outputFile->setValue(yamlConfig["outputFile"].As<std::string>());
    configOverAllRuns.benchmarkName->setValue(yamlConfig["benchmarkName"].As<std::string>());
    configOverAllRuns.query->setValue(yamlConfig["query"].As<std::string>());

    e2EBenchmarkConfig.configOverAllRuns = configOverAllRuns;
}
