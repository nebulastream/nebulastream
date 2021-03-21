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

#include "../../include/util/E2EBenchmarkConfig.hpp"
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>
#include <thread>
using namespace NES;
E2EBenchmarkConfigPtr E2EBenchmarkConfig::create() { return std::make_shared<E2EBenchmarkConfig>(E2EBenchmarkConfig()); }

E2EBenchmarkConfig::E2EBenchmarkConfig() {
    NES_INFO("Generated new Benchmark Config object. Configurations initialized with default values.");

    numberOfWorkerThreads =
        ConfigOption<std::string>::create("numberOfWorkerThreads", "1", "Comma separated list of number of worker threads in the NES Worker.");
    numberOfCoordinatorThreads =
        ConfigOption<std::string>::create("numberOfCoordinatorThreads", "1", "Comma separated list of number of worker threads in the NES Coordinator.");
    numberOfSources = ConfigOption<std::string>::create("numberOfSources", "1", "Comma separated list of number of sources.");

    numberOfBuffersToProduce =
        ConfigOption<uint32_t>::create("numberOfBuffersToProduce", 5000000, "Number of buffers to produce.");
    bufferSizeInBytes = ConfigOption<uint32_t>::create("bufferSizeInBytes", 1024, "buffer size in bytes.");
    numberOfBuffersInGlobalBufferManager =
        ConfigOption<uint32_t>::create("numberOfBuffersInGlobalBufferManager", 1048576, "Number buffers in global buffer pool.");
    numberOfBuffersInTaskLocalBufferPool =
        ConfigOption<uint32_t>::create("numberOfBuffersInTaskLocalBufferPool", 1024, "Number buffers in task local buffer pool.");
    numberOfBuffersInSourceLocalBufferPool = ConfigOption<uint32_t>::create("numberOfBuffersInSourceLocalBufferPool", 1024,
                                                                            "Number buffers in source local buffer pool.");

    query = ConfigOption<std::string>::create("query", "127.0.0.1", "Query to be processed");
    inputOutputMode = ConfigOption<std::string>::create("inputOutputMode", "auto", "modus of how to read data");
    outputFile = ConfigOption<std::string>::create("outputFile", "E2EBenchmarkRunner", "name of the benchmark");
    benchmarkName = ConfigOption<std::string>::create("benchmarkName", "E2ERunner.csv", "benchmark output file");
    logLevel = ConfigOption<std::string>::create("logLevel", "LOG_NONE",
                                                 "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ");
}

void E2EBenchmarkConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {

        NES_INFO("NesE2EBenchmarkConfig: Using config file with path: " << filePath << " .");

        Yaml::Node config = *(new Yaml::Node());
        Yaml::Parse(config, filePath.c_str());
        try {
            setNumberOfWorkerThreads(config["numberOfWorkerThreads"].As<std::string>());
            setNumberOfCoordinatorThreads(config["numberOfCoordinatorThreads"].As<std::string>());
            setNumberOfBuffersToProduce(config["numberOfBuffersToProduce"].As<uint32_t>());
            setNumberOfBuffersInGlobalBufferManager(config["numberOfBuffersInGlobalBufferManager"].As<uint32_t>());
            setNumberOfBuffersInTaskLocalBufferPool(config["numberOfBuffersInTaskLocalBufferPool"].As<uint32_t>());
            setNumberOfBuffersInSourceLocalBufferPool(config["numberOfBuffersInSourceLocalBufferPool"].As<uint32_t>());
            setBufferSizeInBytes(config["bufferSizeInBytes"].As<uint32_t>());
            setNumberOfSources(config["numberOfSources"].As<std::string>());
            setOutputFile(config["inputOutputMode"].As<std::string>());
            setBenchmarkName(config["benchmarkName"].As<std::string>());
            setInputOutputMode(config["outputFile"].As<std::string>());
            setQuery(config["query"].As<std::string>());
            setLogLevel(config["logLevel"].As<std::string>());
        } catch (std::exception& e) {
            NES_ERROR("NesE2EBenchmarkConfig: Error while initializing configuration parameters from YAML file. Keeping default "
                      "values. "
                      << e.what());
            resetOptions();
        }
        return;
    }
    NES_ERROR("NesE2EBenchmarkConfig: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("Keeping default values for Worker Config.");
}

void E2EBenchmarkConfig::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    try {
        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            if (it->first == "--numberOfWorkerThreads") {
                setNumberOfWorkerThreads(it->second);
            } else if (it->first == "--numberOfCoordinatorThreads") {
                setNumberOfCoordinatorThreads(it->second);
            } else if (it->first == "--numberOfBuffersToProduce") {
                setNumberOfBuffersToProduce(stoi(it->second));
            } else if (it->first == "--numberOfBuffersInGlobalBufferManager") {
                setNumberOfBuffersInGlobalBufferManager(stoi(it->second));
            } else if (it->first == "--numberOfBuffersInTaskLocalBufferPool") {
                setNumberOfBuffersInTaskLocalBufferPool(stoi(it->second));
            } else if (it->first == "--numberOfBuffersInSourceLocalBufferPool") {
                setNumberOfBuffersInSourceLocalBufferPool(stoi(it->second));
            } else if (it->first == "--bufferSizeInBytes") {
                setBufferSizeInBytes(stoi(it->second));
            } else if (it->first == "--numberOfSources") {
                setNumberOfSources(it->second);
            } else if (it->first == "--inputOutputMode") {
                setInputOutputMode(it->second);
            } else if (it->first == "--query") {
                setQuery(it->second);
            } else if (it->first == "--outputFile") {
                setOutputFile(it->second);
            } else if (it->first == "--benchmarkName") {
                setBenchmarkName(it->second);
            } else if (it->first == "--logLevel") {
                setLogLevel(it->second);
            } else {
                NES_WARNING("Unknown configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("NesE2EBenchmarkConfig: Error while initializing configuration parameters from command line. Keeping default "
                  "values. "
                  << e.what());
        resetOptions();
    }
}

void E2EBenchmarkConfig::resetOptions() {
    setNumberOfWorkerThreads(numberOfWorkerThreads->getDefaultValue());
    setNumberOfCoordinatorThreads(numberOfCoordinatorThreads->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager->getDefaultValue());
    setNumberOfBuffersInTaskLocalBufferPool(numberOfBuffersInTaskLocalBufferPool->getDefaultValue());
    setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
    setBufferSizeInBytes(bufferSizeInBytes->getDefaultValue());
    setNumberOfSources(numberOfSources->getDefaultValue());
    setInputOutputMode(inputOutputMode->getDefaultValue());
    setQuery(query->getDefaultValue());
    setLogLevel(logLevel->getDefaultValue());
    setOutputFile(outputFile->getDefaultValue());
    setBenchmarkName(benchmarkName->getDefaultValue());
}

void E2EBenchmarkConfig::setNumberOfWorkerThreads(std::string numberOfWorkerThreads) {
    E2EBenchmarkConfig::numberOfWorkerThreads->setValue(numberOfWorkerThreads);
}
void E2EBenchmarkConfig::setNumberOfCoordinatorThreads(std::string numberOfCoordinatorThreads) {
    E2EBenchmarkConfig::numberOfCoordinatorThreads->setValue(numberOfCoordinatorThreads);
}
void E2EBenchmarkConfig::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce) {
    E2EBenchmarkConfig::numberOfBuffersToProduce->setValue(numberOfBuffersToProduce);
}
void E2EBenchmarkConfig::setNumberOfBuffersInGlobalBufferManager(uint32_t numberOfBuffersInGlobalBufferManager) {
    E2EBenchmarkConfig::numberOfBuffersInGlobalBufferManager->setValue(numberOfBuffersInGlobalBufferManager);
}
void E2EBenchmarkConfig::setNumberOfBuffersInTaskLocalBufferPool(uint32_t numberOfBuffersInTaskLocalBufferPool) {
    E2EBenchmarkConfig::numberOfBuffersInTaskLocalBufferPool->setValue(numberOfBuffersInTaskLocalBufferPool);
}
void E2EBenchmarkConfig::setNumberOfBuffersInSourceLocalBufferPool(uint32_t numberOfBuffersInSourceLocalBufferPool) {
    E2EBenchmarkConfig::numberOfBuffersInSourceLocalBufferPool->setValue(numberOfBuffersInSourceLocalBufferPool);
}
void E2EBenchmarkConfig::setBufferSizeInBytes(uint32_t bufferSizeInBytes) {
    E2EBenchmarkConfig::bufferSizeInBytes->setValue(bufferSizeInBytes);
}
void E2EBenchmarkConfig::setNumberOfSources(std::string numberOfSources) {
    E2EBenchmarkConfig::numberOfSources->setValue(numberOfSources);
}
void E2EBenchmarkConfig::setInputOutputMode(std::string inputOutputMode) {
    E2EBenchmarkConfig::inputOutputMode->setValue(inputOutputMode);
}
void E2EBenchmarkConfig::setQuery(std::string query) { E2EBenchmarkConfig::query->setValue(query); }
void E2EBenchmarkConfig::setLogLevel(std::string logLevel) { E2EBenchmarkConfig::logLevel->setValue(logLevel); }

const StringConfigOption E2EBenchmarkConfig::getNumberOfWorkerThreads() const { return numberOfWorkerThreads; }

const StringConfigOption E2EBenchmarkConfig::getNumberOfCoordinatorThreads() const { return numberOfCoordinatorThreads; }

const StringConfigOption E2EBenchmarkConfig::getNumberOfSources() const { return numberOfSources; }

const IntConfigOption E2EBenchmarkConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

const IntConfigOption E2EBenchmarkConfig::getNumberOfBuffersInGlobalBufferManager() const {
    return numberOfBuffersInGlobalBufferManager;
}

const IntConfigOption E2EBenchmarkConfig::getNumberOfBuffersInTaskLocalBufferPool() const {
    return numberOfBuffersInTaskLocalBufferPool;
}

const IntConfigOption E2EBenchmarkConfig::getNumberOfBuffersInSourceLocalBufferPool() const {
    return numberOfBuffersInSourceLocalBufferPool;
}

const IntConfigOption E2EBenchmarkConfig::getBufferSizeInBytes() const { return bufferSizeInBytes; }

StringConfigOption E2EBenchmarkConfig::getInputOutputMode() { return inputOutputMode; }

StringConfigOption E2EBenchmarkConfig::getQuery() { return query; }

StringConfigOption E2EBenchmarkConfig::getLogLevel() { return logLevel; }

StringConfigOption E2EBenchmarkConfig::getOutputFile() const { return outputFile; }

void E2EBenchmarkConfig::setOutputFile(std::string outputFile) { E2EBenchmarkConfig::outputFile->setValue(outputFile); }

StringConfigOption E2EBenchmarkConfig::getBenchmarkName() const { return benchmarkName; }

void E2EBenchmarkConfig::setBenchmarkName(std::string benchmarkName) {
    E2EBenchmarkConfig::benchmarkName->setValue(benchmarkName);
}
