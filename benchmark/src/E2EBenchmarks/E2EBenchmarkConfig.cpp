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

#include "E2EBenchmarks/E2EBenchmarkConfig.hpp"
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>
#include <thread>
using namespace NES;
E2EBenchmarkConfigPtr E2EBenchmarkConfig::create() { return std::make_shared<E2EBenchmarkConfig>(E2EBenchmarkConfig()); }

E2EBenchmarkConfig::E2EBenchmarkConfig() {
    NES_INFO("Generated new Benchmark Config object. Configurations initialized with default values.");

    numberOfWorkerThreads = ConfigOption<std::string>::create(
        "numberOfWorkerThreads", "1", "Comma separated list of number of worker threads in the NES Worker.");
    numberOfSources = ConfigOption<std::string>::create("numberOfSources", "1", "Comma separated list of number of sources.");

    numberOfBuffersToProduce =
        ConfigOption<uint32_t>::create("numberOfBuffersToProduce", 5000000, "Number of buffers to produce.");

    bufferSizeInBytes = ConfigOption<std::string>::create("bufferSizeInBytes", "1024", "buffer size in bytes.");
    numberOfBuffersInGlobalBufferManager = ConfigOption<std::string>::create("numberOfBuffersInGlobalBufferManager", "10000",
                                                                             "Number buffers in global buffer pool.");
    numberOfBuffersPerPipeline =
        ConfigOption<std::string>::create("numberOfBuffersPerPipeline", "1024", "Number buffers in pipeline local buffer pool.");
    numberOfBuffersInSourceLocalBufferPool = ConfigOption<std::string>::create("numberOfBuffersInSourceLocalBufferPool", "1024",
                                                                               "Number buffers in source local buffer pool.");

    query = ConfigOption<std::string>::create("query", "", "Query to be processed");
    inputOutputMode = ConfigOption<std::string>::create("inputOutputMode", "Auto", "modus of how to read data");
    outputFile = ConfigOption<std::string>::create("outputFile", "E2EBenchmarkRunner", "name of the benchmark");
    benchmarkName = ConfigOption<std::string>::create("benchmarkName", "E2ERunner", "benchmark output file");
    scalability = ConfigOption<std::string>::create("scalability", "scale-up", "scale-out or scale-up");
    logLevel = ConfigOption<std::string>::create(
        "logLevel", "LOG_NONE", "Log level (LOG_NONE, LOG_FATAL, LOG_ERROR, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ");

    experimentMeasureIntervalInSeconds =
        ConfigOption<uint32_t>::create("experimentMeasureIntervalInSeconds", 1, "measuring duration of one sample");
    startupSleepIntervalInSeconds = ConfigOption<uint32_t>::create("startupSleepIntervalInSeconds", 3,
                                                                   "Sleep until the benchmark starts after query submission");
    numberOfMeasurementsToCollect =
        ConfigOption<uint32_t>::create("numberOfMeasurementsToCollect", 5, "Number of measurements taken before terminate");
}

void E2EBenchmarkConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("NesE2EBenchmarkConfig: Using config file with path: " << filePath << " .");
        Yaml::Node config = *(new Yaml::Node());
        Yaml::Parse(config, filePath.c_str());
        try {
            setNumberOfWorkerThreads(config["numberOfWorkerThreads"].As<std::string>());
            setNumberOfBuffersToProduce(config["numberOfBuffersToProduce"].As<uint32_t>());
            setNumberOfBuffersInGlobalBufferManager(config["numberOfBuffersInGlobalBufferManager"].As<std::string>());
            setNumberOfBuffersPerPipeline(config["numberOfBuffersPerPipeline"].As<std::string>());
            setNumberOfBuffersInSourceLocalBufferPool(config["numberOfBuffersInSourceLocalBufferPool"].As<std::string>());
            setBufferSizeInBytes(config["bufferSizeInBytes"].As<std::string>());
            setNumberOfSources(config["numberOfSources"].As<std::string>());
            setOutputFile(config["outputFile"].As<std::string>());
            setBenchmarkName(config["benchmarkName"].As<std::string>());
            setScalability(config["scalability"].As<std::string>());
            setInputOutputMode(config["inputOutputMode"].As<std::string>());
            setQuery(config["query"].As<std::string>());
            setLogLevel(config["logLevel"].As<std::string>());
            setExperimentMeasureIntervalInSeconds(config["experimentMeasureIntervalInSeconds"].As<uint32_t>());
            setStartupSleepIntervalInSeconds(config["startupSleepIntervalInSeconds"].As<uint32_t>());
            setNumberOfMeasurementsToCollect(config["numberOfMeasurementsToCollect"].As<uint32_t>());
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
            } else if (it->first == "--numberOfBuffersToProduce") {
                setNumberOfBuffersToProduce(stoi(it->second));
            } else if (it->first == "--numberOfBuffersInGlobalBufferManager") {
                setNumberOfBuffersInGlobalBufferManager(it->second);
            } else if (it->first == "--numberOfBuffersPerPipeline") {
                setNumberOfBuffersPerPipeline(it->second);
            } else if (it->first == "--numberOfBuffersInSourceLocalBufferPool") {
                setNumberOfBuffersInSourceLocalBufferPool(it->second);
            } else if (it->first == "--bufferSizeInBytes") {
                setBufferSizeInBytes(it->second);
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
            } else if (it->first == "--scalability") {
                setScalability(it->second);
            } else if (it->first == "--logLevel") {
                setLogLevel(it->second);
            } else if (it->first == "--experimentMeasureIntervalInSeconds") {
                setExperimentMeasureIntervalInSeconds(stoi(it->second));
            } else if (it->first == "--startupSleepIntervalInSeconds") {
                setStartupSleepIntervalInSeconds(stoi(it->second));
            } else if (it->first == "--numberOfMeasurementsToCollect") {
                setNumberOfMeasurementsToCollect(stoi(it->second));
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
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager->getDefaultValue());
    setNumberOfBuffersPerPipeline(numberOfBuffersPerPipeline->getDefaultValue());
    setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
    setBufferSizeInBytes(bufferSizeInBytes->getDefaultValue());
    setNumberOfSources(numberOfSources->getDefaultValue());
    setInputOutputMode(inputOutputMode->getDefaultValue());
    setQuery(query->getDefaultValue());
    setLogLevel(logLevel->getDefaultValue());
    setOutputFile(outputFile->getDefaultValue());
    setBenchmarkName(benchmarkName->getDefaultValue());
    setScalability(scalability->getDefaultValue());
    setExperimentMeasureIntervalInSeconds(experimentMeasureIntervalInSeconds->getDefaultValue());
    setStartupSleepIntervalInSeconds(startupSleepIntervalInSeconds->getDefaultValue());
    setNumberOfMeasurementsToCollect(numberOfMeasurementsToCollect->getDefaultValue());
}

void E2EBenchmarkConfig::setNumberOfWorkerThreads(std::string numberOfWorkerThreads) {
    E2EBenchmarkConfig::numberOfWorkerThreads->setValue(numberOfWorkerThreads);
}
void E2EBenchmarkConfig::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce) {
    E2EBenchmarkConfig::numberOfBuffersToProduce->setValue(numberOfBuffersToProduce);
}
void E2EBenchmarkConfig::setNumberOfBuffersInGlobalBufferManager(std::string numberOfBuffersInGlobalBufferManager) {
    E2EBenchmarkConfig::numberOfBuffersInGlobalBufferManager->setValue(numberOfBuffersInGlobalBufferManager);
}
void E2EBenchmarkConfig::setNumberOfBuffersPerPipeline(std::string numberOfBuffersPerPipeline) {
    E2EBenchmarkConfig::numberOfBuffersPerPipeline->setValue(numberOfBuffersPerPipeline);
}
void E2EBenchmarkConfig::setNumberOfBuffersInSourceLocalBufferPool(std::string numberOfBuffersInSourceLocalBufferPool) {
    E2EBenchmarkConfig::numberOfBuffersInSourceLocalBufferPool->setValue(numberOfBuffersInSourceLocalBufferPool);
}
void E2EBenchmarkConfig::setBufferSizeInBytes(std::string bufferSizeInBytes) {
    E2EBenchmarkConfig::bufferSizeInBytes->setValue(bufferSizeInBytes);
}
void E2EBenchmarkConfig::setNumberOfSources(std::string numberOfSources) {
    E2EBenchmarkConfig::numberOfSources->setValue(numberOfSources);
}
void E2EBenchmarkConfig::setInputOutputMode(std::string inputOutputMode) {
    E2EBenchmarkConfig::inputOutputMode->setValue(inputOutputMode);
}

void E2EBenchmarkConfig::setScalability(std::string scalability) { E2EBenchmarkConfig::scalability->setValue(scalability); }

void E2EBenchmarkConfig::setQuery(std::string query) { E2EBenchmarkConfig::query->setValue(query); }
void E2EBenchmarkConfig::setLogLevel(std::string logLevel) { E2EBenchmarkConfig::logLevel->setValue(logLevel); }

const StringConfigOption E2EBenchmarkConfig::getNumberOfWorkerThreads() const { return numberOfWorkerThreads; }

const StringConfigOption E2EBenchmarkConfig::getNumberOfSources() const { return numberOfSources; }

const IntConfigOption E2EBenchmarkConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

const StringConfigOption E2EBenchmarkConfig::getNumberOfBuffersInGlobalBufferManager() const {
    return numberOfBuffersInGlobalBufferManager;
}

const StringConfigOption E2EBenchmarkConfig::getNumberOfBuffersPerPipeline() const { return numberOfBuffersPerPipeline; }

const StringConfigOption E2EBenchmarkConfig::getNumberOfBuffersInSourceLocalBufferPool() const {
    return numberOfBuffersInSourceLocalBufferPool;
}

const StringConfigOption E2EBenchmarkConfig::getBufferSizeInBytes() const { return bufferSizeInBytes; }

StringConfigOption E2EBenchmarkConfig::getInputOutputMode() { return inputOutputMode; }

StringConfigOption E2EBenchmarkConfig::getScalability() { return scalability; }

StringConfigOption E2EBenchmarkConfig::getQuery() { return query; }

StringConfigOption E2EBenchmarkConfig::getLogLevel() { return logLevel; }

StringConfigOption E2EBenchmarkConfig::getOutputFile() const { return outputFile; }

void E2EBenchmarkConfig::setOutputFile(std::string outputFile) { E2EBenchmarkConfig::outputFile->setValue(outputFile); }

StringConfigOption E2EBenchmarkConfig::getBenchmarkName() const { return benchmarkName; }

void E2EBenchmarkConfig::setBenchmarkName(std::string benchmarkName) {
    E2EBenchmarkConfig::benchmarkName->setValue(benchmarkName);
}
const IntConfigOption E2EBenchmarkConfig::getExperimentMeasureIntervalInSeconds() const {
    return experimentMeasureIntervalInSeconds;
}
void E2EBenchmarkConfig::setExperimentMeasureIntervalInSeconds(uint32_t experimentMeasureIntervalInSeconds) {
    E2EBenchmarkConfig::experimentMeasureIntervalInSeconds->setValue(experimentMeasureIntervalInSeconds);
}
const IntConfigOption E2EBenchmarkConfig::getStartupSleepIntervalInSeconds() const { return startupSleepIntervalInSeconds; }
void E2EBenchmarkConfig::setStartupSleepIntervalInSeconds(uint32_t startupSleepIntervalInSeconds) {
    E2EBenchmarkConfig::startupSleepIntervalInSeconds->setValue(startupSleepIntervalInSeconds);
}
const IntConfigOption E2EBenchmarkConfig::getNumberOfMeasurementsToCollect() const { return numberOfMeasurementsToCollect; }
void E2EBenchmarkConfig::setNumberOfMeasurementsToCollect(uint32_t numberOfMeasurementsToCollect) {
    E2EBenchmarkConfig::numberOfMeasurementsToCollect->setValue(numberOfMeasurementsToCollect);
}

std::string E2EBenchmarkConfig::toString() {
    std::stringstream ss;
    ss << " numberOfWorkerThreads=" << getNumberOfWorkerThreads()->getValue()
       << " numberOfBuffersToProduce=" << getNumberOfBuffersToProduce()->getValue()
       << " numberOfSources=" << getNumberOfSources()->getValue()
       << " numberOfBuffersInGlobalBufferManager=" << getNumberOfBuffersInGlobalBufferManager()->getValue()
       << " numberOfBuffersPerPipeline=" << getNumberOfBuffersPerPipeline()->getValue()
       << " numberOfBuffersInSourceLocalBufferPool=" << getNumberOfBuffersInSourceLocalBufferPool()->getValue()
       << " bufferSizeInBytes=" << getBufferSizeInBytes()->getValue() << " inputOutputMode=" << getInputOutputMode()->getValue()
       << " outputFile=" << getOutputFile()->getValue() << " query=" << getQuery()->getValue()
       << " experimentMeasureIntervalInSeconds=" << getExperimentMeasureIntervalInSeconds()->getValue()
       << " logLevel=" << getLogLevel()->getValue() << " inputOutputMode=" << getInputOutputMode()->getValue()
       << " startupSleepIntervalInSeconds=" << getStartupSleepIntervalInSeconds()->getValue()
       << " numberOfMeasurementsToCollect=" << getNumberOfMeasurementsToCollect()->getValue() << std::endl;
    return ss.str();
}