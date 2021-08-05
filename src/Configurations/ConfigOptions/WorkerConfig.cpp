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

#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>
#include <thread>
#include <utility>

namespace NES {

WorkerConfigPtr WorkerConfig::create() { return std::make_shared<WorkerConfig>(); }

WorkerConfig::WorkerConfig() {
    NES_INFO("Generated new Worker Config object. Configurations initialized with default values.");
    localWorkerIp = ConfigOption<std::string>::create("localWorkerIp", "127.0.0.1", "Worker IP.");
    coordinatorIp = ConfigOption<std::string>::create("coordinatorIp",
                                                      "127.0.0.1",
                                                      "Server IP of the NES Coordinator to which the NES Worker should connect.");
    coordinatorPort = ConfigOption<uint32_t>::create(
        "coordinatorPort",
        4000,
        "RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs "
        "to be the same as rpcPort in Coordinator.");
    rpcPort = ConfigOption<uint32_t>::create("rpcPort", 4000, "RPC server port of the NES Worker.");
    dataPort = ConfigOption<uint32_t>::create("dataPort", 4001, "Data port of the NES Worker.");
    numberOfSlots = ConfigOption<uint32_t>::create("numberOfSlots", UINT16_MAX, "Number of computing slots for the NES Worker.");
    numWorkerThreads = ConfigOption<uint32_t>::create("numWorkerThreads", 1, "Number of worker threads.");

    numberOfBuffersInGlobalBufferManager =
        ConfigOption<uint32_t>::create("numberOfBuffersInGlobalBufferManager", 1024, "Number buffers in global buffer pool.");
    numberOfBuffersPerPipeline =
        ConfigOption<uint32_t>::create("numberOfBuffersPerPipeline", 128, "Number buffers in task local buffer pool.");
    numberOfBuffersInSourceLocalBufferPool = ConfigOption<uint32_t>::create("numberOfBuffersInSourceLocalBufferPool",
                                                                            64,
                                                                            "Number buffers in source local buffer pool.");
    bufferSizeInBytes = ConfigOption<uint32_t>::create("bufferSizeInBytes", 4096, "BufferSizeInBytes.");
    parentId = ConfigOption<std::string>::create("parentId", "-1", "Parent ID of this node.");
    logLevel = ConfigOption<std::string>::create("logLevel",
                                                 "LOG_DEBUG",
                                                 "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ");

    queryCompilerExecutionMode =
        ConfigOption<std::string>::create("queryCompilerExecutionMode",
                                          "RELEASE",
                                          "Indicates the execution mode for the query compiler [DEBUG|RELEASE]. ");

    queryCompilerOutputBufferOptimizationLevel =
        ConfigOption<std::string>::create("OutputBufferOptimizationLevel",
                                          "ALL",
                                          "Indicates the OutputBufferAllocationStrategy "
                                          "[ALL|NO|ONLY_INPLACE_OPERATIONS_NO_FALLBACK,"
                                          "|REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK,|"
                                          "REUSE_INPUT_BUFFER_NO_FALLBACK|OMIT_OVERFLOW_CHECK_NO_FALLBACK]. ");

    sourcePinList = ConfigOption<std::string>::create("sourcePinList", "", "comma separated list of where to map the sources");

    workerPinList = ConfigOption<std::string>::create("workerPinList", "", "comma separated list of where to map the worker");

    numaAwareness = ConfigOption<bool>::create("numaAwareness", false, "Enable Numa-Aware execution");
}

void WorkerConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {

        NES_INFO("NesWorkerConfig: Using config file with path: " << filePath << " .");

        Yaml::Node config = *(new Yaml::Node());
        Yaml::Parse(config, filePath.c_str());
        try {
            if (!config["coordinatorPort"].As<std::string>().empty() && config["coordinatorPort"].As<std::string>() != "\n") {
                setCoordinatorPort(config["coordinatorPort"].As<uint16_t>());
            }
            if (!config["rpcPort"].As<std::string>().empty() && config["rpcPort"].As<std::string>() != "\n") {
                setRpcPort(config["rpcPort"].As<uint16_t>());
            }
            if (!config["dataPort"].As<std::string>().empty() && config["dataPort"].As<std::string>() != "\n") {
                setDataPort(config["dataPort"].As<uint16_t>());
            }
            if (!config["localWorkerIp"].As<std::string>().empty() && config["localWorkerIp"].As<std::string>() != "\n") {
                setLocalWorkerIp(config["localWorkerIp"].As<std::string>());
            }
            if (!config["coordinatorIp"].As<std::string>().empty() && config["coordinatorIp"].As<std::string>() != "\n") {
                setCoordinatorIp(config["coordinatorIp"].As<std::string>());
            }
            if (!config["numberOfSlots"].As<std::string>().empty() && config["numberOfSlots"].As<std::string>() != "\n") {
                setNumberOfSlots(config["numberOfSlots"].As<uint16_t>());
            }
            if (!config["numWorkerThreads"].As<std::string>().empty() && config["numWorkerThreads"].As<std::string>() != "\n") {
                setNumWorkerThreads(config["numWorkerThreads"].As<uint32_t>());
            }
            if (!config["parentId"].As<std::string>().empty() && config["parentId"].As<std::string>() != "\n") {
                setParentId(config["parentId"].As<std::string>());
            }
            if (!config["logLevel"].As<std::string>().empty() && config["logLevel"].As<std::string>() != "\n") {
                setLogLevel(config["logLevel"].As<std::string>());
            }
            if (!config["bufferSizeInBytes"].As<std::string>().empty() && config["bufferSizeInBytes"].As<std::string>() != "\n") {
                setBufferSizeInBytes(config["bufferSizeInBytes"].As<uint32_t>());
            }
            if (!config["numberOfBuffersInGlobalBufferManager"].As<std::string>().empty()
                && config["numberOfBuffersInGlobalBufferManager"].As<std::string>() != "\n") {
                setNumberOfBuffersInGlobalBufferManager(config["numberOfBuffersInGlobalBufferManager"].As<uint32_t>());
            }
            if (!config["numberOfBuffersPerPipeline"].As<std::string>().empty()
                && config["numberOfBuffersPerPipeline"].As<std::string>() != "\n") {
                setNumberOfBuffersPerPipeline(config["numberOfBuffersPerPipeline"].As<uint32_t>());
            }
            if (!config["numberOfBuffersInSourceLocalBufferPool"].As<std::string>().empty()
                && config["numberOfBuffersInSourceLocalBufferPool"].As<std::string>() != "\n") {
                setNumberOfBuffersInSourceLocalBufferPool(config["numberOfBuffersInSourceLocalBufferPool"].As<uint32_t>());
            }
            if (!config["sourcePinList"].As<std::string>().empty() && config["sourcePinList"].As<std::string>() != "\n") {
                setSourcePinList(config["sourcePinList"].As<std::string>());
            }
            if (!config["workerPinList"].As<std::string>().empty() && config["workerPinList"].As<std::string>() != "\n") {
                setWorkerPinList(config["setWorkerPinList"].As<std::string>());
            }
            if (!config["queryCompilerExecutionMode"].As<std::string>().empty()
                && config["queryCompilerExecutionMode"].As<std::string>() != "\n") {
                setQueryCompilerExecutionMode(config["queryCompilerExecutionMode"].As<std::string>());
            }
            if (!config["queryCompilerOutputBufferOptimizationLevel"].As<std::string>().empty()
                && config["queryCompilerOutputBufferOptimizationLevel"].As<std::string>() != "\n") {
                setQueryCompilerOutputBufferAllocationStrategy(
                    config["queryCompilerOutputBufferOptimizationLevel"].As<std::string>());
            }
            if (!config["numaAwareness"].As<bool>()) {
                numaAwareness->setValue(false);
            } else {
                numaAwareness->setValue(true);
            }
        } catch (std::exception& e) {
            NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from YAML file. Keeping default "
                      "values. "
                      << e.what());
            resetWorkerOptions();
        }
        return;
    }
    NES_ERROR("NesWorkerConfig: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("Keeping default values for Worker Config.");
}

void WorkerConfig::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    try {

        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            if (it->first == "--localWorkerIp" && !it->second.empty()) {
                setLocalWorkerIp(it->second);
            } else if (it->first == "--coordinatorIp" && !it->second.empty()) {
                setCoordinatorIp(it->second);
            } else if (it->first == "--rpcPort" && !it->second.empty()) {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--coordinatorPort" && !it->second.empty()) {
                setCoordinatorPort(stoi(it->second));
            } else if (it->first == "--dataPort" && !it->second.empty()) {
                setDataPort(stoi(it->second));
            } else if (it->first == "--numberOfSlots" && !it->second.empty()) {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--numWorkerThreads" && !it->second.empty()) {
                setNumWorkerThreads(stoi(it->second));
            } else if (it->first == "--numberOfBuffersInGlobalBufferManager" && !it->second.empty()) {
                setNumberOfBuffersInGlobalBufferManager(stoi(it->second));
            } else if (it->first == "--numberOfBuffersPerPipeline" && !it->second.empty()) {
                setNumberOfBuffersPerPipeline(stoi(it->second));
            } else if (it->first == "--numberOfBuffersInSourceLocalBufferPool" && !it->second.empty()) {
                setNumberOfBuffersInSourceLocalBufferPool(stoi(it->second));
            } else if (it->first == "--bufferSizeInBytes" && !it->second.empty()) {
                setBufferSizeInBytes(stoi(it->second));
            } else if (it->first == "--parentId" && !it->second.empty()) {
                setParentId(it->second);
            } else if (it->first == "--queryCompilerExecutionMode" && !it->second.empty()) {
                setQueryCompilerExecutionMode(it->second);
            } else if (it->first == "--queryCompilerOutputBufferOptimizationLevel" && !it->second.empty()) {
                setQueryCompilerOutputBufferAllocationStrategy(it->second);
            } else if (it->first == "--sourcePinList") {
                setSourcePinList(it->second);
            } else if (it->first == "--workerPinList") {
                setWorkerPinList(it->second);
            } else if (it->first == "--numaAwareness") {
                setNumaAware(true);
            } else {
                NES_WARNING("Unknow configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from command line. Keeping default "
                  "values. "
                  << e.what());
        resetWorkerOptions();
    }
}

void WorkerConfig::resetWorkerOptions() {
    setLocalWorkerIp(localWorkerIp->getDefaultValue());
    setCoordinatorIp(coordinatorIp->getDefaultValue());
    setCoordinatorPort(coordinatorPort->getDefaultValue());
    setRpcPort(rpcPort->getDefaultValue());
    setDataPort(dataPort->getDefaultValue());
    setNumberOfSlots(numberOfSlots->getDefaultValue());
    setNumWorkerThreads(numWorkerThreads->getDefaultValue());
    setParentId(parentId->getDefaultValue());
    setLogLevel(logLevel->getDefaultValue());
    setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager->getDefaultValue());
    setNumberOfBuffersPerPipeline(numberOfBuffersPerPipeline->getDefaultValue());
    setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
    setQueryCompilerExecutionMode(queryCompilerExecutionMode->getDefaultValue());
    setQueryCompilerOutputBufferAllocationStrategy(queryCompilerOutputBufferOptimizationLevel->getDefaultValue());
    setWorkerPinList(workerPinList->getDefaultValue());
    setSourcePinList(sourcePinList->getDefaultValue());
}

StringConfigOption WorkerConfig::getLocalWorkerIp() { return localWorkerIp; }

void WorkerConfig::setLocalWorkerIp(std::string localWorkerIpValue) { localWorkerIp->setValue(std::move(localWorkerIpValue)); }

StringConfigOption WorkerConfig::getCoordinatorIp() { return coordinatorIp; }

void WorkerConfig::setCoordinatorIp(std::string coordinatorIpValue) { coordinatorIp->setValue(std::move(coordinatorIpValue)); }

IntConfigOption WorkerConfig::getCoordinatorPort() { return coordinatorPort; }

void WorkerConfig::setCoordinatorPort(uint16_t coordinatorPortValue) { coordinatorPort->setValue(coordinatorPortValue); }

IntConfigOption WorkerConfig::getRpcPort() { return rpcPort; }

void WorkerConfig::setRpcPort(uint16_t rpcPortValue) { rpcPort->setValue(rpcPortValue); }

IntConfigOption WorkerConfig::getDataPort() { return dataPort; }

void WorkerConfig::setDataPort(uint16_t dataPortValue) { dataPort->setValue(dataPortValue); }

IntConfigOption WorkerConfig::getNumberOfSlots() { return numberOfSlots; }

void WorkerConfig::setNumberOfSlots(uint16_t numberOfSlotsValue) { numberOfSlots->setValue(numberOfSlotsValue); }

IntConfigOption WorkerConfig::getNumWorkerThreads() { return numWorkerThreads; }

void WorkerConfig::setNumWorkerThreads(uint16_t numWorkerThreadsValue) { numWorkerThreads->setValue(numWorkerThreadsValue); }

StringConfigOption WorkerConfig::getParentId() { return parentId; }

void WorkerConfig::setParentId(std::string parentIdValue) { parentId->setValue(std::move(parentIdValue)); }

StringConfigOption WorkerConfig::getLogLevel() { return logLevel; }

void WorkerConfig::setLogLevel(std::string logLevelValue) { logLevel->setValue(std::move(logLevelValue)); }

IntConfigOption WorkerConfig::getNumberOfBuffersInGlobalBufferManager() { return numberOfBuffersInGlobalBufferManager; }
IntConfigOption WorkerConfig::getNumberOfBuffersPerPipeline() { return numberOfBuffersPerPipeline; }
IntConfigOption WorkerConfig::getNumberOfBuffersInSourceLocalBufferPool() { return numberOfBuffersInSourceLocalBufferPool; }

void WorkerConfig::setNumberOfBuffersInGlobalBufferManager(uint64_t count) {
    numberOfBuffersInGlobalBufferManager->setValue(count);
}
void WorkerConfig::setNumberOfBuffersPerPipeline(uint64_t count) { numberOfBuffersPerPipeline->setValue(count); }
void WorkerConfig::setNumberOfBuffersInSourceLocalBufferPool(uint64_t count) {
    numberOfBuffersInSourceLocalBufferPool->setValue(count);
}

IntConfigOption WorkerConfig::getBufferSizeInBytes() { return bufferSizeInBytes; }

void WorkerConfig::setBufferSizeInBytes(uint64_t sizeInBytes) { bufferSizeInBytes->setValue(sizeInBytes); }

const StringConfigOption WorkerConfig::getQueryCompilerExecutionMode() const { return queryCompilerExecutionMode; }

void WorkerConfig::setQueryCompilerExecutionMode(std::string queryCompilerExecutionMode) {
    this->queryCompilerExecutionMode->setValue(std::move(queryCompilerExecutionMode));
}
const StringConfigOption WorkerConfig::getQueryCompilerOutputBufferAllocationStrategy() const {
    return queryCompilerOutputBufferOptimizationLevel;
}
void WorkerConfig::setQueryCompilerOutputBufferAllocationStrategy(std::string queryCompilerOutputBufferAllocationStrategy) {
    this->queryCompilerOutputBufferOptimizationLevel->setValue(std::move(queryCompilerOutputBufferAllocationStrategy));
}

const StringConfigOption& WorkerConfig::getWorkerPinList() const { return workerPinList; }

const StringConfigOption& WorkerConfig::getSourcePinList() const { return sourcePinList; }

void WorkerConfig::setWorkerPinList(const std::string list) {
    if (!list.empty()) {
        WorkerConfig::workerPinList->setValue(list);
    }
}

void WorkerConfig::setSourcePinList(const std::string list) {
    if (!list.empty()) {
        WorkerConfig::sourcePinList->setValue(list);
    }
}

bool WorkerConfig::isNumaAware() const { return numaAwareness->getValue(); }

void WorkerConfig::setNumaAware(bool status) { numaAwareness->setValue(status); }

}// namespace NES