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

#include "Util/yaml/Yaml.hpp"
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/yaml/rapidyaml.hpp>
#include <filesystem>
#include <string>
#include <thread>
#include <utility>

namespace NES {

namespace Configurations {

WorkerConfigurationPtr WorkerConfiguration::create() { return std::make_shared<WorkerConfiguration>(); }

WorkerConfiguration::WorkerConfiguration() : physicalSources() {
    NES_INFO("Generated new Worker Config object. Configurations initialized with default values.");
    localWorkerIp = ConfigurationOption<std::string>::create("localWorkerIp", "127.0.0.1", "Worker IP.");
    coordinatorIp =
        ConfigurationOption<std::string>::create("coordinatorIp",
                                                 "127.0.0.1",
                                                 "Server IP of the NES Coordinator to which the NES Worker should connect.");
    coordinatorPort = ConfigurationOption<uint32_t>::create(
        "coordinatorPort",
        4000,
        "RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs "
        "to be the same as rpcPort in Coordinator.");
    rpcPort = ConfigurationOption<uint32_t>::create("rpcPort", 4000, "RPC server port of the NES Worker.");
    dataPort = ConfigurationOption<uint32_t>::create("dataPort", 4001, "Data port of the NES Worker.");
    numberOfSlots =
        ConfigurationOption<uint32_t>::create("numberOfSlots", UINT16_MAX, "Number of computing slots for the NES Worker.");
    numWorkerThreads = ConfigurationOption<uint32_t>::create("numWorkerThreads", 1, "Number of worker threads.");

    numberOfBuffersInGlobalBufferManager = ConfigurationOption<uint32_t>::create("numberOfBuffersInGlobalBufferManager",
                                                                                 1024,
                                                                                 "Number buffers in global buffer pool.");
    numberOfBuffersPerWorker =
        ConfigurationOption<uint32_t>::create("numberOfBuffersPerWorker", 128, "Number buffers in task local buffer pool.");
    numberOfBuffersInSourceLocalBufferPool = ConfigurationOption<uint32_t>::create("numberOfBuffersInSourceLocalBufferPool",
                                                                                   64,
                                                                                   "Number buffers in source local buffer pool.");
    bufferSizeInBytes = ConfigurationOption<uint32_t>::create("bufferSizeInBytes", 4096, "BufferSizeInBytes.");
    parentId = ConfigurationOption<uint32_t>::create("parentId", 0, "Parent ID of this node.");
    logLevel = ConfigurationOption<std::string>::create("logLevel",
                                                        "LOG_DEBUG",
                                                        "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ");

    queryCompilerCompilationStrategy = ConfigurationOption<std::string>::create(
        "queryCompilerCompilationStrategy",
        "OPTIMIZE",
        "Indicates the optimization strategy for the query compiler [FAST|DEBUG|OPTIMIZE].");

    queryCompilerPipeliningStrategy = ConfigurationOption<std::string>::create(
        "queryCompilerPipeliningStrategy",
        "OPERATOR_FUSION",
        "Indicates the pipelining strategy for the query compiler [OPERATOR_FUSION|OPERATOR_AT_A_TIME].");

    queryCompilerOutputBufferOptimizationLevel =
        ConfigurationOption<std::string>::create("OutputBufferOptimizationLevel",
                                                 "ALL",
                                                 "Indicates the OutputBufferAllocationStrategy "
                                                 "[ALL|NO|ONLY_INPLACE_OPERATIONS_NO_FALLBACK,"
                                                 "|REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK,|"
                                                 "REUSE_INPUT_BUFFER_NO_FALLBACK|OMIT_OVERFLOW_CHECK_NO_FALLBACK]. ");

    sourcePinList =
        ConfigurationOption<std::string>::create("sourcePinList", "", "comma separated list of where to map the sources");

    workerPinList =
        ConfigurationOption<std::string>::create("workerPinList", "", "comma separated list of where to map the worker");

    numaAwareness = ConfigurationOption<bool>::create("numaAwareness", false, "Enable Numa-Aware execution");

    enableMonitoring = ConfigurationOption<bool>::create("enableMonitoring", false, "Enable monitoring");
}

void WorkerConfiguration::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("NesWorkerConfig: Using config file with path: " << filePath << " .");
        Yaml::Node config;
        Yaml::Parse(config, filePath.c_str());
        try {
            if (!config[LOCAL_WORKER_IP_CONFIG].As<std::string>().empty()
                && config[LOCAL_WORKER_IP_CONFIG].As<std::string>() != "\n") {
                setLocalWorkerIp(config[LOCAL_WORKER_IP_CONFIG].As<std::string>());
            }
            if (!config[COORDINATOR_IP_CONFIG].As<std::string>().empty()
                && config[COORDINATOR_IP_CONFIG].As<std::string>() != "\n") {
                setCoordinatorIp(config[COORDINATOR_IP_CONFIG].As<std::string>());
            }
            if (!config[COORDINATOR_PORT_CONFIG].As<std::string>().empty()
                && config[COORDINATOR_PORT_CONFIG].As<std::string>() != "\n") {
                setCoordinatorPort(std::stoi(config[COORDINATOR_PORT_CONFIG].As<std::string>()));
            }
            if (!config[RPC_PORT_CONFIG].As<std::string>().empty() && config[RPC_PORT_CONFIG].As<std::string>() != "\n") {
                setRpcPort(std::stoi(config[RPC_PORT_CONFIG].As<std::string>()));
            }
            if (!config[DATA_PORT_CONFIG].As<std::string>().empty() && config[DATA_PORT_CONFIG].As<std::string>() != "\n") {
                setDataPort(std::stoi(config[DATA_PORT_CONFIG].As<std::string>()));
            }
            if (!config[NUMBER_OF_SLOTS_CONFIG].As<std::string>().empty()
                && config[NUMBER_OF_SLOTS_CONFIG].As<std::string>() != "\n") {
                setNumberOfSlots(std::stoi(config[NUMBER_OF_SLOTS_CONFIG].As<std::string>()));
            }
            if (!config[NUM_WORKER_THREADS_CONFIG].As<std::string>().empty()
                && config[NUM_WORKER_THREADS_CONFIG].As<std::string>() != "\n") {
                setNumWorkerThreads(std::stoi(config[NUM_WORKER_THREADS_CONFIG].As<std::string>()));
            }
            if (!config[NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG].As<std::string>().empty()
                && config[NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG].As<std::string>() != "\n") {
                setNumberOfBuffersInGlobalBufferManager(
                    std::stoi(config[NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG].As<std::string>()));
            }
            if (!config[NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG].As<std::string>().empty()
                && config[NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG].As<std::string>() != "\n") {
                setNumberOfBuffersInSourceLocalBufferPool(
                    std::stoi(config[NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG].As<std::string>()));
            }
            if (!config[BUFFERS_SIZE_IN_BYTES_CONFIG].As<std::string>().empty()
                && config[BUFFERS_SIZE_IN_BYTES_CONFIG].As<std::string>() != "\n") {
                setBufferSizeInBytes(std::stoi(config[BUFFERS_SIZE_IN_BYTES_CONFIG].As<std::string>()));
            }
            if (!config[PARENT_ID_CONFIG].As<std::string>().empty() && config[PARENT_ID_CONFIG].As<std::string>() != "\n") {
                setParentId(std::stoi(config[PARENT_ID_CONFIG].As<std::string>()));
            }
            if (!config[LOG_LEVEL_CONFIG].As<std::string>().empty() && config[LOG_LEVEL_CONFIG].As<std::string>() != "\n") {
                setLogLevel(config[LOG_LEVEL_CONFIG].As<std::string>());
            }
            if (!config[QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG].As<std::string>().empty()
                && config[QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG].As<std::string>() != "\n") {
                setQueryCompilerCompilationStrategy(config[QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG].As<std::string>());
            }
            if (!config[QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG].As<std::string>().empty()
                && config[QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG].As<std::string>() != "\n") {
                setQueryCompilerPipeliningStrategy(config[QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG].As<std::string>());
            }
            if (!config[QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG].As<std::string>().empty()
                && config[QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG].As<std::string>() != "\n") {
                setQueryCompilerOutputBufferAllocationStrategy(
                    config[QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG].As<std::string>());
            }
            if (!config[SOURCE_PIN_LIST_CONFIG].As<std::string>().empty()
                && config[SOURCE_PIN_LIST_CONFIG].As<std::string>() != "\n") {
                setSourcePinList(config[SOURCE_PIN_LIST_CONFIG].As<std::string>());
            }
            if (!config[WORKER_PIN_LIST_CONFIG].As<std::string>().empty()
                && config[WORKER_PIN_LIST_CONFIG].As<std::string>() != "\n") {
                setWorkerPinList(config[WORKER_PIN_LIST_CONFIG].As<std::string>());
            }
            if (!config[NUMA_AWARENESS_CONFIG].As<std::string>().empty()
                && config[NUMA_AWARENESS_CONFIG].As<std::string>() != "\n") {
                setNumaAware(config[NUMA_AWARENESS_CONFIG].As<bool>());
            }
            if (!config[ENABLE_MONITORING_CONFIG].As<std::string>().empty()
                && config[ENABLE_MONITORING_CONFIG].As<std::string>() != "\n") {
                setEnableMonitoring(config[ENABLE_MONITORING_CONFIG].As<bool>());
            }
            if (config[PHYSICAL_SOURCES].IsSequence()) {
                auto physicalSources = config[PHYSICAL_SOURCES];
                setPhysicalSources(PhysicalSourceFactory::createPhysicalSources(physicalSources));
            }
        } catch (std::exception& e) {
            NES_THROW_RUNTIME_ERROR("NesWorkerConfig: Error while initializing configuration parameters from YAML file. "
                                    << e.what());
        }
        return;
    }
    NES_THROW_RUNTIME_ERROR("NesWorkerConfig: Either file could not be found at " << filePath << " or supplied empty file.");
}

void WorkerConfiguration::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& commandLineArguments) {
    try {

        for (auto it = commandLineArguments.begin(); it != commandLineArguments.end(); ++it) {
            if (it->first == "--" + LOCAL_WORKER_IP_CONFIG && !it->second.empty()) {
                setLocalWorkerIp(it->second);
            } else if (it->first == "--" + COORDINATOR_IP_CONFIG && !it->second.empty()) {
                setCoordinatorIp(it->second);
            } else if (it->first == "--" + RPC_PORT_CONFIG && !it->second.empty()) {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--" + COORDINATOR_PORT_CONFIG && !it->second.empty()) {
                setCoordinatorPort(stoi(it->second));
            } else if (it->first == "--" + DATA_PORT_CONFIG && !it->second.empty()) {
                setDataPort(stoi(it->second));
            } else if (it->first == "--" + NUMBER_OF_SLOTS_CONFIG && !it->second.empty()) {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--" + NUM_WORKER_THREADS_CONFIG && !it->second.empty()) {
                setNumWorkerThreads(stoi(it->second));
            } else if (it->first == "--" + NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG && !it->second.empty()) {
                setNumberOfBuffersInGlobalBufferManager(stoi(it->second));
            } else if (it->first == "--" + NUMBER_OF_BUFFERS_PER_WORKER_CONFIG && !it->second.empty()) {
                setNumberOfBuffersPerWorker(stoi(it->second));
            } else if (it->first == "--" + NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG && !it->second.empty()) {
                setNumberOfBuffersInSourceLocalBufferPool(stoi(it->second));
            } else if (it->first == "--" + BUFFERS_SIZE_IN_BYTES_CONFIG && !it->second.empty()) {
                setBufferSizeInBytes(stoi(it->second));
            } else if (it->first == "--" + PARENT_ID_CONFIG && !it->second.empty()) {
                setParentId(stoi(it->second));
            } else if (it->first == "--" + QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG && !it->second.empty()) {
                setQueryCompilerCompilationStrategy(it->second);
            } else if (it->first == "--" + QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG && !it->second.empty()) {
                setQueryCompilerPipeliningStrategy(it->second);
            } else if (it->first == "--" + QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG && !it->second.empty()) {
                setQueryCompilerOutputBufferAllocationStrategy(it->second);
            } else if (it->first == "--" + SOURCE_PIN_LIST_CONFIG) {
                setSourcePinList(it->second);
            } else if (it->first == "--" + WORKER_PIN_LIST_CONFIG) {
                setWorkerPinList(it->second);
            } else if (it->first == "--" + NUMA_AWARENESS_CONFIG) {
                setNumaAware(true);
            } else if (it->first == "--" + ENABLE_MONITORING_CONFIG && !it->second.empty()) {
                setEnableMonitoring((it->second == "true"));
            } else {
                NES_WARNING("Unknown configuration value :" << it->first);
            }
        }

        //NOTE: Despite being a vector, there is only one physical source definition possible using command line params
        auto physicalSource = PhysicalSourceFactory::createPhysicalSource(commandLineArguments);
        if (physicalSource) {
            addPhysicalSource(physicalSource);
        }
    } catch (std::exception& e) {
        NES_THROW_RUNTIME_ERROR("NesWorkerConfig: Error while initializing configuration parameters from command line. "
                                << e.what());
    }
}

void WorkerConfiguration::resetWorkerOptions() {
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
    setNumberOfBuffersPerWorker(numberOfBuffersPerWorker->getDefaultValue());
    setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
    setQueryCompilerCompilationStrategy(queryCompilerCompilationStrategy->getDefaultValue());
    setQueryCompilerPipeliningStrategy(queryCompilerPipeliningStrategy->getDefaultValue());
    setQueryCompilerOutputBufferAllocationStrategy(queryCompilerOutputBufferOptimizationLevel->getDefaultValue());
    setWorkerPinList(workerPinList->getDefaultValue());
    setSourcePinList(sourcePinList->getDefaultValue());
    setEnableMonitoring(enableMonitoring->getDefaultValue());
    for (auto& physicalSource : physicalSources) {
        physicalSource->getPhysicalSourceType()->reset();
    }
}

std::string WorkerConfiguration::toString() {
    std::stringstream ss;
    ss << localWorkerIp->toStringNameCurrentValue();
    ss << coordinatorIp->toStringNameCurrentValue();
    ss << coordinatorPort->toStringNameCurrentValue();
    ss << rpcPort->toStringNameCurrentValue();
    ss << dataPort->toStringNameCurrentValue();
    ss << numberOfSlots->toStringNameCurrentValue();
    ss << numWorkerThreads->toStringNameCurrentValue();
    ss << parentId->toStringNameCurrentValue();
    ss << logLevel->toStringNameCurrentValue();
    ss << numberOfBuffersInGlobalBufferManager->toStringNameCurrentValue();
    ss << numberOfBuffersPerWorker->toStringNameCurrentValue();
    ss << numberOfBuffersInSourceLocalBufferPool->toStringNameCurrentValue();
    ss << queryCompilerOutputBufferOptimizationLevel->toStringNameCurrentValue();
    ss << workerPinList->toStringNameCurrentValue();
    ss << sourcePinList->toStringNameCurrentValue();
    ss << enableMonitoring->toStringNameCurrentValue();
    for (PhysicalSourcePtr physicalSource : physicalSources) {
        ss << physicalSource->toString();
    }
    return ss.str();
}

StringConfigOption WorkerConfiguration::getLocalWorkerIp() { return localWorkerIp; }

void WorkerConfiguration::setLocalWorkerIp(std::string localWorkerIpValue) {
    localWorkerIp->setValue(std::move(localWorkerIpValue));
}

StringConfigOption WorkerConfiguration::getCoordinatorIp() { return coordinatorIp; }

void WorkerConfiguration::setCoordinatorIp(std::string coordinatorIpValue) {
    coordinatorIp->setValue(std::move(coordinatorIpValue));
}

IntConfigOption WorkerConfiguration::getCoordinatorPort() { return coordinatorPort; }

void WorkerConfiguration::setCoordinatorPort(uint16_t coordinatorPortValue) { coordinatorPort->setValue(coordinatorPortValue); }

IntConfigOption WorkerConfiguration::getRpcPort() { return rpcPort; }

void WorkerConfiguration::setRpcPort(uint16_t rpcPortValue) { rpcPort->setValue(rpcPortValue); }

IntConfigOption WorkerConfiguration::getDataPort() { return dataPort; }

void WorkerConfiguration::setDataPort(uint16_t dataPortValue) { dataPort->setValue(dataPortValue); }

IntConfigOption WorkerConfiguration::getNumberOfSlots() { return numberOfSlots; }

void WorkerConfiguration::setNumberOfSlots(uint16_t numberOfSlotsValue) { numberOfSlots->setValue(numberOfSlotsValue); }

IntConfigOption WorkerConfiguration::getNumWorkerThreads() { return numWorkerThreads; }

void WorkerConfiguration::setNumWorkerThreads(uint16_t numWorkerThreadsValue) {
    numWorkerThreads->setValue(numWorkerThreadsValue);
}

IntConfigOption WorkerConfiguration::getParentId() { return parentId; }

void WorkerConfiguration::setParentId(uint32_t parentIdValue) { parentId->setValue(parentIdValue); }

StringConfigOption WorkerConfiguration::getLogLevel() { return logLevel; }

void WorkerConfiguration::setLogLevel(std::string logLevelValue) { logLevel->setValue(std::move(logLevelValue)); }

IntConfigOption WorkerConfiguration::getNumberOfBuffersInGlobalBufferManager() { return numberOfBuffersInGlobalBufferManager; }
IntConfigOption WorkerConfiguration::getNumberOfBuffersPerWorker() { return numberOfBuffersPerWorker; }
IntConfigOption WorkerConfiguration::getNumberOfBuffersInSourceLocalBufferPool() {
    return numberOfBuffersInSourceLocalBufferPool;
}

void WorkerConfiguration::setNumberOfBuffersInGlobalBufferManager(uint64_t count) {
    numberOfBuffersInGlobalBufferManager->setValue(count);
}
void WorkerConfiguration::setNumberOfBuffersPerWorker(uint64_t count) { numberOfBuffersPerWorker->setValue(count); }
void WorkerConfiguration::setNumberOfBuffersInSourceLocalBufferPool(uint64_t count) {
    numberOfBuffersInSourceLocalBufferPool->setValue(count);
}

IntConfigOption WorkerConfiguration::getBufferSizeInBytes() { return bufferSizeInBytes; }

void WorkerConfiguration::setBufferSizeInBytes(uint64_t sizeInBytes) { bufferSizeInBytes->setValue(sizeInBytes); }

const StringConfigOption WorkerConfiguration::getQueryCompilerOutputBufferAllocationStrategy() const {
    return queryCompilerOutputBufferOptimizationLevel;
}
void WorkerConfiguration::setQueryCompilerOutputBufferAllocationStrategy(
    std::string queryCompilerOutputBufferAllocationStrategy) {
    this->queryCompilerOutputBufferOptimizationLevel->setValue(std::move(queryCompilerOutputBufferAllocationStrategy));
}

const StringConfigOption WorkerConfiguration::getQueryCompilerCompilationStrategy() const {
    return queryCompilerCompilationStrategy;
}

void WorkerConfiguration::setQueryCompilerCompilationStrategy(std::string queryCompilerCompilationStrategy) {
    this->queryCompilerCompilationStrategy->setValue(queryCompilerCompilationStrategy);
}

const StringConfigOption WorkerConfiguration::getQueryCompilerPipeliningStrategy() const {
    return queryCompilerPipeliningStrategy;
}

void WorkerConfiguration::setQueryCompilerPipeliningStrategy(std::string queryCompilerPipeliningStrategy) {
    this->queryCompilerPipeliningStrategy->setValue(queryCompilerPipeliningStrategy);
}

const StringConfigOption& WorkerConfiguration::getWorkerPinList() const { return workerPinList; }

const StringConfigOption& WorkerConfiguration::getSourcePinList() const { return sourcePinList; }

void WorkerConfiguration::setWorkerPinList(const std::string list) {
    if (!list.empty()) {
        WorkerConfiguration::workerPinList->setValue(list);
    }
}

void WorkerConfiguration::setSourcePinList(const std::string list) {
    if (!list.empty()) {
        WorkerConfiguration::sourcePinList->setValue(list);
    }
}

bool WorkerConfiguration::isNumaAware() const { return numaAwareness->getValue(); }

void WorkerConfiguration::setNumaAware(bool status) { numaAwareness->setValue(status); }

BoolConfigOption WorkerConfiguration::getEnableMonitoring() { return enableMonitoring; }

void WorkerConfiguration::setEnableMonitoring(bool enableMonitoring) {
    WorkerConfiguration::enableMonitoring->setValue(enableMonitoring);
}

void WorkerConfiguration::setPhysicalSources(std::vector<PhysicalSourcePtr> physicalSources) {
    this->physicalSources = physicalSources;
}

std::vector<PhysicalSourcePtr> WorkerConfiguration::getPhysicalSources() { return physicalSources; }

void WorkerConfiguration::addPhysicalSource(PhysicalSourcePtr physicalSource) {
    this->physicalSources.emplace_back(physicalSource);
}

}// namespace Configurations
}// namespace NES