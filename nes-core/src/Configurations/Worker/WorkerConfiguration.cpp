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

        auto contents = Util::detail::file_get_contents<std::string>(filePath.c_str());
        ryml::Tree tree = ryml::parse(ryml::to_csubstr(contents));
        ryml::NodeRef root = tree.rootref();

        try {
            if (root.has_child(ryml::to_csubstr(LOCAL_WORKER_IP_CONFIG))
                && root.find_child(ryml::to_csubstr(LOCAL_WORKER_IP_CONFIG)).val() != nullptr) {
                setLocalWorkerIp(root.find_child(ryml::to_csubstr(LOCAL_WORKER_IP_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(COORDINATOR_IP_CONFIG))
                && root.find_child(ryml::to_csubstr(COORDINATOR_IP_CONFIG)).val() != nullptr) {
                setCoordinatorIp(root.find_child(ryml::to_csubstr(COORDINATOR_IP_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(COORDINATOR_PORT_CONFIG))
                && root.find_child(ryml::to_csubstr(COORDINATOR_PORT_CONFIG)).val() != nullptr) {
                setCoordinatorPort(std::stoi(root.find_child(ryml::to_csubstr(COORDINATOR_PORT_CONFIG)).val().str));
            }
            if (root.has_child(ryml::to_csubstr(RPC_PORT_CONFIG))
                && root.find_child(ryml::to_csubstr(RPC_PORT_CONFIG)).val() != nullptr) {
                setRpcPort(std::stoi(root.find_child(ryml::to_csubstr(RPC_PORT_CONFIG)).val().str));
            }
            if (root.has_child(ryml::to_csubstr(DATA_PORT_CONFIG))
                && root.find_child(ryml::to_csubstr(DATA_PORT_CONFIG)).val() != nullptr) {
                setDataPort(std::stoi(root.find_child(ryml::to_csubstr(DATA_PORT_CONFIG)).val().str));
            }
            if (root.has_child(ryml::to_csubstr(NUMBER_OF_SLOTS_CONFIG))
                && root.find_child(ryml::to_csubstr(NUMBER_OF_SLOTS_CONFIG)).val() != nullptr) {
                setNumberOfSlots(std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_SLOTS_CONFIG)).val().str));
            }
            if (root.has_child(ryml::to_csubstr(NUM_WORKER_THREADS_CONFIG))
                && root.find_child(ryml::to_csubstr(NUM_WORKER_THREADS_CONFIG)).val() != nullptr) {
                setNumWorkerThreads(std::stoi(root.find_child(ryml::to_csubstr(NUM_WORKER_THREADS_CONFIG)).val().str));
            }
            if (root.has_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG))
                && root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG)).val() != nullptr) {
                setNumberOfBuffersInGlobalBufferManager(
                    std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG)).val().str));
            }
            if (root.has_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG))
                && root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG)).val() != nullptr) {
                setNumberOfBuffersInSourceLocalBufferPool(
                    std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG)).val().str));
            }
            if (root.has_child(ryml::to_csubstr(BUFFERS_SIZE_IN_BYTES_CONFIG))
                && root.find_child(ryml::to_csubstr(BUFFERS_SIZE_IN_BYTES_CONFIG)).val() != nullptr) {
                setBufferSizeInBytes(std::stoi(root.find_child(ryml::to_csubstr(BUFFERS_SIZE_IN_BYTES_CONFIG)).val().str));
            }
            if (root.has_child(ryml::to_csubstr(PARENT_ID_CONFIG))
                && root.find_child(ryml::to_csubstr(PARENT_ID_CONFIG)).val() != nullptr) {
                setParentId(std::stoi(root.find_child(ryml::to_csubstr(PARENT_ID_CONFIG)).val().str));
            }
            if (root.has_child(ryml::to_csubstr(LOG_LEVEL_CONFIG))
                && root.find_child(ryml::to_csubstr(LOG_LEVEL_CONFIG)).val() != nullptr) {
                setLogLevel(root.find_child(ryml::to_csubstr(LOG_LEVEL_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG))
                && root.find_child(ryml::to_csubstr(QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG)).val() != nullptr) {
                setQueryCompilerCompilationStrategy(
                    root.find_child(ryml::to_csubstr(QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG))
                && root.find_child(ryml::to_csubstr(QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG)).val() != nullptr) {
                setQueryCompilerPipeliningStrategy(
                    root.find_child(ryml::to_csubstr(QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG))
                && root.find_child(ryml::to_csubstr(QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG)).val() != nullptr) {
                setQueryCompilerOutputBufferAllocationStrategy(
                    root.find_child(ryml::to_csubstr(QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(SOURCE_PIN_LIST_CONFIG))
                && root.find_child(ryml::to_csubstr(SOURCE_PIN_LIST_CONFIG)).val() != nullptr) {
                setSourcePinList(root.find_child(ryml::to_csubstr(SOURCE_PIN_LIST_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(WORKER_PIN_LIST_CONFIG))
                && root.find_child(ryml::to_csubstr(WORKER_PIN_LIST_CONFIG)).val() != nullptr) {
                setWorkerPinList(root.find_child(ryml::to_csubstr(WORKER_PIN_LIST_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(NUMA_AWARENESS_CONFIG))
                && root.find_child(ryml::to_csubstr(NUMA_AWARENESS_CONFIG)).val() != nullptr) {
                setNumaAware(root.find_child(ryml::to_csubstr(NUMA_AWARENESS_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(ENABLE_MONITORING_CONFIG))
                && root.find_child(ryml::to_csubstr(ENABLE_MONITORING_CONFIG)).val() != nullptr) {
                setEnableMonitoring(root.find_child(ryml::to_csubstr(ENABLE_MONITORING_CONFIG)).val().str);
            }
            if (root.has_child(ryml::to_csubstr(PHYSICAL_STREAMS_CONFIG))
                && root.find_child(ryml::to_csubstr(PHYSICAL_STREAMS_CONFIG)) != nullptr) {
                const c4::yml::NodeRef& physicalSourceConfigs = root.find_child(ryml::to_csubstr(PHYSICAL_STREAMS_CONFIG));
                setPhysicalSources(PhysicalSourceFactory::createPhysicalSources(physicalSourceConfigs));
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