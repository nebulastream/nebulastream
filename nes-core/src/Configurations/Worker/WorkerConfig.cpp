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
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/PhysicalStreamTypeConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/rapidyaml.hpp>
#include <Util/UtilityFunctions.hpp>
#include <filesystem>
#include <string>
#include <thread>
#include <utility>

namespace NES {

namespace Configurations {

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
    numberOfBuffersPerWorker =
        ConfigOption<uint32_t>::create("numberOfBuffersPerWorker", 128, "Number buffers in task local buffer pool.");
    numberOfBuffersInSourceLocalBufferPool = ConfigOption<uint32_t>::create("numberOfBuffersInSourceLocalBufferPool",
                                                                            64,
                                                                            "Number buffers in source local buffer pool.");
    bufferSizeInBytes = ConfigOption<uint32_t>::create("bufferSizeInBytes", 4096, "BufferSizeInBytes.");
    parentId = ConfigOption<std::string>::create("parentId", "-1", "Parent ID of this node.");
    logLevel = ConfigOption<std::string>::create("logLevel",
                                                 "LOG_DEBUG",
                                                 "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ");

    queryCompilerCompilationStrategy =
        ConfigOption<std::string>::create("queryCompilerCompilationStrategy",
                                          "OPTIMIZE",
                                          "Indicates the optimization strategy for the query compiler [FAST|DEBUG|OPTIMIZE].");

    queryCompilerPipeliningStrategy = ConfigOption<std::string>::create(
        "queryCompilerPipeliningStrategy",
        "OPERATOR_FUSION",
        "Indicates the pipelining strategy for the query compiler [OPERATOR_FUSION|OPERATOR_AT_A_TIME].");

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

    enableMonitoring = ConfigOption<bool>::create("enableMonitoring", false, "Enable monitoring");
}

void WorkerConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {

        NES_INFO("NesWorkerConfig: Using config file with path: " << filePath << " .");

        auto contents = Util::detail::file_get_contents<std::string>(filePath.c_str());
        ryml::Tree tree = ryml::parse(ryml::to_csubstr(contents));
        ryml::NodeRef root = tree.rootref();

        try {
            if (root.find_child(ryml::to_csubstr(LOCAL_WORKER_IP_CONFIG)).has_val()){
                setLocalWorkerIp(root.find_child(ryml::to_csubstr(LOCAL_WORKER_IP_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(COORDINATOR_IP_CONFIG)).has_val()){
                setCoordinatorIp(root.find_child(ryml::to_csubstr(COORDINATOR_IP_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(COORDINATOR_PORT_CONFIG)).has_val()){
                setCoordinatorPort(std::stoi(root.find_child(ryml::to_csubstr(COORDINATOR_PORT_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(RPC_PORT_CONFIG)).has_val()){
                setRpcPort(std::stoi(root.find_child(ryml::to_csubstr(RPC_PORT_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(DATA_PORT_CONFIG)).has_val()){
                setDataPort(std::stoi(root.find_child(ryml::to_csubstr(DATA_PORT_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(NUMBER_OF_SLOTS_CONFIG)).has_val()){
                setNumberOfSlots(std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_SLOTS_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(NUM_WORKER_THREADS_CONFIG)).has_val()){
                setNumWorkerThreads(std::stoi(root.find_child(ryml::to_csubstr(NUM_WORKER_THREADS_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG)).has_val()){
                setNumberOfBuffersInGlobalBufferManager(std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG)).has_val()){
                setNumberOfBuffersInSourceLocalBufferPool(std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(BUFFERS_SIZE_IN_BYTES_CONFIG)).has_val()){
                setBufferSizeInBytes(std::stoi(root.find_child(ryml::to_csubstr(BUFFERS_SIZE_IN_BYTES_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(PARENT_ID_CONFIG)).has_val()){
                setParentId(root.find_child(ryml::to_csubstr(PARENT_ID_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(LOG_LEVEL_CONFIG)).has_val()){
                setLogLevel(root.find_child(ryml::to_csubstr(LOG_LEVEL_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG)).has_val()){
                setQueryCompilerCompilationStrategy(root.find_child(ryml::to_csubstr(QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG)).has_val()){
                setQueryCompilerPipeliningStrategy(root.find_child(ryml::to_csubstr(QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG)).has_val()){
                setQueryCompilerOutputBufferAllocationStrategy(root.find_child(ryml::to_csubstr(QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(SOURCE_PIN_LIST_CONFIG)).has_val()){
                setSourcePinList(root.find_child(ryml::to_csubstr(SOURCE_PIN_LIST_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(WORKER_PIN_LIST_CONFIG)).has_val()){
                setWorkerPinList(root.find_child(ryml::to_csubstr(WORKER_PIN_LIST_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(NUMA_AWARENESS_CONFIG)).has_val()){
                setNumaAware(root.find_child(ryml::to_csubstr(NUMA_AWARENESS_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(ENABLE_MONITORING_CONFIG)).has_val()){
                setEnableMonitoring(root.find_child(ryml::to_csubstr(ENABLE_MONITORING_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(PHYSICAL_STREAMS_CONFIG)).has_val()){
                for(ryml::NodeRef const& child : root.find_child(ryml::to_csubstr(PHYSICAL_STREAMS_CONFIG))){
                    physicalStreamsConfig.push_back(PhysicalStreamTypeConfig::create(child));
                }
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
            if (it->first == "--"+LOCAL_WORKER_IP_CONFIG && !it->second.empty()) {
                setLocalWorkerIp(it->second);
            } else if (it->first == "--"+COORDINATOR_IP_CONFIG && !it->second.empty()) {
                setCoordinatorIp(it->second);
            } else if (it->first == "--"+RPC_PORT_CONFIG && !it->second.empty()) {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--"+COORDINATOR_PORT_CONFIG && !it->second.empty()) {
                setCoordinatorPort(stoi(it->second));
            } else if (it->first == "--"+DATA_PORT_CONFIG && !it->second.empty()) {
                setDataPort(stoi(it->second));
            } else if (it->first == "--"+NUMBER_OF_SLOTS_CONFIG && !it->second.empty()) {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--"+NUM_WORKER_THREADS_CONFIG && !it->second.empty()) {
                setNumWorkerThreads(stoi(it->second));
            } else if (it->first == "--"+NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG && !it->second.empty()) {
                setNumberOfBuffersInGlobalBufferManager(stoi(it->second));
            } else if (it->first == "--"+NUMBER_OF_BUFFERS_PER_WORKER_CONFIG && !it->second.empty()) {
                setNumberOfBuffersPerWorker(stoi(it->second));
            } else if (it->first == "--"+NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG && !it->second.empty()) {
                setNumberOfBuffersInSourceLocalBufferPool(stoi(it->second));
            } else if (it->first == "--"+BUFFERS_SIZE_IN_BYTES_CONFIG && !it->second.empty()) {
                setBufferSizeInBytes(stoi(it->second));
            } else if (it->first == "--"+PARENT_ID_CONFIG && !it->second.empty()) {
                setParentId(it->second);
            } else if (it->first == "--"+QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG && !it->second.empty()) {
                setQueryCompilerCompilationStrategy(it->second);
            } else if (it->first == "--"+QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG && !it->second.empty()) {
                setQueryCompilerPipeliningStrategy(it->second);
            } else if (it->first == "--"+QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG && !it->second.empty()) {
                setQueryCompilerOutputBufferAllocationStrategy(it->second);
            } else if (it->first == "--"+SOURCE_PIN_LIST_CONFIG) {
                setSourcePinList(it->second);
            } else if (it->first == "--"+WORKER_PIN_LIST_CONFIG) {
                setWorkerPinList(it->second);
            } else if (it->first == "--"+NUMA_AWARENESS_CONFIG) {
                setNumaAware(true);
            } else if (it->first == "--"+ENABLE_MONITORING_CONFIG && !it->second.empty()) {
                setEnableMonitoring((it->second == "true"));
            } else {
                NES_WARNING("Unknow configuration value :" << it->first);
            }
        }
        //Despite being a vector, there is only one physical source definition possible using command line params
        physicalStreamsConfig.push_back(PhysicalStreamTypeConfig::create(inputParams));

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
    setNumberOfBuffersPerWorker(numberOfBuffersPerWorker->getDefaultValue());
    setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
    setQueryCompilerCompilationStrategy(queryCompilerCompilationStrategy->getDefaultValue());
    setQueryCompilerPipeliningStrategy(queryCompilerPipeliningStrategy->getDefaultValue());
    setQueryCompilerOutputBufferAllocationStrategy(queryCompilerOutputBufferOptimizationLevel->getDefaultValue());
    setWorkerPinList(workerPinList->getDefaultValue());
    setSourcePinList(sourcePinList->getDefaultValue());
    setEnableMonitoring(enableMonitoring->getDefaultValue());
    for (PhysicalStreamTypeConfigPtr physicalStreamTypeConfig : physicalStreamsConfig){
        physicalStreamTypeConfig->resetPhysicalStreamOptions();
    }
}

std::string WorkerConfig::toString() {
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
    for (PhysicalStreamTypeConfigPtr physicalStreamTypeConfig : physicalStreamsConfig){
        ss << physicalStreamTypeConfig->toString();
    }
    return ss.str();
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
IntConfigOption WorkerConfig::getNumberOfBuffersPerWorker() { return numberOfBuffersPerWorker; }
IntConfigOption WorkerConfig::getNumberOfBuffersInSourceLocalBufferPool() { return numberOfBuffersInSourceLocalBufferPool; }

void WorkerConfig::setNumberOfBuffersInGlobalBufferManager(uint64_t count) {
    numberOfBuffersInGlobalBufferManager->setValue(count);
}
void WorkerConfig::setNumberOfBuffersPerWorker(uint64_t count) { numberOfBuffersPerWorker->setValue(count); }
void WorkerConfig::setNumberOfBuffersInSourceLocalBufferPool(uint64_t count) {
    numberOfBuffersInSourceLocalBufferPool->setValue(count);
}

IntConfigOption WorkerConfig::getBufferSizeInBytes() { return bufferSizeInBytes; }

void WorkerConfig::setBufferSizeInBytes(uint64_t sizeInBytes) { bufferSizeInBytes->setValue(sizeInBytes); }

const StringConfigOption WorkerConfig::getQueryCompilerOutputBufferAllocationStrategy() const {
    return queryCompilerOutputBufferOptimizationLevel;
}
void WorkerConfig::setQueryCompilerOutputBufferAllocationStrategy(std::string queryCompilerOutputBufferAllocationStrategy) {
    this->queryCompilerOutputBufferOptimizationLevel->setValue(std::move(queryCompilerOutputBufferAllocationStrategy));
}

const StringConfigOption WorkerConfig::getQueryCompilerCompilationStrategy() const { return queryCompilerCompilationStrategy; }

void WorkerConfig::setQueryCompilerCompilationStrategy(std::string queryCompilerCompilationStrategy) {
    this->queryCompilerCompilationStrategy->setValue(queryCompilerCompilationStrategy);
}

const StringConfigOption WorkerConfig::getQueryCompilerPipeliningStrategy() const { return queryCompilerPipeliningStrategy; }

void WorkerConfig::setQueryCompilerPipeliningStrategy(std::string queryCompilerPipeliningStrategy) {
    this->queryCompilerPipeliningStrategy->setValue(queryCompilerPipeliningStrategy);
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

BoolConfigOption WorkerConfig::getEnableMonitoring() { return enableMonitoring; }

void WorkerConfig::setEnableMonitoring(bool enableMonitoring) { WorkerConfig::enableMonitoring->setValue(enableMonitoring); }
void WorkerConfig::setPhysicalStreamsConfig(std::vector<PhysicalStreamTypeConfigPtr> physicalStreamsConfigValues) {
    physicalStreamsConfig = physicalStreamsConfigValues;
}
std::vector<PhysicalStreamTypeConfigPtr> WorkerConfig::getPhysicalStreamsConfig() {
    return physicalStreamsConfig;
}

}// namespace Configurations
}// namespace NES