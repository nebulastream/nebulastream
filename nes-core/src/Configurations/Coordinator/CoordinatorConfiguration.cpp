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

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Util/Logger.hpp>
#define RYML_SINGLE_HDR_DEFINE_NOW
#include <Util/yaml/rapidyaml.hpp>
#include <Util/UtilityFunctions.hpp>
#include <filesystem>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

CoordinatorConfigurationPtr CoordinatorConfiguration::create() { return std::make_shared<CoordinatorConfiguration>(CoordinatorConfiguration()); }

CoordinatorConfiguration::CoordinatorConfiguration() {
    NES_INFO("Generated new Coordinator Config object. Configurations initialized with default values.");
    restIp = ConfigurationOption<std::string>::create("restIp", "127.0.0.1", "NES ip of the REST server.");
    coordinatorIp = ConfigurationOption<std::string>::create("coordinatorIp", "127.0.0.1", "RPC IP address of NES Coordinator.");
    rpcPort = ConfigurationOption<uint32_t>::create("rpcPort", 4000, "RPC server port of the NES Coordinator");
    restPort = ConfigurationOption<uint32_t>::create("restPort", 8081, "Port exposed for rest endpoints");
    dataPort = ConfigurationOption<uint32_t>::create("dataPort", 3001, "NES data server port");
    numberOfSlots =
        ConfigurationOption<uint32_t>::create("numberOfSlots", UINT16_MAX, "Number of computing slots for NES Coordinator");
    logLevel = ConfigurationOption<std::string>::create("logLevel",
                                                 "LOG_DEBUG",
                                                 "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)");
    numberOfBuffersInGlobalBufferManager = ConfigurationOption<uint32_t>::create("numberOfBuffersInGlobalBufferManager", 1024, "Number buffers in global buffer pool.");
    numberOfBuffersPerWorker =
        ConfigurationOption<uint32_t>::create("numberOfBuffersPerWorker", 128, "Number buffers in task local buffer pool.");
    numberOfBuffersInSourceLocalBufferPool = ConfigurationOption<uint32_t>::create("numberOfBuffersInSourceLocalBufferPool",
                                                                            64,
                                                                            "Number buffers in source local buffer pool.");
    bufferSizeInBytes = ConfigurationOption<uint32_t>::create("bufferSizeInBytes", 4096, "BufferSizeInBytes.");
    numWorkerThreads = ConfigurationOption<uint32_t>::create("numWorkerThreads", 1, "Number of worker threads.");
    queryBatchSize = ConfigurationOption<uint32_t>::create("queryBatchSize", 1, "The number of queries to be processed together");
    queryMergerRule = ConfigurationOption<std::string>::create("queryMergerRule",
                                                        "DefaultQueryMergerRule",
                                                        "The rule to be used for performing query merging");
    enableSemanticQueryValidation =
        ConfigurationOption<bool>::create("enableSemanticQueryValidation", false, "Enable semantic query validation feature");
    enableMonitoring = ConfigurationOption<bool>::create("enableMonitoring", false, "Enable monitoring");
    memoryLayoutPolicy = ConfigurationOption<std::string>::create(
        "memoryLayoutPolicy",
        "FORCE_ROW_LAYOUT",
        "selects the memory layout selection policy can be [FORCE_ROW_LAYOUT|FORCE_COLUMN_LAYOUT]");
    performOnlySourceOperatorExpansion = ConfigurationOption<bool>::create(
        "performOnlySourceOperatorExpansion",
        false,
        "Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule. (Default: false)");
}

void CoordinatorConfiguration::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("CoordinatorConfiguration: Using config file with path: " << filePath << " .");
        auto contents = Util::detail::file_get_contents<std::string>(filePath.c_str());
        ryml::Tree tree = ryml::parse(ryml::to_csubstr(contents));
        ryml::NodeRef root = tree.rootref();

        try {
            if (root.find_child(ryml::to_csubstr(REST_PORT_CONFIG)).has_val()){
                setRestPort(std::stoi(root.find_child(ryml::to_csubstr(REST_PORT_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(RPC_PORT_CONFIG)).has_val()){
                setRpcPort(std::stoi(root.find_child(ryml::to_csubstr(RPC_PORT_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(DATA_PORT_CONFIG)).has_val()){
                setDataPort(std::stoi(root.find_child(ryml::to_csubstr(DATA_PORT_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(COORDINATOR_IP_CONFIG)).has_val()){
                setCoordinatorIp(root.find_child(ryml::to_csubstr(COORDINATOR_IP_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(NUMBER_OF_SLOTS_CONFIG)).has_val()){
                setNumberOfSlots(std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_SLOTS_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG)).has_val()){
                setNumberOfBuffersInGlobalBufferManager(std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_PER_WORKER_CONFIG)).has_val()){
                setNumberOfBuffersPerWorker(std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_PER_WORKER_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG)).has_val()){
                setNumberOfBuffersInSourceLocalBufferPool(std::stoi(root.find_child(ryml::to_csubstr(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(BUFFERS_SIZE_IN_BYTES_CONFIG)).has_val()){
                setBufferSizeInBytes(std::stoi(root.find_child(ryml::to_csubstr(BUFFERS_SIZE_IN_BYTES_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(QUERY_BATCH_SIZE_CONFIG)).has_val()){
                setQueryBatchSize(std::stoi(root.find_child(ryml::to_csubstr(QUERY_BATCH_SIZE_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(QUERY_MERGER_RULE_CONFIG)).has_val()){
                setQueryMergerRule(root.find_child(ryml::to_csubstr(QUERY_MERGER_RULE_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG)).has_val()){
                setEnableSemanticQueryValidation(std::stoi(root.find_child(ryml::to_csubstr(ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(ENABLE_MONITORING_CONFIG)).has_val()){
                setEnableMonitoring(root.find_child(ryml::to_csubstr(ENABLE_MONITORING_CONFIG)).val().str);
            }
            if (root.find_child(ryml::to_csubstr(NUM_WORKER_THREADS_CONFIG)).has_val()){
                setNumWorkerThreads(std::stoi(root.find_child(ryml::to_csubstr(NUM_WORKER_THREADS_CONFIG)).val().str));
            }
            if (root.find_child(ryml::to_csubstr(MEMORY_LAYOUT_POLICY_CONFIG)).has_val()){
                setMemoryLayoutPolicy(root.find_child(ryml::to_csubstr(MEMORY_LAYOUT_POLICY_CONFIG)).val().str);
            }
            if (!config["performOnlySourceOperatorExpansion"].As<std::string>().empty()
                && config["performOnlySourceOperatorExpansion"].As<std::string>() != "\n") {
                setPerformOnlySourceOperatorExpansion(config["performOnlySourceOperatorExpansion"].As<bool>());
            }
        } catch (std::exception& e) {
            NES_ERROR("CoordinatorConfiguration: Error while initializing configuration parameters from YAML file. " << e.what());
            NES_WARNING("CoordinatorConfiguration: Keeping default values.");
            resetCoordinatorOptions();
        }
        return;
    }
    NES_ERROR("CoordinatorConfiguration: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("CoordinatorConfiguration: Keeping default values for Coordinator Config.");
}

void CoordinatorConfiguration::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    try {
        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            if (it->first == "--"+REST_IP_CONFIG && !it->second.empty()) {
                setRestIp(it->second);
            } else if (it->first == "--"+COORDINATOR_IP_CONFIG && !it->second.empty()) {
                setCoordinatorIp(it->second);
            } else if (it->first == "--"+RPC_PORT_CONFIG && !it->second.empty()) {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--"+REST_PORT_CONFIG && !it->second.empty()) {
                setRestPort(stoi(it->second));
            } else if (it->first == "--"+DATA_PORT_CONFIG && !it->second.empty()) {
                setDataPort(stoi(it->second));
            } else if (it->first == "--"+NUMBER_OF_SLOTS_CONFIG && !it->second.empty()) {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--"+LOG_LEVEL_CONFIG && !it->second.empty()) {
                setLogLevel(it->second);
            } else if (it->first == "--"+NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG && !it->second.empty()) {
                setNumberOfBuffersInGlobalBufferManager(stoi(it->second));
            } else if (it->first == "--"+NUMBER_OF_BUFFERS_PER_WORKER_CONFIG && !it->second.empty()) {
                setNumberOfBuffersPerWorker(stoi(it->second));
            } else if (it->first == "--"+NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG && !it->second.empty()) {
                setNumberOfBuffersInSourceLocalBufferPool(stoi(it->second));
            } else if (it->first == "--"+BUFFERS_SIZE_IN_BYTES_CONFIG && !it->second.empty()) {
                setBufferSizeInBytes(stoi(it->second));
            } else if (it->first == "--"+NUM_WORKER_THREADS_CONFIG && !it->second.empty()) {
                setNumWorkerThreads(stoi(it->second));
            } else if (it->first == "--"+QUERY_BATCH_SIZE_CONFIG && !it->second.empty()) {
                setQueryBatchSize(stoi(it->second));
            } else if (it->first == "--"+QUERY_MERGER_RULE_CONFIG && !it->second.empty()) {
                setQueryMergerRule(it->second);
            } else if (it->first == "--"+ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG && !it->second.empty()) {
                setEnableSemanticQueryValidation((it->second == "true"));
            } else if (it->first == "--"+ENABLE_MONITORING_CONFIG && !it->second.empty()) {
                setEnableMonitoring((it->second == "true"));
            } else if (it->first == "--"+MEMORY_LAYOUT_POLICY_CONFIG && !it->second.empty()) {
                setMemoryLayoutPolicy(it->second);
            } else if (it->first == "--performOnlySourceOperatorExpansion" && !it->second.empty()) {
                setPerformOnlySourceOperatorExpansion((it->second == "true"));
            } else {
                NES_WARNING("Unknown configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("CoordinatorConfiguration: Error while initializing configuration parameters from command line.");
        NES_WARNING("CoordinatorConfiguration: Keeping default values.");
        resetCoordinatorOptions();
    }
}

void CoordinatorConfiguration::resetCoordinatorOptions() {
    setRestPort(restPort->getDefaultValue());
    setRpcPort(rpcPort->getDefaultValue());
    setDataPort(dataPort->getDefaultValue());
    setRestIp(restIp->getDefaultValue());
    setCoordinatorIp(coordinatorIp->getDefaultValue());
    setNumberOfSlots(numberOfSlots->getDefaultValue());
    setLogLevel(logLevel->getDefaultValue());
    setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager->getDefaultValue());
    setNumberOfBuffersPerWorker(numberOfBuffersPerWorker->getDefaultValue());
    setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
    setBufferSizeInBytes(bufferSizeInBytes->getDefaultValue());
    setNumWorkerThreads(numWorkerThreads->getDefaultValue());
    setQueryBatchSize(queryBatchSize->getDefaultValue());
    setQueryMergerRule(queryMergerRule->getDefaultValue());
    setEnableSemanticQueryValidation(enableSemanticQueryValidation->getDefaultValue());
    setEnableMonitoring(enableMonitoring->getDefaultValue());
    setMemoryLayoutPolicy(memoryLayoutPolicy->getDefaultValue());
    setPerformOnlySourceOperatorExpansion(performOnlySourceOperatorExpansion->getDefaultValue());
}

std::string CoordinatorConfiguration::toString() {
    std::stringstream ss;
    ss << restPort->toStringNameCurrentValue();
    ss << rpcPort->toStringNameCurrentValue();
    ss << dataPort->toStringNameCurrentValue();
    ss << restIp->toStringNameCurrentValue();
    ss << coordinatorIp->toStringNameCurrentValue();
    ss << numberOfSlots->toStringNameCurrentValue();
    ss << logLevel->toStringNameCurrentValue();
    ss << numberOfBuffersInGlobalBufferManager->toStringNameCurrentValue();
    ss << numberOfBuffersPerWorker->toStringNameCurrentValue();
    ss << numberOfBuffersInSourceLocalBufferPool->toStringNameCurrentValue();
    ss << bufferSizeInBytes->toStringNameCurrentValue();
    ss << numWorkerThreads->toStringNameCurrentValue();
    ss << queryBatchSize->toStringNameCurrentValue();
    ss << queryMergerRule->toStringNameCurrentValue();
    ss << enableSemanticQueryValidation->toStringNameCurrentValue();
    ss << enableMonitoring->toStringNameCurrentValue();
    ss << memoryLayoutPolicy->toStringNameCurrentValue();
    ss << performOnlySourceOperatorExpansion->toStringNameCurrentValue();
    return ss.str();
}

StringConfigOption CoordinatorConfiguration::getRestIp() { return restIp; }

void CoordinatorConfiguration::setRestIp(std::string restIpValue) { restIp->setValue(std::move(restIpValue)); }

StringConfigOption CoordinatorConfiguration::getCoordinatorIp() { return coordinatorIp; }

void CoordinatorConfiguration::setCoordinatorIp(std::string coordinatorIpValue) {
    coordinatorIp->setValue(std::move(coordinatorIpValue));
}

IntConfigOption CoordinatorConfiguration::getRpcPort() { return rpcPort; }

void CoordinatorConfiguration::setRpcPort(uint16_t rpcPortValue) { rpcPort->setValue(rpcPortValue); }

IntConfigOption CoordinatorConfiguration::getRestPort() { return restPort; }

void CoordinatorConfiguration::setRestPort(uint16_t restPortValue) { restPort->setValue(restPortValue); }

IntConfigOption CoordinatorConfiguration::getDataPort() { return dataPort; }

void CoordinatorConfiguration::setDataPort(uint16_t dataPortValue) { dataPort->setValue(dataPortValue); }

IntConfigOption CoordinatorConfiguration::getNumberOfSlots() { return numberOfSlots; }

void CoordinatorConfiguration::setNumberOfSlots(uint16_t numberOfSlotsValue) { numberOfSlots->setValue(numberOfSlotsValue); }

void CoordinatorConfiguration::setNumWorkerThreads(uint16_t numWorkerThreadsValue) { numWorkerThreads->setValue(numWorkerThreadsValue); }

IntConfigOption CoordinatorConfiguration::getNumWorkerThreads() { return numWorkerThreads; }

StringConfigOption CoordinatorConfiguration::getLogLevel() { return logLevel; }

void CoordinatorConfiguration::setLogLevel(std::string logLevelValue) { logLevel->setValue(std::move(logLevelValue)); }

IntConfigOption CoordinatorConfiguration::getNumberOfBuffersInGlobalBufferManager() { return numberOfBuffersInGlobalBufferManager; }
IntConfigOption CoordinatorConfiguration::getNumberOfBuffersPerWorker() { return numberOfBuffersPerWorker; }
IntConfigOption CoordinatorConfiguration::getNumberOfBuffersInSourceLocalBufferPool() { return numberOfBuffersInSourceLocalBufferPool; }

void CoordinatorConfiguration::setNumberOfBuffersInGlobalBufferManager(uint64_t count) {
    numberOfBuffersInGlobalBufferManager->setValue(count);
}
void CoordinatorConfiguration::setNumberOfBuffersPerWorker(uint64_t count) { numberOfBuffersPerWorker->setValue(count); }
void CoordinatorConfiguration::setNumberOfBuffersInSourceLocalBufferPool(uint64_t count) {
    numberOfBuffersInSourceLocalBufferPool->setValue(count);
}

IntConfigOption CoordinatorConfiguration::getBufferSizeInBytes() { return bufferSizeInBytes; }

void CoordinatorConfiguration::setBufferSizeInBytes(uint64_t sizeInBytes) { bufferSizeInBytes->setValue(sizeInBytes); }

IntConfigOption CoordinatorConfiguration::getQueryBatchSize() { return queryBatchSize; }

void CoordinatorConfiguration::setQueryBatchSize(uint32_t batchSize) { queryBatchSize->setValue(batchSize); }

StringConfigOption CoordinatorConfiguration::getQueryMergerRule() { return queryMergerRule; }

void CoordinatorConfiguration::setQueryMergerRule(std::string queryMergerRuleValue) {
    queryMergerRule->setValue(std::move(queryMergerRuleValue));
}

BoolConfigOption CoordinatorConfiguration::getEnableSemanticQueryValidation() { return enableSemanticQueryValidation; }

void CoordinatorConfiguration::setEnableSemanticQueryValidation(bool enableSemanticQueryValidation) {
    CoordinatorConfiguration::enableSemanticQueryValidation->setValue(enableSemanticQueryValidation);
}

BoolConfigOption CoordinatorConfiguration::getEnableMonitoring() { return enableMonitoring; }

void CoordinatorConfiguration::setEnableMonitoring(bool enableMonitoring) {
    CoordinatorConfiguration::enableMonitoring->setValue(enableMonitoring);
}

void CoordinatorConfiguration::setMemoryLayoutPolicy(std::string memoryLayoutPolicy) {
    this->memoryLayoutPolicy->setValue(std::move(memoryLayoutPolicy));
}

StringConfigOption CoordinatorConfiguration::getMemoryLayoutPolicy() { return memoryLayoutPolicy; }

BoolConfigOption CoordinatorConfiguration::getPerformOnlySourceOperatorExpansion() { return performOnlySourceOperatorExpansion; }
void CoordinatorConfiguration::setPerformOnlySourceOperatorExpansion(bool performOnlySourceOperatorExpansion) {
    this->performOnlySourceOperatorExpansion->setValue(performOnlySourceOperatorExpansion);
}

}// namespace Configurations
}// namespace NES
