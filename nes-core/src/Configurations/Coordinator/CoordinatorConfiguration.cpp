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

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

CoordinatorConfigurationPtr CoordinatorConfiguration::create() {
    return std::make_shared<CoordinatorConfiguration>(CoordinatorConfiguration());
}

CoordinatorConfiguration::CoordinatorConfiguration() {
    NES_INFO("Generated new Coordinator Config object. Configurations initialized with default values.");
    restIp = ConfigurationOption<std::string>::create(REST_IP_CONFIG, "127.0.0.1", "NES ip of the REST server.");
    coordinatorIp =
        ConfigurationOption<std::string>::create(COORDINATOR_IP_CONFIG, "127.0.0.1", "RPC IP address of NES Coordinator.");
    rpcPort = ConfigurationOption<uint32_t>::create(RPC_PORT_CONFIG, 4000, "RPC server port of the NES Coordinator");
    restPort = ConfigurationOption<uint32_t>::create(REST_PORT_CONFIG, 8081, "Port exposed for rest endpoints");
    dataPort = ConfigurationOption<uint32_t>::create(DATA_PORT_CONFIG, 3001, "NES data server port");
    numberOfSlots = ConfigurationOption<uint32_t>::create(NUMBER_OF_SLOTS_CONFIG,
                                                          UINT16_MAX,
                                                          "Number of computing slots for NES Coordinator");
    logLevel = ConfigurationOption<std::string>::create(LOG_LEVEL_CONFIG,
                                                        "LOG_DEBUG",
                                                        "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)");
    numberOfBuffersInGlobalBufferManager =
        ConfigurationOption<uint32_t>::create(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG,
                                              1024,
                                              "Number buffers in global buffer pool.");
    numberOfBuffersPerWorker = ConfigurationOption<uint32_t>::create(NUMBER_OF_BUFFERS_PER_WORKER_CONFIG,
                                                                     128,
                                                                     "Number buffers in task local buffer pool.");
    numberOfBuffersInSourceLocalBufferPool =
        ConfigurationOption<uint32_t>::create(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG,
                                              64,
                                              "Number buffers in source local buffer pool.");
    bufferSizeInBytes = ConfigurationOption<uint32_t>::create(BUFFERS_SIZE_IN_BYTES_CONFIG, 4096, "BufferSizeInBytes.");
    numWorkerThreads = ConfigurationOption<uint32_t>::create(NUM_WORKER_THREADS_CONFIG, 1, "Number of worker threads.");
    queryBatchSize =
        ConfigurationOption<uint32_t>::create(QUERY_BATCH_SIZE_CONFIG, 1, "The number of queries to be processed together");
    queryMergerRule = ConfigurationOption<std::string>::create(QUERY_MERGER_RULE_CONFIG,
                                                               "DefaultQueryMergerRule",
                                                               "The rule to be used for performing query merging");
    enableSemanticQueryValidation = ConfigurationOption<bool>::create(ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG,
                                                                      false,
                                                                      "Enable semantic query validation feature");
    enableMonitoring = ConfigurationOption<bool>::create(ENABLE_MONITORING_CONFIG, false, "Enable monitoring");
    memoryLayoutPolicy = ConfigurationOption<std::string>::create(
        MEMORY_LAYOUT_POLICY_CONFIG,
        "FORCE_ROW_LAYOUT",
        "selects the memory layout selection policy can be [FORCE_ROW_LAYOUT|FORCE_COLUMN_LAYOUT]");
    performOnlySourceOperatorExpansion = ConfigurationOption<bool>::create(
        PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION,
        false,
        "Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule. (Default: false)");
    enableQueryReconfiguration =
        ConfigurationOption<bool>::create(ENABLE_QUERY_RECONFIGURATION,
                                          false,
                                          "Enable reconfiguration of running query plans. (Default: false)");
}

void CoordinatorConfiguration::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("CoordinatorConfiguration: Using config file with path: " << filePath << " .");
        Yaml::Node config;
        Yaml::Parse(config, filePath.c_str());

        try {
            if (!config[REST_PORT_CONFIG].As<std::string>().empty() && config[REST_PORT_CONFIG].As<std::string>() != "\n") {
                setRestPort(config[REST_PORT_CONFIG].As<uint64_t>());
            }
            if (!config[RPC_PORT_CONFIG].As<std::string>().empty() && config[RPC_PORT_CONFIG].As<std::string>() != "\n") {
                setRpcPort(config[RPC_PORT_CONFIG].As<uint64_t>());
            }
            if (!config[DATA_PORT_CONFIG].As<std::string>().empty() && config[DATA_PORT_CONFIG].As<std::string>() != "\n") {
                setDataPort(config[DATA_PORT_CONFIG].As<uint64_t>());
            }
            if (!config[COORDINATOR_IP_CONFIG].As<std::string>().empty()
                && config[COORDINATOR_IP_CONFIG].As<std::string>() != "\n") {
                setCoordinatorIp(config[COORDINATOR_IP_CONFIG].As<std::string>());
            }
            if (!config[REST_IP_CONFIG].As<std::string>().empty() && config[REST_IP_CONFIG].As<std::string>() != "\n") {
                setRestIp(config[REST_IP_CONFIG].As<std::string>());
            }
            if (!config[NUMBER_OF_SLOTS_CONFIG].As<std::string>().empty()
                && config[NUMBER_OF_SLOTS_CONFIG].As<std::string>() != "\n") {
                setNumberOfSlots(config[NUMBER_OF_SLOTS_CONFIG].As<uint64_t>());
            }
            if (!config[NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG].As<std::string>().empty()
                && config[NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG].As<std::string>() != "\n") {
                setNumberOfBuffersInGlobalBufferManager(config[NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG].As<uint64_t>());
            }
            if (!config[NUMBER_OF_BUFFERS_PER_WORKER_CONFIG].As<std::string>().empty()
                && config[NUMBER_OF_BUFFERS_PER_WORKER_CONFIG].As<std::string>() != "\n") {
                setNumberOfBuffersPerWorker(config[NUMBER_OF_BUFFERS_PER_WORKER_CONFIG].As<uint64_t>());
            }
            if (!config[NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG].As<std::string>().empty()
                && config[NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG].As<std::string>() != "\n") {
                setNumberOfBuffersInSourceLocalBufferPool(
                    config[NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG].As<uint64_t>());
            }
            if (!config[BUFFERS_SIZE_IN_BYTES_CONFIG].As<std::string>().empty()
                && config[BUFFERS_SIZE_IN_BYTES_CONFIG].As<std::string>() != "\n") {
                setBufferSizeInBytes(config[BUFFERS_SIZE_IN_BYTES_CONFIG].As<uint64_t>());
            }
            if (!config[QUERY_BATCH_SIZE_CONFIG].As<std::string>().empty()
                && config[QUERY_BATCH_SIZE_CONFIG].As<std::string>() != "\n") {
                setQueryBatchSize(config[QUERY_BATCH_SIZE_CONFIG].As<uint32_t>());
            }
            if (!config[QUERY_MERGER_RULE_CONFIG].As<std::string>().empty()
                && config[QUERY_MERGER_RULE_CONFIG].As<std::string>() != "\n") {
                setQueryMergerRule(config[QUERY_MERGER_RULE_CONFIG].As<std::string>());
            }
            if (!config[ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG].As<std::string>().empty()
                && config[ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG].As<std::string>() != "\n") {
                setEnableSemanticQueryValidation(config[ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG].As<bool>());
            }
            if (!config[ENABLE_MONITORING_CONFIG].As<std::string>().empty()
                && config[ENABLE_MONITORING_CONFIG].As<std::string>() != "\n") {
                setEnableMonitoring(config[ENABLE_MONITORING_CONFIG].As<bool>());
            }
            if (!config[NUM_WORKER_THREADS_CONFIG].As<std::string>().empty()
                && config[NUM_WORKER_THREADS_CONFIG].As<std::string>() != "\n") {
                setNumWorkerThreads(config[NUM_WORKER_THREADS_CONFIG].As<uint64_t>());
            }
            if (!config[MEMORY_LAYOUT_POLICY_CONFIG].As<std::string>().empty()
                && config[MEMORY_LAYOUT_POLICY_CONFIG].As<std::string>() != "\n") {
                setMemoryLayoutPolicy(config[MEMORY_LAYOUT_POLICY_CONFIG].As<std::string>());
            }
            if (!config[PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION].As<std::string>().empty()
                && config[PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION].As<std::string>() != "\n") {
                setPerformOnlySourceOperatorExpansion(config[PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION].As<bool>());
            }
            if (!config[PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION].As<std::string>().empty()
                && config[PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION].As<std::string>() != "\n") {
                setPerformOnlySourceOperatorExpansion(config[PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION].As<bool>());
            }
            if (!config[ENABLE_QUERY_RECONFIGURATION].As<std::string>().empty()
                && config[ENABLE_QUERY_RECONFIGURATION].As<std::string>() != "\n") {
                setQueryReconfiguration(config[ENABLE_QUERY_RECONFIGURATION].As<bool>());
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
            if (it->first == "--" + REST_IP_CONFIG && !it->second.empty()) {
                setRestIp(it->second);
            } else if (it->first == "--" + COORDINATOR_IP_CONFIG && !it->second.empty()) {
                setCoordinatorIp(it->second);
            } else if (it->first == "--" + RPC_PORT_CONFIG && !it->second.empty()) {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--" + REST_PORT_CONFIG && !it->second.empty()) {
                setRestPort(stoi(it->second));
            } else if (it->first == "--" + DATA_PORT_CONFIG && !it->second.empty()) {
                setDataPort(stoi(it->second));
            } else if (it->first == "--" + NUMBER_OF_SLOTS_CONFIG && !it->second.empty()) {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--" + LOG_LEVEL_CONFIG && !it->second.empty()) {
                setLogLevel(it->second);
            } else if (it->first == "--" + NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG && !it->second.empty()) {
                setNumberOfBuffersInGlobalBufferManager(stoi(it->second));
            } else if (it->first == "--" + NUMBER_OF_BUFFERS_PER_WORKER_CONFIG && !it->second.empty()) {
                setNumberOfBuffersPerWorker(stoi(it->second));
            } else if (it->first == "--" + NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG && !it->second.empty()) {
                setNumberOfBuffersInSourceLocalBufferPool(stoi(it->second));
            } else if (it->first == "--" + BUFFERS_SIZE_IN_BYTES_CONFIG && !it->second.empty()) {
                setBufferSizeInBytes(stoi(it->second));
            } else if (it->first == "--" + NUM_WORKER_THREADS_CONFIG && !it->second.empty()) {
                setNumWorkerThreads(stoi(it->second));
            } else if (it->first == "--" + QUERY_BATCH_SIZE_CONFIG && !it->second.empty()) {
                setQueryBatchSize(stoi(it->second));
            } else if (it->first == "--" + QUERY_MERGER_RULE_CONFIG && !it->second.empty()) {
                setQueryMergerRule(it->second);
            } else if (it->first == "--" + ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG && !it->second.empty()) {
                setEnableSemanticQueryValidation((it->second == "true"));
            } else if (it->first == "--" + ENABLE_MONITORING_CONFIG && !it->second.empty()) {
                setEnableMonitoring((it->second == "true"));
            } else if (it->first == "--" + MEMORY_LAYOUT_POLICY_CONFIG && !it->second.empty()) {
                setMemoryLayoutPolicy(it->second);
            } else if (it->first == "--" + PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION && !it->second.empty()) {
                setPerformOnlySourceOperatorExpansion((it->second == "true"));
            } else if (it->first == "--" + ENABLE_QUERY_RECONFIGURATION && !it->second.empty()) {
                setQueryReconfiguration((it->second == "true"));
            } else {
                NES_WARNING("Unknown configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("CoordinatorConfiguration: Error while initializing configuration parameters from command line. " << e.what());
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
    setQueryReconfiguration(enableQueryReconfiguration->getDefaultValue());
    this->logicalSources.clear();
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
    ss << enableQueryReconfiguration->toStringNameCurrentValue();
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

void CoordinatorConfiguration::setNumWorkerThreads(uint16_t numWorkerThreadsValue) {
    numWorkerThreads->setValue(numWorkerThreadsValue);
}

IntConfigOption CoordinatorConfiguration::getNumWorkerThreads() { return numWorkerThreads; }

StringConfigOption CoordinatorConfiguration::getLogLevel() { return logLevel; }

void CoordinatorConfiguration::setLogLevel(std::string logLevelValue) { logLevel->setValue(std::move(logLevelValue)); }

IntConfigOption CoordinatorConfiguration::getNumberOfBuffersInGlobalBufferManager() {
    return numberOfBuffersInGlobalBufferManager;
}

IntConfigOption CoordinatorConfiguration::getNumberOfBuffersPerWorker() { return numberOfBuffersPerWorker; }

IntConfigOption CoordinatorConfiguration::getNumberOfBuffersInSourceLocalBufferPool() {
    return numberOfBuffersInSourceLocalBufferPool;
}

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

void CoordinatorConfiguration::setQueryReconfiguration(bool queryReconfigurationState) {
    this->enableQueryReconfiguration->setValue(queryReconfigurationState);
}

BoolConfigOption CoordinatorConfiguration::getQueryReconfiguration() { return enableQueryReconfiguration; }

std::vector<LogicalSourcePtr> CoordinatorConfiguration::getLogicalSources() { return logicalSources; }

void CoordinatorConfiguration::setLogicalSources(std::vector<LogicalSourcePtr> logicalSources) {
    this->logicalSources = logicalSources;
}

void CoordinatorConfiguration::addLogicalSources(LogicalSourcePtr logicalSource) {
    this->logicalSources.emplace_back(logicalSource);
}

}// namespace Configurations
}// namespace NES
