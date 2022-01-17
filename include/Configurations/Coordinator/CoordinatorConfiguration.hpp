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

#ifndef NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_COORDINATOR_CONFIG_HPP_
#define NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_COORDINATOR_CONFIG_HPP_

#include <Configurations/ConfigurationOption.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>

namespace NES {

namespace Configurations {

const std::string REST_PORT_CONFIG = "restPort";
const std::string RPC_PORT_CONFIG = "rpcPort"; //used to be coordinator port, renamed to uniform naming
const std::string DATA_PORT_CONFIG = "dataPort";
const std::string REST_IP_CONFIG = "restIp";
const std::string COORDINATOR_IP_CONFIG = "coordinatorIp";
const std::string NUMBER_OF_SLOTS_CONFIG = "numberOfSlots";
const std::string LOG_LEVEL_CONFIG = "logLevel";
const std::string NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG = "numberOfBuffersInGlobalBufferManager";
const std::string NUMBER_OF_BUFFERS_PER_WORKER_CONFIG = "numberOfBuffersPerWorker";
const std::string NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG = "numberOfBuffersInSourceLocalBufferPool";
const std::string BUFFERS_SIZE_IN_BYTES_CONFIG = "bufferSizeInBytes";
const std::string QUERY_BATCH_SIZE_CONFIG = "queryBatchSize";
const std::string QUERY_MERGER_RULE_CONFIG = "queryMergerRule";
const std::string ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG = "enableSemanticQueryValidation";
const std::string ENABLE_MONITORING_CONFIG = "enableMonitoring";
const std::string NUM_WORKER_THREADS_CONFIG = "numWorkerThreads";
const std::string MEMORY_LAYOUT_POLICY_CONFIG = "memoryLayoutPolicy";

class CoordinatorConfiguration;
using CoordinatorConfigPtr = std::shared_ptr<CoordinatorConfiguration>;
using IntConfigOption = std::shared_ptr<ConfigurationOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigurationOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigurationOption<bool>>;

/**
 * @brief ConfigOptions for Coordinator
 */
class CoordinatorConfiguration {

  public:
    static CoordinatorConfigPtr create();
    CoordinatorConfiguration();

    /**
     * @brief overwrite the default configurations with those loaded from a yaml file
     * @param filePath file path to the yaml file
     */
    void overwriteConfigWithYAMLFileInput(const std::string& filePath);

    /**
     * @brief overwrite the default and the yaml file configurations with command line input
     * @param inputParams map with key=command line parameter and value = value
     */
    void overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams);

    /**
     * @brief resets all options to default values
     */
    void resetCoordinatorOptions();

    /**
     * @brief prints the current coordinator configuration (name: current value)
     */
    std::string toString();

    /**
     * @brief gets a ConfigurationOption object with restIp
     * @return Configuration option for rest IP
     */
    StringConfigOption getRestIp();

    /**
     * @brief set the value for rest ip with the appropriate data format
     * @param restIp: the rest ip address
     */
    void setRestIp(std::string restIp);

    /**
     * @brief gets a ConfigurationOption object with coordinatorip
     * @return Config option for coordinator ip
     */
    StringConfigOption getCoordinatorIp();

    /**
     * @brief set the value for coordinatorIp with the appropriate data format
     * @param coordinatorIp: the coordinator ip
     */
    void setCoordinatorIp(std::string coordinatorIp);

    /**
     * @brief gets a ConfigurationOption object with rpcPort
     * @return config option for RPC port
     */
    IntConfigOption getRpcPort();

    /**
     * @brief set the value for rpcPort with the appropriate data format
     * @param rpcPort: the rpc port
     */
    void setRpcPort(uint16_t rpcPort);

    /**
     * @brief gets a ConfigurationOption object with rest port
     * @return config option for rest port
     */
    IntConfigOption getRestPort();

    /**
     * @brief set the value for restPort with the appropriate data format
     * @param restPort: the rest port
     */
    void setRestPort(uint16_t restPort);

    /**
     * @brief gets a ConfigurationOption object with data port
     * @return config option for data port
     */
    IntConfigOption getDataPort();

    /**
     * @brief set the value for dataPort with the appropriate data format
     * @param dataPort: data port
     */
    void setDataPort(uint16_t dataPort);

    /**
     * @brief gets a ConfigurationOption object with number of slots
     * @return config option for number of slots
     */
    IntConfigOption getNumberOfSlots();

    /**
     * @brief set the value for numberOfSlots with the appropriate data format
     * @param numberOfSlots: number of slots
     */
    void setNumberOfSlots(uint16_t numberOfSlots);

    /**
     * @brief gets a ConfigurationOption object with log level
     * @return Config option for Log level for coordinator
     */
    StringConfigOption getLogLevel();

    /**
     * @brief set the value for logLevel with the appropriate data format
     * @param logLevel: the log level value
     */
    void setLogLevel(std::string logLevel);

    /**
    * @brief gets a ConfigurationOption object with numWorkerThreads
    */
    IntConfigOption getNumWorkerThreads();

    /**
     * @brief set the value for numWorkerThreads with the appropriate data format
     */
    void setNumWorkerThreads(uint16_t numWorkerThreads);

    /**
    * @brief gets a ConfigurationOption object with buffer size in bytes
    */
    IntConfigOption getBufferSizeInBytes();

    /**
     * @brief set the value for buffer size in bytes with the appropriate data format
     */
    void setBufferSizeInBytes(uint64_t sizeInBytes);

    /**
    * @brief gets a ConfigurationOption object with number of numberOfBuffersInGlobalBufferManager
    */
    IntConfigOption getNumberOfBuffersInGlobalBufferManager();

    /**
    * @brief gets a ConfigurationOption object with number of numberOfBuffersPerWorker
    */
    IntConfigOption getNumberOfBuffersPerWorker();

    /**
    * @brief gets a ConfigurationOption object with number of numberOfBuffersInSourceLocalBufferPool
    */
    IntConfigOption getNumberOfBuffersInSourceLocalBufferPool();

    /**
     * @brief set the value for number of numberOfBuffersInGlobalBufferManager
     */
    void setNumberOfBuffersInGlobalBufferManager(uint64_t count);

    /**
     * @brief set the value for number of numberOfBuffersPerWorker
     */
    void setNumberOfBuffersPerWorker(uint64_t count);

    /**
     * @brief set the value for number of numberOfBuffersInSourceLocalBufferPool
     */
    void setNumberOfBuffersInSourceLocalBufferPool(uint64_t count);

    /**
     * @brief Get the query batch size
     * @return query batch size
     */
    IntConfigOption getQueryBatchSize();

    /**
     * @brief Set the number of queries to be processed together
     * @param batchSize: the batch size
     */
    void setQueryBatchSize(uint32_t batchSize);

    /**
     * @brief Get the query merger rule
     * @return query merger rule selected by user
     */
    StringConfigOption getQueryMergerRule();

    /**
     * @brief Set the query merger rule
     * @param queryMergerRule : the query merger rule name
     */
    void setQueryMergerRule(std::string queryMergerRule);

    /**
     * @brief Get the value for enabling SemanticQueryValidation
     * @return semantic validation config option
     */
    BoolConfigOption getEnableSemanticQueryValidation();

    /**
     * @brief Set the value for enabling SemanticQueryValidation
     * @param enableSemanticQueryValidation: enable or disable semantic validation
     */
    void setEnableSemanticQueryValidation(bool enableSemanticQueryValidation);

    /**
     * @brief Get the value for enabling monitoring.
     * @return monitoring config option
     */
    BoolConfigOption getEnableMonitoring();

    /**
     * Set the value for enabling monitoring.
     * @param enableMonitoring enable or disable monitoring.
     */
    void setEnableMonitoring(bool enableMonitoring);

    /**
    * @brief Get the memory layout policy
    * @return memory layout policy
   */
    StringConfigOption getMemoryLayoutPolicy();

    /**
     * @brief Set the memory layout policy
     * @param memoryLayoutPolicy
     */
    void setMemoryLayoutPolicy(std::string memoryLayoutPolicy);

    /**
     * @brief fetch the values for PerformOnlySourceOperatorExpansion
     * @return bool configuration value
     */
    BoolConfigOption getPerformOnlySourceOperatorExpansion();

    /**
     * @brief set the value for PerformOnlySourceOperatorExpansion
     * @param performOnlySourceOperatorExpansion
     */
    void setPerformOnlySourceOperatorExpansion(bool performOnlySourceOperatorExpansion);

  private:
    /**
     * @brief constructor to create a new coordinator option object initialized with default values as set below
     */

    StringConfigOption restIp;
    StringConfigOption coordinatorIp;
    IntConfigOption rpcPort;
    IntConfigOption restPort;
    IntConfigOption dataPort;
    IntConfigOption numberOfSlots;
    IntConfigOption numberOfBuffersInGlobalBufferManager;
    IntConfigOption numberOfBuffersPerWorker;
    IntConfigOption numberOfBuffersInSourceLocalBufferPool;
    IntConfigOption bufferSizeInBytes;
    IntConfigOption numWorkerThreads;
    StringConfigOption logLevel;
    IntConfigOption queryBatchSize;
    StringConfigOption queryMergerRule;
    BoolConfigOption enableMonitoring;
    StringConfigOption memoryLayoutPolicy;
    BoolConfigOption performOnlySourceOperatorExpansion;

    // temorary flag:
    BoolConfigOption enableSemanticQueryValidation;
};

}// namespace Configurations
}// namespace NES

#endif// NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_COORDINATOR_CONFIG_HPP_
