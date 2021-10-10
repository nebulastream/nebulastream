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

#ifndef NES_COORDINATORCONFIG_HPP
#define NES_COORDINATORCONFIG_HPP

#include <map>
#include <string>
#include <thread>

namespace NES {

class CoordinatorConfig;
using CoordinatorConfigPtr = std::shared_ptr<CoordinatorConfig>;

template<class T>
class ConfigOption;
using IntConfigOption = std::shared_ptr<ConfigOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigOption<bool>>;

/**
 * @brief ConfigOptions for Coordinator
 */
class CoordinatorConfig {

  public:
    static CoordinatorConfigPtr create();

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
     * @brief gets a ConfigOption object with restIp
     * @return Configuration option for rest IP
     */
    StringConfigOption getRestIp();

    /**
     * @brief set the value for rest ip with the appropriate data format
     * @param restIp: the rest ip address
     */
    void setRestIp(std::string restIp);

    /**
     * @brief gets a ConfigOption object with coordinatorip
     * @return Config option for coordinator ip
     */
    StringConfigOption getCoordinatorIp();

    /**
     * @brief set the value for coordinatorIp with the appropriate data format
     * @param coordinatorIp: the coordinator ip
     */
    void setCoordinatorIp(std::string coordinatorIp);

    /**
     * @brief gets a ConfigOption object with rpcPort
     * @return config option for RPC port
     */
    IntConfigOption getRpcPort();

    /**
     * @brief set the value for rpcPort with the appropriate data format
     * @param rpcPort: the rpc port
     */
    void setRpcPort(uint16_t rpcPort);

    /**
     * @brief gets a ConfigOption object with rest port
     * @return config option for rest port
     */
    IntConfigOption getRestPort();

    /**
     * @brief set the value for restPort with the appropriate data format
     * @param restPort: the rest port
     */
    void setRestPort(uint16_t restPort);

    /**
     * @brief gets a ConfigOption object with data port
     * @return config option for data port
     */
    IntConfigOption getDataPort();

    /**
     * @brief set the value for dataPort with the appropriate data format
     * @param dataPort: data port
     */
    void setDataPort(uint16_t dataPort);

    /**
     * @brief gets a ConfigOption object with number of slots
     * @return config option for number of slots
     */
    IntConfigOption getNumberOfSlots();

    /**
     * @brief set the value for numberOfSlots with the appropriate data format
     * @param numberOfSlots: number of slots
     */
    void setNumberOfSlots(uint16_t numberOfSlots);

    /**
     * @brief gets a ConfigOption object with enable query merging
     * @param Config option for query merging
     */
    BoolConfigOption getEnableQueryMerging();

    /**
     * @brief set the value for enableQueryMerging with the appropriate data format
     * @param enableQueryMerging: enable or disable query merging
     */
    void setEnableQueryMerging(bool enableQueryMerging);

    /**
     * @brief gets a ConfigOption object with log level
     * @return Config option for Log level for coordinator
     */
    StringConfigOption getLogLevel();

    /**
     * @brief set the value for logLevel with the appropriate data format
     * @param logLevel: the log level value
     */
    void setLogLevel(std::string logLevel);

    /**
    * @brief gets a ConfigOption object with numWorkerThreads
    */
    IntConfigOption getNumWorkerThreads();

    /**
     * @brief set the value for numWorkerThreads with the appropriate data format
     */
    void setNumWorkerThreads(uint16_t numWorkerThreads);

    /**
    * @brief gets a ConfigOption object with buffer size in bytes
    */
    IntConfigOption getBufferSizeInBytes();

    /**
     * @brief set the value for buffer size in bytes with the appropriate data format
     */
    void setBufferSizeInBytes(uint64_t sizeInBytes);

    /**
    * @brief gets a ConfigOption object with number of numberOfBuffersInGlobalBufferManager
    */
    IntConfigOption getNumberOfBuffersInGlobalBufferManager();

    /**
    * @brief gets a ConfigOption object with number of numberOfBuffersPerPipeline
    */
    IntConfigOption getnumberOfBuffersPerPipeline();

    /**
    * @brief gets a ConfigOption object with number of numberOfBuffersInSourceLocalBufferPool
    */
    IntConfigOption getNumberOfBuffersInSourceLocalBufferPool();

    /**
     * @brief set the value for number of numberOfBuffersInGlobalBufferManager
     */
    void setNumberOfBuffersInGlobalBufferManager(uint64_t count);

    /**
     * @brief set the value for number of numberOfBuffersPerPipeline
     */
    void setnumberOfBuffersPerPipeline(uint64_t count);

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
     * @brief Get the query batch size
     * @return query batch size
     */
    IntConfigOption getLocationUpdateInterval();

    /**
     * @brief Set the number of queries to be processed together
     * @param batchSize: the batch size
     */
    void setLocationUpdateInterval(uint32_t interval);

    /**
     * @brief Get the max number of points that each node can store
     * @return location storage size
     */
    IntConfigOption getNumberOfPointsInLocationStorage();

    /**
     * @brief Set the storage size for all location storages
     * @param storageSize: the storage size
     */
    void setNumberOfPointsInLocationStorage(uint32_t storageSize);

    /**
     * @brief Enables a dynamic duplicates filter on the sink
     * @return boolean flag
     */
    BoolConfigOption getDynamicDuplicatesFilterEnabled();

    /**
     * @brief Get the value for enabling route prediction
     * @return route prediction config option
     */
    BoolConfigOption getRoutePredictionEnabled();

    /**
     * @brief Set the value for enabling route prediction
     * @param routePredictionEnabled: enable or disable route prediction
     */
    void setRoutePredictionEnabled(bool routePredictionEnabled);

    /**
     * @brief Set the if the dynamic duplicates filter is enabled
     * @param dynamicDuplicateFilterEnabled: activation flag
     */
    void setDynamicDuplicatesFilterEnabled(bool dynamicDuplicateFilterEnabled);

    /**
    * @brief Get the max number of points that the filter storage should keep
    * @return filter storage size
    */
    IntConfigOption getNumberOfTuplesInFilterStorage();

    /**
     * @brief Set the storage size for the filter cache
     * @param storageSize: the storage size
     */
    void setNumberOfTuplesInFilterStorage(uint32_t storageSize);


    /**
     * @brief Get the value for enabling SemanticQueryValidation
     * @return semantic validation config option
     */
    BoolConfigOption getEnableSemanticQueryValidation();

    /**
     * @brief Set the value for enableing SemanticQueryValidation
     * @param enableSemanticQueryValidation: enable or disable semantic validation
     */
    void setEnableSemanticQueryValidation(bool enableSemanticQueryValidation);

  private:
    /**
     * @brief constructor to create a new coordinator option object initialized with default values as set below
     */
    CoordinatorConfig();
    StringConfigOption restIp;
    StringConfigOption coordinatorIp;
    IntConfigOption rpcPort;
    IntConfigOption restPort;
    IntConfigOption dataPort;
    IntConfigOption numberOfSlots;
    IntConfigOption numberOfBuffersInGlobalBufferManager;
    IntConfigOption numberOfBuffersPerPipeline;
    IntConfigOption numberOfBuffersInSourceLocalBufferPool;
    IntConfigOption bufferSizeInBytes;
    BoolConfigOption enableQueryMerging;
    IntConfigOption numWorkerThreads;
    StringConfigOption logLevel;
    IntConfigOption queryBatchSize;
    StringConfigOption queryMergerRule;
    IntConfigOption locationUpdateInterval;
    IntConfigOption numberOfPointsInLocationStorage;
    BoolConfigOption dynamicDuplicatesFilterEnabled;
    BoolConfigOption routePredictionEnabled;
    IntConfigOption numberOfTuplesInFilterStorage;

    // temorary flag:
    BoolConfigOption enableSemanticQueryValidation;
};

}// namespace NES

#endif//NES_COORDINATORCONFIG_HPP
