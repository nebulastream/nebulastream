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

#ifndef NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_WORKER_CONFIG_HPP_
#define NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_WORKER_CONFIG_HPP_

#include <Configurations/ConfigurationOption.hpp>
#include <map>
#include <string>

namespace NES {

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace Configurations {

class WorkerConfiguration;
using WorkerConfigurationPtr = std::shared_ptr<WorkerConfiguration>;

/**
 * @brief object for storing worker configuration
 */
class WorkerConfiguration {

  public:
    /**
     * @brief constructor to create a new coordinator option object initialized with default values as set below
     */
    explicit WorkerConfiguration();

    static WorkerConfigurationPtr create();

    /**
     * @brief overwrite the default configurations with those loaded from a yaml file
     * @param filePath file path to the yaml file
     */
    void overwriteConfigWithYAMLFileInput(const std::string& filePath);

    /**
     * @brief overwrite the default and the yaml file configurations with command line input
     * @param commandLineArguments map with key=command line parameter and value = value
     */
    void overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& commandLineArguments);

    /**
     * @brief resets all options to default values
     */
    void resetWorkerOptions();

    /**
     * @brief prints the current worker configuration (name: current value)
     */
    std::string toString();

    /**
     * @brief gets a ConfigurationOption object with localWorkerIp
     */
    StringConfigOption getLocalWorkerIp();

    /**
     * @brief set the value for localWorkerIp with the appropriate data format
     */
    void setLocalWorkerIp(std::string localWorkerIp);

    /**
     * @brief gets a ConfigurationOption object with coordinatorIp
     */
    StringConfigOption getCoordinatorIp();

    /**
     * @brief set the value for coordinatorIp with the appropriate data format
     */
    void setCoordinatorIp(std::string coordinatorIp);

    /**
     * @brief gets a ConfigurationOption object with coordinatorPort
     */
    IntConfigOption getCoordinatorPort();

    /**
     * @brief set the value for coordinatorPort with the appropriate data format
     */
    void setCoordinatorPort(uint16_t coordinatorPort);

    /**
     * @brief gets a ConfigurationOption object with rpcPort
     */
    IntConfigOption getRpcPort();

    /**
     * @brief set the value for rpcPort with the appropriate data format
     */
    void setRpcPort(uint16_t rpcPort);

    /**
     * @brief gets a ConfigurationOption object with dataPort
     */
    IntConfigOption getDataPort();

    /**
     * @brief set the value for dataPort with the appropriate data format
     */
    void setDataPort(uint16_t dataPort);

    /**
     * @brief gets a ConfigurationOption object with numberOfSlots
     */
    IntConfigOption getNumberOfSlots();

    /**
     * @brief set the value for numberOfSlots with the appropriate data format
     */
    void setNumberOfSlots(uint16_t numberOfSlots);

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
     * @brief gets a ConfigurationOption object with parentId
     */
    StringConfigOption getParentId();

    /**
     * @brief set the value for parentId with the appropriate data format
     */
    void setParentId(std::string parentId);

    /**
     * @brief gets a ConfigurationOption object with logLevel
     */
    StringConfigOption getLogLevel();

    /**
     * @brief set the value for logLevel with the appropriate data format
     */
    void setLogLevel(std::string logLevel);

    /**
     * @brief gets the configuration for the query compiler execution mode
     */
    [[nodiscard]] const StringConfigOption getQueryCompilerCompilationStrategy() const;

    /**
     * @brief sets the configuration for the query compiler execution mode
     * @param queryCompilerExecutionMode
     */
    void setQueryCompilerCompilationStrategy(std::string queryCompilerCompilationStrategy);

    /**
     * @brief gets the configuration for the query compiler execution mode
     */
    [[nodiscard]] const StringConfigOption getQueryCompilerPipeliningStrategy() const;

    /**
     * @brief sets the configuration for the query compiler execution mode
     * @param queryCompilerPipeliningStrategy
     */
    void setQueryCompilerPipeliningStrategy(std::string queryCompilerPipeliningStrategy);

    /**
    * @brief gets the configuration for the query compiler buffer allocation strategy
    */
    [[nodiscard]] const StringConfigOption getQueryCompilerOutputBufferAllocationStrategy() const;

    /**
    * @brief sets the configuration for the query compiler buffer allocation strategy
    * @param queryCompilerExecutionMode
    */
    void setQueryCompilerOutputBufferAllocationStrategy(std::string queryCompilerOutputBufferAllocationStrategy);

    /**
    * @brief getter/setter for sourcePinList
    * @return
    */
    [[nodiscard]] const StringConfigOption& getSourcePinList() const;
    void setSourcePinList(std::string list);

    /**
    * @brief getter/setter for workerPinList
    * @return
    */
    [[nodiscard]] const StringConfigOption& getWorkerPinList() const;
    void setWorkerPinList(std::string list);

    /**
    * @brief getter/setter for numa awareness
    * @return
    */
    [[nodiscard]] bool isNumaAware() const;
    void setNumaAware(bool status);

    /**
    * @brief getter/setter to check if monitoring is enabled
    * @return
    */
    BoolConfigOption getEnableMonitoring();
    void setEnableMonitoring(bool enableMonitoring);

    /**
    * @brief getter/setter to obtain physicalSources
    * @return
    */
    std::vector<PhysicalSourcePtr> getPhysicalSources();

    /**
     * @brief Set physical stream configurations
     * @param physicalSources: vector of physical stream configurations
     */
    void setPhysicalSources(std::vector<PhysicalSourcePtr> physicalSources);

    /**
     * @brief add a physical stream configuration to the worker configuration
     * @param physicalSource: physical source configuration to add
     */
    void addPhysicalSource(PhysicalSourcePtr physicalSource);

  private:
    StringConfigOption localWorkerIp;
    StringConfigOption coordinatorIp;
    IntConfigOption coordinatorPort;
    IntConfigOption rpcPort;
    IntConfigOption dataPort;
    IntConfigOption numberOfSlots;
    IntConfigOption numWorkerThreads;
    IntConfigOption numberOfBuffersInGlobalBufferManager;
    IntConfigOption numberOfBuffersPerWorker;
    IntConfigOption numberOfBuffersInSourceLocalBufferPool;
    IntConfigOption bufferSizeInBytes;
    StringConfigOption parentId;
    StringConfigOption logLevel;
    // indicates the compilation strategy of the query compiler [FAST|DEBUG|OPTIMIZE].
    StringConfigOption queryCompilerCompilationStrategy;
    // indicates the pipelining strategy for the query compiler [OPERATOR_FUSION, OPERATOR_AT_A_TIME].
    StringConfigOption queryCompilerPipeliningStrategy;
    // indicates, which output buffer allocation strategy should be used.
    StringConfigOption queryCompilerOutputBufferOptimizationLevel;
    // numa awareness
    BoolConfigOption numaAwareness;
    // enable monitoring
    BoolConfigOption enableMonitoring;
    StringConfigOption sourcePinList;
    StringConfigOption workerPinList;
    std::vector<PhysicalSourcePtr> physicalSources;
};
}// namespace Configurations
}// namespace NES

#endif// NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_WORKER_CONFIG_HPP_
