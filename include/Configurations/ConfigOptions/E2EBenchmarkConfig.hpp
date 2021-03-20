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

#ifndef NES_E2EBenchmarkConfig_HPP
#define NES_E2EBenchmarkConfig_HPP

#include <map>
#include <string>

namespace NES {

class E2EBenchmarkConfig;
typedef std::shared_ptr<E2EBenchmarkConfig> E2EBenchmarkConfigPtr;

template<class T>
class ConfigOption;
typedef std::shared_ptr<ConfigOption<uint32_t>> IntConfigOption;
typedef std::shared_ptr<ConfigOption<std::string>> StringConfigOption;

/**
 * @brief object for storing worker configuration
 */
class E2EBenchmarkConfig {

  public:
    static E2EBenchmarkConfigPtr create();

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
    void resetOptions();

    /**
     * @brief set the value for numberOfWorkerThreads
     */
    void setNumberOfWorkerThreads(uint32_t numberOfWorkerThreads);

    /**
     * @brief gets a ConfigOption object with numberOfWorkerThreads
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getNumberOfWorkerThreads() const;

    /**
    * @brief set the value for numberOfCoordinatorThreads
    */
    void setNumberOfCoordinatorThreads(uint32_t numberOfCoordinatorThreads);

    /**
     * @brief gets a ConfigOption object with numberOfCoordinatorThreads
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getNumberOfCoordinatorThreads() const;

    /**
    * @brief set the value for numberOfBuffersToProduce
    */
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersToProduce
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getNumberOfBuffersToProduce() const;

    /**
     * @brief set the value for numberOfBuffersInGlobalBufferManager
     */
    void setNumberOfBuffersInGlobalBufferManager(uint32_t numberOfBuffersInGlobalBufferManager);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersInGlobalBufferManager
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getNumberOfBuffersInGlobalBufferManager() const;

    /**
     * @brief set the value for numberOfBuffersInTaskLocalBufferPool
     */
    void setNumberOfBuffersInTaskLocalBufferPool(uint32_t numberOfBuffersInTaskLocalBufferPool);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersInTaskLocalBufferPool
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getNumberOfBuffersInTaskLocalBufferPool() const;

    /**
     * @brief set the value for numberOfBuffersInSourceLocalBufferPool
     */
    void setNumberOfBuffersInSourceLocalBufferPool(uint32_t numberOfBuffersInSourceLocalBufferPool);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersInSourceLocalBufferPool
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getNumberOfBuffersInSourceLocalBufferPool() const;

    /**
       * @brief set the value for bufferSizeInBytes
    */
    void setBufferSizeInBytes(uint32_t bufferSizeInBytes);

    /**
     * @brief gets a ConfigOption object with bufferSizeInBytes
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getBufferSizeInBytes() const;

    /**
       * @brief set the value for numberOfSources
    */
    void setNumberOfSources(uint32_t numberOfSources);

    /**
     * @brief gets a ConfigOption object with numberOfSources
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getNumberOfSources() const;

    /**
     * @brief set the value for inputOutputMode
     */
    void setInputOutputMode(std::string inputOutputMode);

    /**
     * @brief gets a ConfigOption object with inputOutputMode
     */
    StringConfigOption getInputOutputMode();

    /**
     * @brief set the value for query
     */
    void setQuery(std::string query);

    /**
     * @brief gets a ConfigOption object with query
     */
    StringConfigOption getQuery();

    /**
      * @brief set the value for logLevel
      */
    void setLogLevel(std::string logLevel);

    /**
     * @brief gets a ConfigOption object with logLevel
     */
    StringConfigOption getLogLevel();

  private:
    /**
     * @brief constructor to create a new coordinator option object initialized with default values as set below
     */
    E2EBenchmarkConfig();

    IntConfigOption numberOfWorkerThreads;
    IntConfigOption numberOfCoordinatorThreads;
    IntConfigOption numberOfBuffersToProduce;
    IntConfigOption numberOfBuffersInGlobalBufferManager;
    IntConfigOption numberOfBuffersInTaskLocalBufferPool;
    IntConfigOption numberOfBuffersInSourceLocalBufferPool;
    IntConfigOption bufferSizeInBytes;
    IntConfigOption numberOfSources;

    StringConfigOption inputOutputMode;
    StringConfigOption query;
    StringConfigOption logLevel;
};

}// namespace NES

#endif//NES_E2EBenchmarkConfig_HPP
