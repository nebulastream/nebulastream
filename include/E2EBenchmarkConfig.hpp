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

#include "Configurations/ConfigOption.hpp"
#include <map>
#include <memory>
#include <string>

namespace NES {
class E2EBenchmarkConfig;
typedef std::shared_ptr<E2EBenchmarkConfig> E2EBenchmarkConfigPtr;
//template<class T>
//class ConfigOption;
typedef std::shared_ptr<NES::ConfigOption<uint32_t>> IntConfigOption;
typedef std::shared_ptr<NES::ConfigOption<std::string>> StringConfigOption;
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
    void setNumberOfWorkerThreads(std::string numberOfWorkerThreads);

    /**
     * @brief gets a ConfigOption object with numberOfWorkerThreads
     */
    const StringConfigOption getNumberOfWorkerThreads() const;

    /**
       * @brief set the value for numberOfSources
    */
    void setNumberOfSources(std::string numberOfSources);

    /**
     * @brief gets a ConfigOption object with numberOfSources
     */
    const StringConfigOption getNumberOfSources() const;

    /**
    * @brief set the value for numberOfBuffersToProduce
    */
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersToProduce
     */
    const IntConfigOption getNumberOfBuffersToProduce() const;

    /**
     * @brief set the value for numberOfBuffersInGlobalBufferManager
     */
    void setNumberOfBuffersInGlobalBufferManager(std::string numberOfBuffersInGlobalBufferManager);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersInGlobalBufferManager
     */
    const StringConfigOption getNumberOfBuffersInGlobalBufferManager() const;

    /**
     * @brief set the value for numberOfBuffersPerPipeline
     */
    void setNumberOfBuffersPerPipeline(std::string numberOfBuffersPerPipeline);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersPerPipeline
     */
    const StringConfigOption getNumberOfBuffersPerPipeline() const;

    /**
     * @brief set the value for numberOfBuffersInSourceLocalBufferPool
     */
    void setNumberOfBuffersInSourceLocalBufferPool(std::string numberOfBuffersInSourceLocalBufferPool);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersInSourceLocalBufferPool
     */
    const StringConfigOption getNumberOfBuffersInSourceLocalBufferPool() const;

    /**
       * @brief set the value for bufferSizeInBytes
    */
    void setBufferSizeInBytes(std::string bufferSizeInBytes);

    /**
     * @brief gets a ConfigOption object with bufferSizeInBytes
     */
    const StringConfigOption getBufferSizeInBytes() const;

    /**
     * @brief set the value for InputType
     */
    void setInputType(std::string InputType);

    /**
     * @brief gets a ConfigOption object with InputType
     */
    StringConfigOption getInputType();

    /**
    * @brief set the value for scalability
    */
    void setScalability(std::string scalability);

    /**
     * @brief gets a ConfigOption object with scalability
     */
    StringConfigOption getScalability();

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

    /**
    * @brief gets a ConfigOption object with outputFile
    */
    StringConfigOption getOutputFile() const;

    /**
    * @brief set the value for outputFile
    */
    void setOutputFile(std::string outputFile);

    /**
    * @brief gets a ConfigOption object with benchmarkName
    */
    StringConfigOption getBenchmarkName() const;

    /**
    * @brief set the value for benchmarkName
    */
    void setBenchmarkName(std::string benchmarkName);

    /**
    * @brief gets a ConfigOption object with experimentMeasureIntervalInSeconds
    */
    const IntConfigOption getExperimentMeasureIntervalInSeconds() const;

    /**
    * @brief set the value for experimentMeasureIntervalInSeconds
    */
    void setExperimentMeasureIntervalInSeconds(uint32_t experimentMeasureIntervalInSeconds);

    /**
    * @brief gets a ConfigOption object with startupSleepIntervalInSeconds
    */
    const IntConfigOption getStartupSleepIntervalInSeconds() const;

    /**
    * @brief set the value for startupSleepIntervalInSeconds
    */
    void setStartupSleepIntervalInSeconds(uint32_t startupSleepIntervalInSeconds);

    /**
    * @brief gets a ConfigOption object with numberOfMeasurementsToCollect
    */
    const IntConfigOption getNumberOfMeasurementsToCollect() const;

    /**
      * @brief set the value for numberOfMeasurementsToCollect
    */
    void setNumberOfMeasurementsToCollect(uint32_t numberOfMeasurementsToCollect);

    std::string toString();

    /**
     * @brief getter/setter for input mode
     * @return
     */
    const StringConfigOption& getGatheringMode() const;
    void setGatheringMode(const std::string gatheringMode);

    /**
     * @brief getter/setter for gathering values
     * @return
     */
    const StringConfigOption& getGatheringValues() const;
    void setGatheringValues(const std::string gatheringValues);

    /**
    * @brief getter/setter for source mode
    * @return
    */
    const StringConfigOption& getSourceMode() const;
    void setSourceMode(const std::string gatheringValues);

    /**
    * @brief getter/setter for sourcePinList
    * @return
    */
    const StringConfigOption& getSourcePinList() const;
    void setSourcePinList(const std::string list);

    /**
    * @brief getter/setter for workerPinList
    * @return
    */
    const StringConfigOption& getWorkerPinList() const;
    void setWorkerPinList(const std::string list);

  private:
    /**
     * @brief constructor to create a new coordinator option object initialized with default values as set below
     */
    E2EBenchmarkConfig();

    //parameter that can be changed per run
    StringConfigOption numberOfWorkerThreads;
    StringConfigOption numberOfSources;

    //parameter that are valid for the entire run for the config of the worker and coordinator
    IntConfigOption numberOfBuffersToProduce;
    StringConfigOption numberOfBuffersInGlobalBufferManager;
    StringConfigOption numberOfBuffersPerPipeline;
    StringConfigOption numberOfBuffersInSourceLocalBufferPool;
    StringConfigOption bufferSizeInBytes;
    StringConfigOption query;
    StringConfigOption inputType;
    StringConfigOption gatheringMode;
    StringConfigOption gatheringValues;
    StringConfigOption sourceMode;

    StringConfigOption sourcePinList;
    StringConfigOption workerPinList;

    StringConfigOption scalability;

    //general benchmark setup parameter
    StringConfigOption outputFile;
    StringConfigOption benchmarkName;
    StringConfigOption logLevel;

    IntConfigOption experimentMeasureIntervalInSeconds;
    IntConfigOption startupSleepIntervalInSeconds;
    IntConfigOption numberOfMeasurementsToCollect;
};
}// namespace NES
#endif//NES_E2EBenchmarkConfig_HPP
