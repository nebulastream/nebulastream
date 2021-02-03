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

#ifndef NES_WORKERCONFIG_HPP
#define NES_WORKERCONFIG_HPP

#include <Configurations/ConfigOption.hpp>
#include <map>
#include <string>
#include <thread>

namespace NES {

class WorkerConfig {

  public:
    /**
     * @brief constructor to create a new coordinator option object initialized with default values as set below
     */
    WorkerConfig();

    /**
     * @brief overwrite the default configurations with those loaded from a yaml file
     * @param filePath file path to the yaml file
     */
    void overwriteConfigWithYAMLFileInput(std::string filePath);

    /**
     * @brief overwrite the default and the yaml file configurations with command line input
     * @param inputParams map with key=command line parameter and value = value
     */
    void overwriteConfigWithCommandLineInput(std::map<std::string, std::string> inputParams);

    /**
     * @brief resets all options to default values
     */
    void resetWorkerOptions();

    /**
     * @brief gets a ConfigOption object with localWorkerIp
     */
    StringConfigOption getLocalWorkerIp();

    /**
     * @brief set the value for localWorkerIp with the appropriate data format
     */
    void setLocalWorkerIp(std::string localWorkerIp);

    /**
     * @brief gets a ConfigOption object with coordinatorIp
     */
    StringConfigOption getCoordinatorIp();

    /**
     * @brief set the value for coordinatorIp with the appropriate data format
     */
    void setCoordinatorIp(std::string coordinatorIp);

    /**
     * @brief gets a ConfigOption object with coordinatorPort
     */
    IntConfigOption getCoordinatorPort();

    /**
     * @brief set the value for coordinatorPort with the appropriate data format
     */
    void setCoordinatorPort(uint16_t coordinatorPort);

    /**
     * @brief gets a ConfigOption object with rpcPort
     */
    IntConfigOption getRpcPort();

    /**
     * @brief set the value for rpcPort with the appropriate data format
     */
    void setRpcPort(uint16_t rpcPort);

    /**
     * @brief gets a ConfigOption object with dataPort
     */
    IntConfigOption getDataPort();

    /**
     * @brief set the value for dataPort with the appropriate data format
     */
    void setDataPort(uint16_t dataPort);

    /**
     * @brief gets a ConfigOption object with numberOfSlots
     */
    IntConfigOption getNumberOfSlots();

    /**
     * @brief set the value for numberOfSlots with the appropriate data format
     */
    void setNumberOfSlots(uint16_t numberOfSlots);

    /**
     * @brief gets a ConfigOption object with numWorkerThreads
     */
    IntConfigOption getNumWorkerThreads();

    /**
     * @brief set the value for numWorkerThreads with the appropriate data format
     */
    void setNumWorkerThreads(uint16_t numWorkerThreads);

    /**
     * @brief gets a ConfigOption object with parentId
     */
    StringConfigOption getParentId();

    /**
     * @brief set the value for parentId with the appropriate data format
     */
    void setParentId(std::string parentId);

    /**
     * @brief gets a ConfigOption object with logLevel
     */
    StringConfigOption getLogLevel();

    /**
     * @brief set the value for logLevel with the appropriate data format
     */
    void setLogLevel(std::string logLevel);

  private:
    StringConfigOption localWorkerIp;
    StringConfigOption coordinatorIp;
    IntConfigOption coordinatorPort;
    IntConfigOption rpcPort;
    IntConfigOption dataPort;
    IntConfigOption numberOfSlots;
    IntConfigOption numWorkerThreads;
    StringConfigOption parentId;
    StringConfigOption logLevel;
};

}// namespace NES

#endif//NES_WORKERCONFIG_HPP
