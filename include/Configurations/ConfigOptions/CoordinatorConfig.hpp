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
typedef std::shared_ptr<CoordinatorConfig> CoordinatorConfigPtr;

template<class T>
class ConfigOption;
typedef std::shared_ptr<ConfigOption<uint32_t>> IntConfigOption;
typedef std::shared_ptr<ConfigOption<std::string>> StringConfigOption;
typedef std::shared_ptr<ConfigOption<bool>> BoolConfigOption;

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
    void overwriteConfigWithYAMLFileInput(std::string filePath);

    /**
     * @brief overwrite the default and the yaml file configurations with command line input
     * @param inputParams map with key=command line parameter and value = value
     */
    void overwriteConfigWithCommandLineInput(std::map<std::string, std::string> inputParams);

    /**
     * @brief resets all options to default values
     */
    void resetCoordinatorOptions();

    /**
     * @brief gets a ConfigOption object with restIp
     */
    StringConfigOption getRestIp();

    /**
     * @brief set the value for rest ip with the appropriate data format
     */
    void setRestIp(std::string restIp);

    /**
     * @brief gets a ConfigOption object with coordinatorip
     */
    StringConfigOption getCoordinatorIp();

    /**
     * @brief set the value for coordinatorIp with the appropriate data format
     */
    void setCoordinatorIp(std::string coordinatorIp);

    /**
     * @brief gets a ConfigOption object with rpcPort
     */
    IntConfigOption getRpcPort();

    /**
     * @brief set the value for rpcPort with the appropriate data format
     */
    void setRpcPort(uint16_t rpcPort);

    /**
     * @brief gets a ConfigOption object with rest port
     */
    IntConfigOption getRestPort();

    /**
     * @brief set the value for restPort with the appropriate data format
     */
    void setRestPort(uint16_t restPort);

    /**
     * @brief gets a ConfigOption object with data port
     */
    IntConfigOption getDataPort();

    /**
     * @brief set the value for dataPort with the appropriate data format
     */
    void setDataPort(uint16_t dataPort);

    /**
     * @brief gets a ConfigOption object with number of slots
     */
    IntConfigOption getNumberOfSlots();

    /**
     * @brief set the value for numberOfSlots with the appropriate data format
     */
    void setNumberOfSlots(uint16_t numberOfSlots);

    /**
     * @brief gets a ConfigOption object with enable query merging
     */
    BoolConfigOption getEnableQueryMerging();

    /**
     * @brief set the value for enableQueryMerging with the appropriate data format
     */
    void setEnableQueryMerging(bool enableQueryMerging);

    /**
     * @brief gets a ConfigOption object with log level
     */
    StringConfigOption getLogLevel();

    /**
     * @brief set the value for logLevel with the appropriate data format
     */
    void setLogLevel(std::string logLevel);

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
    BoolConfigOption enableQueryMerging;
    StringConfigOption logLevel;
};

}// namespace NES

#endif//NES_COORDINATORCONFIG_HPP
