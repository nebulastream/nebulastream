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

#include <Configurations/ConfigOption.hpp>
#include <map>
#include <string>
#include <thread>

using namespace std;

namespace NES {
/**
 * @brief ConfigOptions for Coordinator
 */
class CoordinatorConfig {

  public:
    /**
     * @brief constructor to create a new coordinator option object initialized with default values as set below
     */
    CoordinatorConfig();

    /**
     * @brief overwrite the default configurations with those loaded from a yaml file
     * @param filePath file path to the yaml file
     */
    void overwriteConfigWithYAMLFileInput(string filePath);
    /**
     * @brief overwrite the default and the yaml file configurations with command line input
     * @param inputParams map with key=command line parameter and value = value
     */
    void overwriteConfigWithCommandLineInput(map<string, string> inputParams);
    /**
     * @brief resets all options to default values
     */
    void resetCoordinatorOptions();

    /**
     * @brief gets a ConfigOption object with restIp
     */
    const ConfigOption<std::string>& getRestIp() const;
    /**
     * @brief set the value for rest ip with the appropriate data format
     */
    void setRestIp(const string& restIp);
    /**
    * @brief gets a ConfigOption object with coordinatorip
    */
    const ConfigOption<std::string>& getCoordinatorIp() const;
    /**
    * @brief set the value for coordinatorIp with the appropriate data format
    */
    void setCoordinatorIp(const string& coordinatorIp);
    /**
    * @brief gets a ConfigOption object with rpcPort
    */
    const ConfigOption<uint16_t>& getRpcPort() const;
    /**
    * @brief set the value for rpcPort with the appropriate data format
    */
    void setRpcPort(const uint16_t& rpcPort);
    /**
    * @brief gets a ConfigOption object with rest port
    */
    const ConfigOption<uint16_t>& getRestPort() const;
    /**
    * @brief set the value for restPort with the appropriate data format
    */
    void setRestPort(const uint16_t& restPort);
    /**
    * @brief gets a ConfigOption object with data port
    */
    const ConfigOption<uint16_t>& getDataPort() const;
    /**
    * @brief set the value for dataPort with the appropriate data format
    */
    void setDataPort(const uint16_t& dataPort);
    /**
    * @brief gets a ConfigOption object with number of slots
    */
    const ConfigOption<uint16_t>& getNumberOfSlots() const;
    /**
    * @brief set the value for numberOfSlots with the appropriate data format
    */
    void setNumberOfSlots(const uint16_t& numberOfSlots);
    /**
    * @brief gets a ConfigOption object with enable query merging
    */
    const ConfigOption<bool>& getEnableQueryMerging() const;
    /**
    * @brief set the value for enableQueryMerging with the appropriate data format
    */
    void setEnableQueryMerging(const bool& enableQueryMerging);
    /**
    * @brief gets a ConfigOption object with log level
    */
    const ConfigOption<std::string>& getLogLevel() const;
    /**
    * @brief set the value for logLevel with the appropriate data format
    */
    void setLogLevel(const string& logLevel);
/**
 * @brief definition of options and information for coordinator config options
 */
  private:

    ConfigOption<std::string> restIp =
        ConfigOption("restIp", std::string("127.0.0.1"), "NES ip of the REST server.", "string", false);

    ConfigOption<std::string> coordinatorIp =
        ConfigOption("coordinatorIp", std::string("127.0.0.1"), "RPC IP address of NES Coordinator.", "string", false);

    ConfigOption<uint16_t> rpcPort =
        ConfigOption("rpcPort", uint16_t(4000), "RPC server port of the NES Coordinator", "uint16_t", false);

    ConfigOption<uint16_t> restPort =
        ConfigOption("restPort", uint16_t(8081), "Port exposed for rest endpoints", "uint16_t", false);

    ConfigOption<uint16_t> dataPort = ConfigOption("dataPort", uint16_t(3001), "NES data server port", "uint16_t", false);

    ConfigOption<uint16_t> numberOfSlots = ConfigOption("numberOfSlots", uint16_t(std::thread::hardware_concurrency()),
                                                        "Number of computing slots for NES Coordinator", "uint16_t", false);

    ConfigOption<bool> enableQueryMerging =
        ConfigOption("enableQueryMerging", false, "Enable Query Merging Feature", "bool", false);

    ConfigOption<std::string> logLevel =
        ConfigOption("logLevel", std::string("LOG_DEBUG"),
                     "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)", "string", false);
};

}// namespace NES

#endif//NES_COORDINATORCONFIG_HPP
