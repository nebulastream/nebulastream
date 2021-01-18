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

using namespace std;

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
    void overwriteConfigWithYAMLFileInput(string filePath);
    /**
 * @brief overwrite the default and the yaml file configurations with command line input
 * @param inputParams map with key=command line parameter and value = value
 */
    void overwriteConfigWithCommandLineInput(map<string, string> inputParams);
    /**
 * @brief resets all options to default values
 */
    void resetWorkerOptions();

    /**
 * @brief gets a ConfigOption object with localWorkerIp
 */
    const ConfigOption<std::string>& getLocalWorkerIp() const;
    /**
 * @brief set the value for localWorkerIp with the appropriate data format
 */
    void setLocalWorkerIp(const string& localWorkerIp);
    /**
 * @brief gets a ConfigOption object with coordinatorIp
 */
    const ConfigOption<std::string>& getCoordinatorIp() const;
    /**
 * @brief set the value for coordinatorIp with the appropriate data format
 */
    void setCoordinatorIp(const string& coordinatorIp);
    /**
 * @brief gets a ConfigOption object with coordinatorPort
 */
    const ConfigOption<uint16_t>& getCoordinatorPort() const;
    /**
 * @brief set the value for coordinatorPort with the appropriate data format
 */
    void setCoordinatorPort(const uint16_t& coordinatorPort);
    /**
 * @brief gets a ConfigOption object with rpcPort
 */
    const ConfigOption<uint16_t>& getRpcPort() const;
    /**
 * @brief set the value for rpcPort with the appropriate data format
 */
    void setRpcPort(const uint16_t& rpcPort);
    /**
 * @brief gets a ConfigOption object with dataPort
 */
    const ConfigOption<uint16_t>& getDataPort() const;
    /**
 * @brief set the value for dataPort with the appropriate data format
 */
    void setDataPort(const uint16_t& dataPort);
    /**
 * @brief gets a ConfigOption object with numberOfSlots
 */
    const ConfigOption<uint16_t>& getNumberOfSlots() const;
    /**
 * @brief set the value for numberOfSlots with the appropriate data format
 */
    void setNumberOfSlots(const uint16_t& numberOfSlots);
    /**
 * @brief gets a ConfigOption object with numWorkerThreads
 */
    const ConfigOption<uint16_t>& getNumWorkerThreads() const;
    /**
 * @brief set the value for numWorkerThreads with the appropriate data format
 */
    void setNumWorkerThreads(const uint16_t& numWorkerThreads);
    /**
 * @brief gets a ConfigOption object with parentId
 */
    const ConfigOption<std::string>& getParentId() const;
    /**
 * @brief set the value for parentId with the appropriate data format
 */
    void setParentId(const string& parentId);
    /**
 * @brief gets a ConfigOption object with logLevel
 */
    const ConfigOption<std::string>& getLogLevel() const;
    /**
 * @brief set the value for logLevel with the appropriate data format
 */
    void setLogLevel(const std::string& logLevel);

    /**
 * @brief definition of options and information for coordinator config options
 */
  private:
    ConfigOption<std::string> localWorkerIp =
        ConfigOption("localWorkerIp", std::string("127.0.0.1"), "Worker IP.", "string", false);

    ConfigOption<std::string> coordinatorIp =
        ConfigOption("coordinatorIp", std::string("127.0.0.1"),
                     "Server IP of the NES Coordinator to which the NES Worker should connect.", "string", false);

    ConfigOption<uint16_t> coordinatorPort =
        ConfigOption("coordinatorPort", uint16_t(4000),
                     "RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs "
                     "to be the same as rpcPort in Coordinator.",
                     "uint16_t", false);

    ConfigOption<uint16_t> rpcPort =
        ConfigOption("rpcPort", uint16_t(3000), "RPC server port of the NES Worker.", "uint16_t", false);

    ConfigOption<uint16_t> dataPort = ConfigOption("dataPort", uint16_t(3001), "Data port of the NES Worker.", "uint16_t", false);

    ConfigOption<uint16_t> numberOfSlots = ConfigOption("numberOfSlots", uint16_t(std::thread::hardware_concurrency()),
                                                        "Number of computing slots for the NES Worker.", "uint16_t", false);

    ConfigOption<uint16_t> numWorkerThreads =
        ConfigOption("numWorkerThreads", uint16_t(1), "Number of worker threads.", "uint16_t", false);

    ConfigOption<std::string> parentId = ConfigOption("parentId", std::string("-1"), "Parent ID of this node.", "string", false);

    ConfigOption<std::string> logLevel =
        ConfigOption("logLevel", std::string("LOG_DEBUG"), "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ",
                     "string", false);
};

}// namespace NES

#endif//NES_WORKERCONFIG_HPP
