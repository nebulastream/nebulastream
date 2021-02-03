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

#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>
#include <thread>

namespace NES {

WorkerConfigPtr WorkerConfig::create() { return std::make_shared<WorkerConfig>(WorkerConfig()); }

WorkerConfig::WorkerConfig() {
    NES_INFO("Generated new Worker Config object. Configurations initialized with default values.");
    localWorkerIp = ConfigOption<std::string>::create("localWorkerIp", "127.0.0.1", "Worker IP.");
    coordinatorIp = ConfigOption<std::string>::create("coordinatorIp", "127.0.0.1",
                                                      "Server IP of the NES Coordinator to which the NES Worker should connect.");
    coordinatorPort = ConfigOption<uint32_t>::create(
        "coordinatorPort", 4000,
        "RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs "
        "to be the same as rpcPort in Coordinator.");
    rpcPort = ConfigOption<uint32_t>::create("rpcPort", 3000, "RPC server port of the NES Worker.");
    dataPort = ConfigOption<uint32_t>::create("dataPort", 3001, "Data port of the NES Worker.");
    numberOfSlots = ConfigOption<uint32_t>::create("numberOfSlots", std::thread::hardware_concurrency(),
                                                   "Number of computing slots for the NES Worker.");
    numWorkerThreads = ConfigOption<uint32_t>::create("numWorkerThreads", 1, "Number of worker threads.");
    parentId = ConfigOption<std::string>::create("parentId", "-1", "Parent ID of this node.");
    logLevel = ConfigOption<std::string>::create("logLevel", "LOG_DEBUG",
                                                 "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ");
}

void WorkerConfig::overwriteConfigWithYAMLFileInput(std::string filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {

        NES_INFO("NesWorkerConfig: Using config file with path: " << filePath << " .");

        Yaml::Node config = *(new Yaml::Node());
        Yaml::Parse(config, filePath.c_str());
        try {
            setCoordinatorPort(config["coordinatorPort"].As<uint16_t>());
            setRpcPort(config["rpcPort"].As<uint16_t>());
            setDataPort(config["dataPort"].As<uint16_t>());
            setLocalWorkerIp(config["localWorkerIp"].As<std::string>());
            setCoordinatorIp(config["coordinatorIp"].As<std::string>());
            setNumberOfSlots(config["numberOfSlots"].As<uint16_t>());
            setNumWorkerThreads(config["numWorkerThreads"].As<bool>());
            setParentId(config["parentId"].As<std::string>());
            setLogLevel(config["logLevel"].As<std::string>());
        } catch (std::exception& e) {
            NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from YAML file. Keeping default "
                      "values. "
                      << e.what());
            resetWorkerOptions();
        }
        return;
    }
    NES_ERROR("NesWorkerConfig: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("Keeping default values for Worker Config.");
}

void WorkerConfig::overwriteConfigWithCommandLineInput(std::map<std::string, std::string> inputParams) {
    try {

        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            if (it->first == "--localWorkerIp") {
                setLocalWorkerIp(it->second);
            } else if (it->first == "--coordinatorIp") {
                setCoordinatorIp(it->second);
            } else if (it->first == "--rpcPort") {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--coordinatorPort") {
                setCoordinatorPort(stoi(it->second));
            } else if (it->first == "--dataPort") {
                setDataPort(stoi(it->second));
            } else if (it->first == "--numberOfSlots") {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--numWorkerThreads") {
                setNumWorkerThreads(stoi(it->second));
            } else if (it->first == "--parentId") {
                setParentId(it->second);
            } else if (it->first == "--logLevel") {
                setLogLevel(it->second);
            } else {
                NES_WARNING("Unknow configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from command line. Keeping default "
                  "values. "
                  << e.what());
        resetWorkerOptions();
    }
}

void WorkerConfig::resetWorkerOptions() {
    setLocalWorkerIp(localWorkerIp->getDefaultValue());
    setCoordinatorIp(coordinatorIp->getDefaultValue());
    setCoordinatorPort(coordinatorPort->getDefaultValue());
    setRpcPort(rpcPort->getDefaultValue());
    setDataPort(dataPort->getDefaultValue());
    setNumberOfSlots(numberOfSlots->getDefaultValue());
    setNumWorkerThreads(numWorkerThreads->getDefaultValue());
    setParentId(parentId->getDefaultValue());
    setLogLevel(logLevel->getDefaultValue());
}

StringConfigOption WorkerConfig::getLocalWorkerIp() { return localWorkerIp; }

void WorkerConfig::setLocalWorkerIp(std::string localWorkerIpValue) { localWorkerIp->setValue(localWorkerIpValue); }

StringConfigOption WorkerConfig::getCoordinatorIp() { return coordinatorIp; }

void WorkerConfig::setCoordinatorIp(std::string coordinatorIpValue) { coordinatorIp->setValue(coordinatorIpValue); }

IntConfigOption WorkerConfig::getCoordinatorPort() { return coordinatorPort; }

void WorkerConfig::setCoordinatorPort(uint16_t coordinatorPortValue) { coordinatorPort->setValue(coordinatorPortValue); }

IntConfigOption WorkerConfig::getRpcPort() { return rpcPort; }

void WorkerConfig::setRpcPort(uint16_t rpcPortValue) { rpcPort->setValue(rpcPortValue); }

IntConfigOption WorkerConfig::getDataPort() { return dataPort; }

void WorkerConfig::setDataPort(uint16_t dataPortValue) { dataPort->setValue(dataPortValue); }

IntConfigOption WorkerConfig::getNumberOfSlots() { return numberOfSlots; }

void WorkerConfig::setNumberOfSlots(uint16_t numberOfSlotsValue) { numberOfSlots->setValue(numberOfSlotsValue); }

IntConfigOption WorkerConfig::getNumWorkerThreads() { return numWorkerThreads; }

void WorkerConfig::setNumWorkerThreads(uint16_t numWorkerThreadsValue) { numWorkerThreads->setValue(numWorkerThreadsValue); }

StringConfigOption WorkerConfig::getParentId() { return parentId; }

void WorkerConfig::setParentId(std::string parentIdValue) { parentId->setValue(parentIdValue); }

StringConfigOption WorkerConfig::getLogLevel() { return logLevel; }

void WorkerConfig::setLogLevel(std::string logLevelValue) { logLevel->setValue(logLevelValue); }
}// namespace NES