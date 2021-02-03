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
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>

using namespace std;

namespace NES {

CoordinatorConfig::CoordinatorConfig() {
    NES_INFO("Generated new Coordinator Config object. Configurations initialized with default values.");
}

void CoordinatorConfig::overwriteConfigWithYAMLFileInput(string filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("CoordinatorConfig: Using config file with path: " << filePath << " .");
        Yaml::Node config = *(new Yaml::Node());
        Yaml::Parse(config, filePath.c_str());
        try {
            setRestPort(config["restPort"].As<uint16_t>());
            setRpcPort(config["rpcPort"].As<uint16_t>());
            setDataPort(config["dataPort"].As<uint16_t>());
            setRestIp(config["restIp"].As<string>());
            setCoordinatorIp(config["coordinatorIp"].As<string>());
            setNumberOfSlots(config["numberOfSlots"].As<uint16_t>());
            setEnableQueryMerging(config["enableQueryMerging"].As<bool>());
            setLogLevel(config["logLevel"].As<string>());
        } catch (exception& e) {
            NES_ERROR("CoordinatorConfig: Error while initializing configuration parameters from YAML file.");
            NES_WARNING("CoordinatorConfig: Keeping default values.");
            resetCoordinatorOptions();
        }
        return;
    }
    NES_ERROR("CoordinatorConfig: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("CoordinatorConfig: Keeping default values for Coordinator Config.");
}

void CoordinatorConfig::overwriteConfigWithCommandLineInput(map<string, string> inputParams) {
    try {
        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            if (it->first == "--restIp") {
                setRestIp(it->second);
            } else if (it->first == "--coordinatorIp") {
                setCoordinatorIp(it->second);
            } else if (it->first == "--coordinatorPort") {
                setRpcPort(stoi(it->second));
            } else if (it->first == "--restPort") {
                setRestPort(stoi(it->second));
            } else if (it->first == "--dataPort") {
                setDataPort(stoi(it->second));
            } else if (it->first == "--numberOfSlots") {
                setNumberOfSlots(stoi(it->second));
            } else if (it->first == "--enableQueryMerging") {
                setEnableQueryMerging((it->second == "true"));
            } else if (it->first == "--logLevel") {
                setLogLevel(it->second);
            }
        }
    } catch (exception e) {
        NES_ERROR("CoordinatorConfig: Error while initializing configuration parameters from command line.");
        NES_WARNING("CoordinatorConfig: Keeping default values.");
        resetCoordinatorOptions();
    }
}

void CoordinatorConfig::resetCoordinatorOptions() {
    setRestPort(restPort.getDefaultValue());
    setRpcPort(rpcPort.getDefaultValue());
    setDataPort(dataPort.getDefaultValue());
    setRestIp(restIp.getDefaultValue());
    setCoordinatorIp(coordinatorIp.getDefaultValue());
    setNumberOfSlots(numberOfSlots.getDefaultValue());
    setEnableQueryMerging(enableQueryMerging.getDefaultValue());
    setLogLevel(logLevel.getDefaultValue());
}

const ConfigOption<std::string>& CoordinatorConfig::getRestIp() const { return restIp; }
void CoordinatorConfig::setRestIp(const string& restIp) { CoordinatorConfig::restIp.setValue(restIp); }
const ConfigOption<std::string>& CoordinatorConfig::getCoordinatorIp() const { return coordinatorIp; }
void CoordinatorConfig::setCoordinatorIp(const string& coordinatorIp) {
    CoordinatorConfig::coordinatorIp.setValue(coordinatorIp);
}
const ConfigOption<uint16_t>& CoordinatorConfig::getRpcPort() const { return rpcPort; }
void CoordinatorConfig::setRpcPort(const uint16_t& rpcPort) { CoordinatorConfig::rpcPort.setValue(rpcPort); }
const ConfigOption<uint16_t>& CoordinatorConfig::getRestPort() const { return restPort; }
void CoordinatorConfig::setRestPort(const uint16_t& restPort) { CoordinatorConfig::restPort.setValue(restPort); }
const ConfigOption<uint16_t>& CoordinatorConfig::getDataPort() const { return dataPort; }
void CoordinatorConfig::setDataPort(const uint16_t& dataPort) { CoordinatorConfig::dataPort.setValue(dataPort); }
const ConfigOption<uint16_t>& CoordinatorConfig::getNumberOfSlots() const { return numberOfSlots; }
void CoordinatorConfig::setNumberOfSlots(const uint16_t& numberOfSlots) {
    CoordinatorConfig::numberOfSlots.setValue(numberOfSlots);
}
const ConfigOption<bool>& CoordinatorConfig::getEnableQueryMerging() const { return enableQueryMerging; }
void CoordinatorConfig::setEnableQueryMerging(const bool& enableQueryMerging) {
    CoordinatorConfig::enableQueryMerging.setValue(enableQueryMerging);
}
const ConfigOption<std::string>& CoordinatorConfig::getLogLevel() const { return logLevel; }
void CoordinatorConfig::setLogLevel(const string& logLevel) { CoordinatorConfig::logLevel.setValue(logLevel); }

}// namespace NES
