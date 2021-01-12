

#include <Configs/ConfigOption.hpp>
#include <Configs/ConfigOptions/CoordinatorConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hh>
#include <Util/yaml/YamlDef.hh>
#include <string>
#include <sys/stat.h>

using namespace std;

namespace NES {

CoordinatorConfig::CoordinatorConfig(){}

void CoordinatorConfig::overwriteConfigWithYAMLFileInput(string filePath) {

    if (!filePath.empty()) {

        NES_INFO("NESCOORDINATORCONFIG: Using config file with path: " << filePath << " .");
        struct stat buffer {};
        if (stat(filePath.c_str(), &buffer) == -1) {
            NES_ERROR("NESCOORDINATORCONFIG: Configuration file not found at: " << filePath << '\n');
            NES_INFO("NESCOORDINATORCONFIG: Use default values for Coordinator Config instead.");
        }
        Yaml::Node config;
        Yaml::Parse(config, filePath.c_str());

        restPort.setUint16_tValue(config["restPort"].As<uint16_t>());
        rpcPort.setUint16_tValue(config["rpcPort"].As<uint16_t>());
        dataPort.setUint16_tValue(config["dataPort"].As<uint16_t>());
        restIp.setStringValue(config["restIp"].As<string>());
        coordinatorIp.setStringValue(config["coordinatorIp"].As<string>());
        numberOfSlots.setUint16_tValue(config["numberOfSlots"].As<uint16_t>());
        enableQueryMerging.setBoolValue(config["enableQueryMerging"].As<bool>());
        logLevel.setStringValue(config["logLevel"].As<string>());
    }

    NES_INFO("NESCOORDINATORCONFIG: No file path was provided, keeping default values for Coordinator Config.");
}

/*void CoordinatorConfig::overwriteConfigWithCommandLineInput() {}

const ConfigOption<std::string>& CoordinatorConfig::getRestIp() const { return restIp; }
void CoordinatorConfig::setRestIp(const ConfigOption<std::string>& restIp) { CoordinatorConfig::restIp = restIp; }
const ConfigOption<std::string>& CoordinatorConfig::getCoordinatorIp() const { return coordinatorIp; }
void CoordinatorConfig::setCoordinatorIp(const ConfigOption<std::string>& coordinatorIp) {
    CoordinatorConfig::coordinatorIp = coordinatorIp;
}
const ConfigOption<uint16_t>& CoordinatorConfig::getRpcPort() const { return rpcPort; }
void CoordinatorConfig::setRpcPort(const ConfigOption<uint16_t>& rpcPort) { CoordinatorConfig::rpcPort = rpcPort; }
const ConfigOption<uint16_t>& CoordinatorConfig::getRestPort() const { return restPort; }
void CoordinatorConfig::setRestPort(const ConfigOption<uint16_t>& restPort) { CoordinatorConfig::restPort = restPort; }
const ConfigOption<uint16_t>& CoordinatorConfig::getDataPort() const { return dataPort; }
void CoordinatorConfig::setDataPort(const ConfigOption<uint16_t>& dataPort) { CoordinatorConfig::dataPort = dataPort; }
const ConfigOption<uint16_t>& CoordinatorConfig::getNumberOfSlots() const { return numberOfSlots; }
void CoordinatorConfig::setNumberOfSlots(const ConfigOption<uint16_t>& numberOfSlots) {
    CoordinatorConfig::numberOfSlots = numberOfSlots;
}
const ConfigOption<bool>& CoordinatorConfig::getEnableQueryMerging() const { return enableQueryMerging; }
void CoordinatorConfig::setEnableQueryMerging(const ConfigOption<bool>& enableQueryMerging) {
    CoordinatorConfig::enableQueryMerging = enableQueryMerging;
}
const ConfigOption<std::string>& CoordinatorConfig::getLogLevel() const { return logLevel; }
void CoordinatorConfig::setLogLevel(const ConfigOption<std::string>& logLevel) { CoordinatorConfig::logLevel = logLevel; }
const string& CoordinatorConfig::getFilePath() const { return filePath; }
void CoordinatorConfig::setFilePath(const string& filePath) { CoordinatorConfig::filePath = filePath; }*/

}// namespace NES
