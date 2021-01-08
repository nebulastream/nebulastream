//
// Created by eleicha on 08.01.21.
//

#include <Configs/ConfigOptions/CoordinatorConfig.hpp>

NES::CoordinatorConfig::CoordinatorConfig(NES::ConfigOption<std::string> restIp, NES::ConfigOption<std::string> coordinatorIp,
                                          NES::ConfigOption<uint16_t> restPort, NES::ConfigOption<uint16_t> rpcPort,
                                          NES::ConfigOption<uint16_t> dataPort, NES::ConfigOption<uint16_t> numberOfSlots,
                                          NES::ConfigOption<bool> enableQueryMerging, NES::ConfigOption<std::string> logLevel)
    : restIp(restIp), coordinatorIp(coordinatorIp), restPort(restPort), rpcPort(rpcPort), dataPort(dataPort),
      numberOfSlots(numberOfSlots), enableQueryMerging(enableQueryMerging), logLevel(logLevel) {}

//Todo: add sanity checks to setters

const NES::ConfigOption<std::string>& NES::CoordinatorConfig::getRestIp() const { return restIp; }
void NES::CoordinatorConfig::setRestIp(const NES::ConfigOption<std::string>& restIp) { CoordinatorConfig::restIp = restIp; }
const NES::ConfigOption<std::string>& NES::CoordinatorConfig::getCoordinatorIp() const { return coordinatorIp; }
void NES::CoordinatorConfig::setCoordinatorIp(const NES::ConfigOption<std::string>& coordinatorIp) {
    CoordinatorConfig::coordinatorIp = coordinatorIp;
}
const NES::ConfigOption<uint16_t>& NES::CoordinatorConfig::getRestPort() const { return restPort; }
void NES::CoordinatorConfig::setRestPort(const NES::ConfigOption<uint16_t>& restPort) { CoordinatorConfig::restPort = restPort; }
const NES::ConfigOption<uint16_t>& NES::CoordinatorConfig::getRpcPort() const { return rpcPort; }
void NES::CoordinatorConfig::setRpcPort(const NES::ConfigOption<uint16_t>& rpcPort) { CoordinatorConfig::rpcPort = rpcPort; }
const NES::ConfigOption<uint16_t>& NES::CoordinatorConfig::getDataPort() const { return dataPort; }
void NES::CoordinatorConfig::setDataPort(const NES::ConfigOption<uint16_t>& dataPort) { CoordinatorConfig::dataPort = dataPort; }
const NES::ConfigOption<uint16_t>& NES::CoordinatorConfig::getNumberOfSlots() const { return numberOfSlots; }
void NES::CoordinatorConfig::setNumberOfSlots(const NES::ConfigOption<uint16_t>& numberOfSlots) {
    CoordinatorConfig::numberOfSlots = numberOfSlots;
}
const NES::ConfigOption<bool>& NES::CoordinatorConfig::getEnableQueryMerging() const { return enableQueryMerging; }
void NES::CoordinatorConfig::setEnableQueryMerging(const NES::ConfigOption<bool>& enableQueryMerging) {
    CoordinatorConfig::enableQueryMerging = enableQueryMerging;
}
const NES::ConfigOption<std::string>& NES::CoordinatorConfig::getLogLevel() const { return logLevel; }
void NES::CoordinatorConfig::setLogLevel(const NES::ConfigOption<std::string>& logLevel) {
    CoordinatorConfig::logLevel = logLevel;
}
