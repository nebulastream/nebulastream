

#include <Configs/ConfigOptions/CoordinatorConfig.hpp>

namespace NES {
CoordinatorConfig::CoordinatorConfig(NES::ConfigOption<std::string> restIp, NES::ConfigOption<std::string> coordinatorIp,
                                     NES::ConfigOption<uint16_t> restPort, NES::ConfigOption<uint16_t> rpcPort,
                                     NES::ConfigOption<uint16_t> dataPort, NES::ConfigOption<uint16_t> numberOfSlots,
                                     NES::ConfigOption<bool> enableQueryMerging, NES::ConfigOption<std::string> logLevel)
    : restIp(restIp), coordinatorIp(coordinatorIp), restPort(restPort), rpcPort(rpcPort), dataPort(dataPort),
      numberOfSlots(numberOfSlots), enableQueryMerging(enableQueryMerging), logLevel(logLevel) {}

//Todo: add sanity checks to setters

const ConfigOption<std::string>& NES::CoordinatorConfig::getRestIp() const { return restIp; }
void CoordinatorConfig::setRestIp(const NES::ConfigOption<std::string>& restIp) { CoordinatorConfig::restIp = restIp; }
const ConfigOption<std::string>& NES::CoordinatorConfig::getCoordinatorIp() const { return coordinatorIp; }
void CoordinatorConfig::setCoordinatorIp(const NES::ConfigOption<std::string>& coordinatorIp) {
    CoordinatorConfig::coordinatorIp = coordinatorIp;
}
const ConfigOption<uint16_t>& NES::CoordinatorConfig::getRestPort() const { return restPort; }
void CoordinatorConfig::setRestPort(const NES::ConfigOption<uint16_t>& restPort) { CoordinatorConfig::restPort = restPort; }
const ConfigOption<uint16_t>& NES::CoordinatorConfig::getRpcPort() const { return rpcPort; }
void CoordinatorConfig::setRpcPort(const NES::ConfigOption<uint16_t>& rpcPort) { CoordinatorConfig::rpcPort = rpcPort; }
const ConfigOption<uint16_t>& NES::CoordinatorConfig::getDataPort() const { return dataPort; }
void CoordinatorConfig::setDataPort(const NES::ConfigOption<uint16_t>& dataPort) { CoordinatorConfig::dataPort = dataPort; }
const ConfigOption<uint16_t>& NES::CoordinatorConfig::getNumberOfSlots() const { return numberOfSlots; }
void CoordinatorConfig::setNumberOfSlots(const NES::ConfigOption<uint16_t>& numberOfSlots) {
    CoordinatorConfig::numberOfSlots = numberOfSlots;
}
const ConfigOption<bool>& NES::CoordinatorConfig::getEnableQueryMerging() const { return enableQueryMerging; }
void CoordinatorConfig::setEnableQueryMerging(const NES::ConfigOption<bool>& enableQueryMerging) {
    CoordinatorConfig::enableQueryMerging = enableQueryMerging;
}
const ConfigOption<std::string>& NES::CoordinatorConfig::getLogLevel() const { return logLevel; }
void CoordinatorConfig::setLogLevel(const NES::ConfigOption<std::string>& logLevel) { CoordinatorConfig::logLevel = logLevel; }
CoordinatorConfig::CoordinatorConfig() {}
}// namespace NES
