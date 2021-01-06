#ifndef NES_COORDINATORCONFIG_HPP
#define NES_COORDINATORCONFIG_HPP

#include <Configs/ConfigOption.hpp>

namespace NES{

class CoordinatorConfig{

  public:
    CoordinatorConfig(ConfigOption<std::string> restIp, ConfigOption<std::string> coordinatorIp, ConfigOption<uint16_t> restPort, ConfigOption<uint16_t> rpcPort,
                      ConfigOption<uint16_t> dataPort, ConfigOption<uint16_t> numberOfSlots, ConfigOption<bool> enableQueryMerging, ConfigOption<std::string> logLevel);

    ConfigOption<std::string> restIp;
    ConfigOption<std::string> coordinatorIp;
    ConfigOption<uint16_t> restPort;
    ConfigOption<uint16_t> rpcPort;
    ConfigOption<uint16_t> dataPort;
    ConfigOption<uint16_t> numberOfSlots;
    ConfigOption<bool> enableQueryMerging;
    ConfigOption<std::string> logLevel;

};

} // namespace NES

#endif//NES_COORDINATORCONFIG_HPP
