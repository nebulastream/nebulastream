//
// Created by eleicha on 06.01.21.
//

#ifndef NES_SOURCECONFIG_HPP
#define NES_SOURCECONFIG_HPP


#include <Configs/ConfigOption.hpp>

namespace NES{

class SourceConfig{

  public:
    CoordinatorConfig(ConfigOption<std::string> sourceType, ConfigOption<std::string> sourceConfig, ConfigOption<uint16_t> sourceFrequency,
                      ConfigOption<uint16_t> numberOfBuffersToProduce, ConfigOption<uint16_t> numberOfTuplesToProducePerBuffer,
                      ConfigOption<std::string> physicalStreamName, ConfigOption<std::string> logicalStreamName,
                      ConfigOption<bool> skipHeader, ConfigOption<std::string> logLevel);

    ConfigOption<std::string> sourceType;
    ConfigOption<std::string> sourceConfig;
    ConfigOption<uint16_t> sourceFrequency;
    ConfigOption<uint16_t> numberOfBuffersToProduce;
    ConfigOption<uint16_t> numberOfTuplesToProducePerBuffer;
    ConfigOption<std::string> physicalStreamName;
    ConfigOption<std::string> logicalStreamName;
    ConfigOption<bool> skipHeader;
    ConfigOption<std::string> logLevel;

};

} // namespace NES

#endif//NES_SOURCECONFIG_HPP
