//
// Created by eleicha on 06.01.21.
//

#ifndef NES_SOURCECONFIG_HPP
#define NES_SOURCECONFIG_HPP

#include <Configs/ConfigOption.hpp>
#include <string>

namespace NES {

class SourceConfig {

  public:
    SourceConfig(ConfigOption<std::string> sourceType, ConfigOption<std::string> sourceConfig,
                      ConfigOption<uint64_t> sourceFrequency, ConfigOption<uint64_t> numberOfBuffersToProduce,
                      ConfigOption<uint16_t> numberOfTuplesToProducePerBuffer, ConfigOption<std::string> physicalStreamName,
                      ConfigOption<std::string> logicalStreamName, ConfigOption<bool> skipHeader,
                      ConfigOption<std::string> logLevel);

    const ConfigOption<std::string>& getSourceType() const;
    void setSourceType(const ConfigOption<std::string>& sourceType);
    const ConfigOption<std::string>& getSourceConfig() const;
    void setSourceConfig(const ConfigOption<std::string>& sourceConfig);
    const ConfigOption<uint64_t>& getSourceFrequency() const;
    void setSourceFrequency(const ConfigOption<uint64_t>& sourceFrequency);
    const ConfigOption<uint64_t>& getNumberOfBuffersToProduce() const;
    void setNumberOfBuffersToProduce(const ConfigOption<uint64_t>& numberOfBuffersToProduce);
    const ConfigOption<uint16_t>& getNumberOfTuplesToProducePerBuffer() const;
    void setNumberOfTuplesToProducePerBuffer(const ConfigOption<uint16_t>& numberOfTuplesToProducePerBuffer);
    const ConfigOption<std::string>& getPhysicalStreamName() const;
    void setPhysicalStreamName(const ConfigOption<std::string>& physicalStreamName);
    const ConfigOption<std::string>& getLogicalStreamName() const;
    void setLogicalStreamName(const ConfigOption<std::string>& logicalStreamName);
    const ConfigOption<bool>& getSkipHeader() const;
    void setSkipHeader(const ConfigOption<bool>& skipHeader);
    const ConfigOption<std::string>& getLogLevel() const;
    void setLogLevel(const ConfigOption<std::string>& logLevel);

  private:
    ConfigOption<std::string> sourceType =
        ConfigOption("sourceType", std::string("NoSource"),
                     "Type of the Source (available options: DefaultSource, CSVSource, BinarySource, YSBSource).",
                     "string", ConfigType::DEFAULT, false);
    ConfigOption<std::string> sourceConfig =
        ConfigOption("sourceConfig", std::string("NoConfig"),
                     "Source configuration. Options depend on source type. See Source Configurations on our wiki page for further details.",
                     "string", ConfigType::DEFAULT, false);
    ConfigOption<uint64_t> sourceFrequency =
        ConfigOption("sourceFrequency", uint64_t(0),
                     "Sampling frequency of the source.",
                     "uint64_t", ConfigType::DEFAULT, false);
    ConfigOption<uint64_t> numberOfBuffersToProduce =
        ConfigOption("numberOfBuffersToProduce", uint64_t(1),
                     "Number of buffers to produce.",
                     "uint64_t", ConfigType::DEFAULT, false);
    ConfigOption<uint16_t> numberOfTuplesToProducePerBuffer =
        ConfigOption("numberOfTuplesToProducePerBuffer", uint16_t(0),
                     "Number of tuples to produce per buffer.",
                     "uint16_t", ConfigType::DEFAULT, false);
    ConfigOption<std::string> physicalStreamName =
        ConfigOption("physicalStreamName", std::string(""),
                     "Physical name of the stream.",
                     "string", ConfigType::DEFAULT, false);
    ConfigOption<std::string> logicalStreamName =
        ConfigOption("logicalStreamName", std::string(""),
                     "Logical name of the stream.",
                     "string", ConfigType::DEFAULT, false);
    ConfigOption<bool> skipHeader =
        ConfigOption("skipHeader", false,
                     "Skip first line of the file.",
                     "bool", ConfigType::DEFAULT, false);
    ConfigOption<std::string> logLevel =
        ConfigOption("logLevel", std::string("LOG_DEBUG"),
                     "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ",
                     "string", ConfigType::DEFAULT, false);
};

}// namespace NES

#endif//NES_SOURCECONFIG_HPP
