//
// Created by eleicha on 06.01.21.
//

#ifndef NES_SOURCECONFIG_HPP
#define NES_SOURCECONFIG_HPP

#include <Configs/ConfigOption.hpp>
#include <map>
#include <string>

namespace NES {

class SourceConfig {

  public:
    SourceConfig();

    void overwriteConfigWithYAMLFileInput(string filePath);
    void overwriteConfigWithCommandLineInput(map<string,string> inputParams);
    void resetSourceOptions();

    const ConfigOption<std::string>& getSourceType() const;
    void setSourceType(const string& sourceType);
    const ConfigOption<std::string>& getSourceConfig() const;
    void setSourceConfig(const std::string& sourceConfig);
    const ConfigOption<uint64_t>& getSourceFrequency() const;
    void setSourceFrequency(const uint64_t& sourceFrequency);
    const ConfigOption<uint64_t>& getNumberOfBuffersToProduce() const;
    void setNumberOfBuffersToProduce(const uint64_t& numberOfBuffersToProduce);
    const ConfigOption<uint16_t>& getNumberOfTuplesToProducePerBuffer() const;
    void setNumberOfTuplesToProducePerBuffer(const uint16_t& numberOfTuplesToProducePerBuffer);
    const ConfigOption<std::string>& getPhysicalStreamName() const;
    void setPhysicalStreamName(const string& physicalStreamName);
    const ConfigOption<std::string>& getLogicalStreamName() const;
    void setLogicalStreamName(const string& logicalStreamName);
    const ConfigOption<bool>& getSkipHeader() const;
    void setSkipHeader(const bool& skipHeader);
    const ConfigOption<std::string>& getLogLevel() const;
    void setLogLevel(const std::string& logLevel);

  private:
    ConfigOption<std::string> sourceType = ConfigOption(
        "sourceType", std::string("NoSource"),
        "Type of the Source (available options: DefaultSource, CSVSource, BinarySource, YSBSource).", "string", false);
    ConfigOption<std::string> sourceConfig = ConfigOption(
        "sourceConfig", std::string("NoConfig"),
        "Source configuration. Options depend on source type. See Source Configurations on our wiki page for further details.",
        "string", false);
    ConfigOption<uint64_t> sourceFrequency =
        ConfigOption("sourceFrequency", uint64_t(0), "Sampling frequency of the source.", "uint64_t", false);
    ConfigOption<uint64_t> numberOfBuffersToProduce =
        ConfigOption("numberOfBuffersToProduce", uint64_t(1), "Number of buffers to produce.", "uint64_t", false);
    ConfigOption<uint16_t> numberOfTuplesToProducePerBuffer = ConfigOption(
        "numberOfTuplesToProducePerBuffer", uint16_t(0), "Number of tuples to produce per buffer.", "uint16_t", false);
    ConfigOption<std::string> physicalStreamName =
        ConfigOption("physicalStreamName", std::string(""), "Physical name of the stream.", "string", false);
    ConfigOption<std::string> logicalStreamName =
        ConfigOption("logicalStreamName", std::string(""), "Logical name of the stream.", "string", false);
    ConfigOption<bool> skipHeader = ConfigOption("skipHeader", false, "Skip first line of the file.", "bool", false);
    ConfigOption<std::string> logLevel =
        ConfigOption("logLevel", std::string("LOG_DEBUG"), "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ",
                     "string", false);
};

}// namespace NES

#endif//NES_SOURCECONFIG_HPP
