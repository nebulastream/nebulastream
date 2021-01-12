
#include <Configs/ConfigOption.hpp>
#include <Configs/ConfigOptions/SourceConfig.hpp>

namespace NES {
SourceConfig::SourceConfig(NES::ConfigOption<std::string> sourceType, NES::ConfigOption<std::string> sourceConfig,
                           NES::ConfigOption<uint64_t> sourceFrequency, NES::ConfigOption<uint64_t> numberOfBuffersToProduce,
                           NES::ConfigOption<uint16_t> numberOfTuplesToProducePerBuffer,
                           NES::ConfigOption<std::string> physicalStreamName, NES::ConfigOption<std::string> logicalStreamName,
                           NES::ConfigOption<bool> skipHeader, NES::ConfigOption<std::string> logLevel)
    : sourceType(sourceType), sourceConfig(sourceConfig), sourceFrequency(sourceFrequency),
      numberOfBuffersToProduce(numberOfBuffersToProduce), numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer),
      physicalStreamName(physicalStreamName), logicalStreamName(logicalStreamName), skipHeader(skipHeader), logLevel(logLevel) {}

//todo: add sanity checks to setters
const ConfigOption<std::string>& NES::SourceConfig::getSourceType() const { return sourceType; }
void SourceConfig::setSourceType(const NES::ConfigOption<std::string>& sourceType) { SourceConfig::sourceType = sourceType; }
const ConfigOption<std::string>& NES::SourceConfig::getSourceConfig() const { return sourceConfig; }
void SourceConfig::setSourceConfig(const NES::ConfigOption<std::string>& sourceConfig) {
    SourceConfig::sourceConfig = sourceConfig;
}
const ConfigOption<uint64_t>& NES::SourceConfig::getSourceFrequency() const { return sourceFrequency; }
void SourceConfig::setSourceFrequency(const NES::ConfigOption<uint64_t>& sourceFrequency) {
    SourceConfig::sourceFrequency = sourceFrequency;
}
const ConfigOption<uint64_t>& NES::SourceConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }
void SourceConfig::setNumberOfBuffersToProduce(const NES::ConfigOption<uint64_t>& numberOfBuffersToProduce) {
    SourceConfig::numberOfBuffersToProduce = numberOfBuffersToProduce;
}
const ConfigOption<uint16_t>& NES::SourceConfig::getNumberOfTuplesToProducePerBuffer() const {
    return numberOfTuplesToProducePerBuffer;
}
void SourceConfig::setNumberOfTuplesToProducePerBuffer(const NES::ConfigOption<uint16_t>& numberOfTuplesToProducePerBuffer) {
    SourceConfig::numberOfTuplesToProducePerBuffer = numberOfTuplesToProducePerBuffer;
}
const ConfigOption<std::string>& NES::SourceConfig::getPhysicalStreamName() const { return physicalStreamName; }
void SourceConfig::setPhysicalStreamName(const NES::ConfigOption<std::string>& physicalStreamName) {
    SourceConfig::physicalStreamName = physicalStreamName;
}
const ConfigOption<std::string>& NES::SourceConfig::getLogicalStreamName() const { return logicalStreamName; }
void SourceConfig::setLogicalStreamName(const NES::ConfigOption<std::string>& logicalStreamName) {
    SourceConfig::logicalStreamName = logicalStreamName;
}
const ConfigOption<bool>& NES::SourceConfig::getSkipHeader() const { return skipHeader; }
void SourceConfig::setSkipHeader(const NES::ConfigOption<bool>& skipHeader) { SourceConfig::skipHeader = skipHeader; }
const ConfigOption<std::string>& NES::SourceConfig::getLogLevel() const { return logLevel; }
void SourceConfig::setLogLevel(const NES::ConfigOption<std::string>& logLevel) { SourceConfig::logLevel = logLevel; }
SourceConfig::SourceConfig() {}

}// namespace NES