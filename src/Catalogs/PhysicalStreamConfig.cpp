#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Util/Logger.hpp>
#include <sstream>
namespace NES {

PhysicalStreamConfigPtr PhysicalStreamConfig::create(std::string sourceType, std::string sourceConfig, uint32_t sourceFrequency, uint32_t numberOfTuplesToProducePerBuffer,
                                                     uint32_t numberOfBuffersToProduce, std::string physicalStreamName, std::string logicalStreamName) {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(sourceType, sourceConfig, sourceFrequency, numberOfTuplesToProducePerBuffer,
                                                                       numberOfBuffersToProduce, physicalStreamName, logicalStreamName));
}

PhysicalStreamConfig::PhysicalStreamConfig(std::string sourceType,
                                           std::string sourceConfig,
                                           size_t sourceFrequency,
                                           size_t numberOfTuplesToProducePerBuffer,
                                           size_t numberOfBuffersToProduce,
                                           std::string physicalStreamName,
                                           std::string logicalStreamName)
    : sourceType(sourceType),
      sourceConfig(sourceConfig),
      sourceFrequency(sourceFrequency),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer),
      numberOfBuffersToProduce(numberOfBuffersToProduce),
      physicalStreamName(physicalStreamName),
      logicalStreamName(logicalStreamName){};

std::string PhysicalStreamConfig::toString() {
    std::stringstream ss;
    ss << "sourceType=" << sourceType << " sourceConfig=" << sourceConfig
       << " sourceFrequency=" << sourceFrequency << " numberOfTuplesToProducePerBuffer="
       << numberOfTuplesToProducePerBuffer << " numberOfBuffersToProduce="
       << numberOfBuffersToProduce << " physicalStreamName=" << physicalStreamName
       << " logicalStreamName=" << logicalStreamName;
    return ss.str();
}
}// namespace NES
