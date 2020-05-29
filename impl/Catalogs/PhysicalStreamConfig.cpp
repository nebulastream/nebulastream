#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Util/Logger.hpp>
#include <sstream>
namespace NES {

PhysicalStreamConfig::PhysicalStreamConfig() {
    sourceType = "DefaultSource";
    sourceConfig = "1";
    sourceFrequency = 0;
    numberOfBuffersToProduce = 1;
    physicalStreamName = "default_physical";
    logicalStreamName = "default_logical";
};

PhysicalStreamConfig::PhysicalStreamConfig(std::string sourceType,
                                           std::string sourceConfig,
                                           size_t sourceFrequency,
                                           size_t numberOfBuffersToProduce,
                                           std::string physicalStreamName,
                                           std::string logicalStreamName)
    : sourceType(sourceType),
      sourceConfig(sourceConfig),
      sourceFrequency(sourceFrequency),
      numberOfBuffersToProduce(numberOfBuffersToProduce),
      physicalStreamName(physicalStreamName),
      logicalStreamName(logicalStreamName){};

std::string PhysicalStreamConfig::toString() {
    std::stringstream ss;
    ss << "sourceType=" << sourceType << " sourceConfig=" << sourceConfig
       << " sourceFrequency=" << sourceFrequency << " numberOfBuffersToProduce="
       << numberOfBuffersToProduce << " physicalStreamName=" << physicalStreamName
       << " logicalStreamName=" << logicalStreamName;
    return ss.str();
}
}// namespace NES
