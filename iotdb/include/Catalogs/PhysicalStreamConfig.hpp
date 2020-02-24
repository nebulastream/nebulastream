#ifndef INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_
#define INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_

#include <string>

namespace NES {

/**
 * @brief this struct covers the information about the attached sensor
 * @param sourceType: string of data source, e.g., DefaultSource or CSVSource
 * @param sourceConf: parameter for the data source, e.g., numberOfProducedBuffer or file path
 * @param physicalStreamName: name of the stream created by this source
 * @param logicalStreamName: name of the logical steam where this physical stream contributes to
 */
struct PhysicalStreamConfig {

  PhysicalStreamConfig() {
    sourceType = "DefaultSource";
    sourceConfig = "1";
    sourceFrequency = 0.0;
    numberOfBuffersToProduce = 1;
    physicalStreamName = "default_physical";
    logicalStreamName = "default_logical";

  }
  ;

  PhysicalStreamConfig(std::string sourceType, std::string sourceConfig,
                       double sourceFrequency, size_t numberOfBuffersToProduce,
                       std::string physicalStreamName,
                       std::string logicalStreamName)
      :
      sourceType(sourceType),
      sourceConfig(sourceConfig),
      sourceFrequency(sourceFrequency),
      numberOfBuffersToProduce(numberOfBuffersToProduce),
      physicalStreamName(physicalStreamName),
      logicalStreamName(logicalStreamName) {
  }
  ;

  std::string sourceType;
  std::string sourceConfig;
  double sourceFrequency;
  size_t numberOfBuffersToProduce;
  std::string physicalStreamName;
  std::string logicalStreamName;
};

}
#endif /* INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_ */
