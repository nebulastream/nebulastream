#ifndef INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_
#define INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_

#include <string>

namespace NES {

/**
 * @brief this struct covers the information about the attached sensor
 * @param sourceType: string of data source, e.g., DefaultSource or CSVSource
 * @param sourceConf: parameter for the data source, e.g., numberOfProducedBuffer or file path
 * @param sourceFrequency: the sampling frequency in which the stream should sample a result
 * @param numberOfBuffersToProduce: the number of buffers to produce
 * @param physicalStreamName: name of the stream created by this source
 * @param logicalStreamName: name of the logical steam where this physical stream contributes to
 */
struct PhysicalStreamConfig {

    PhysicalStreamConfig();

    PhysicalStreamConfig(std::string sourceType, std::string sourceConfig,
                         size_t sourceFrequency, size_t numberOfBuffersToProduce,
                         std::string physicalStreamName,
                         std::string logicalStreamName);

    std::string toString();

    std::string sourceType;
    std::string sourceConfig;
    size_t sourceFrequency;
    size_t numberOfBuffersToProduce;
    std::string physicalStreamName;
    std::string logicalStreamName;
};

}// namespace NES
#endif /* INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_ */
