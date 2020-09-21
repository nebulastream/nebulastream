#ifndef INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_
#define INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_

#include <memory>
#include <string>

namespace NES {

class PhysicalStreamConfig;
typedef std::shared_ptr<PhysicalStreamConfig> PhysicalStreamConfigPtr;

/**
 * @brief this struct covers the information about the attached sensor
 * @param sourceType: string of data source, e.g., DefaultSource or CSVSource
 * @param sourceConf: parameter for the data source, e.g., numberOfProducedBuffer or file path
 * @param sourceFrequency: the sampling frequency in which the stream should sample a result
 * @param numberOfTuplesToProducePerBuffer: the number of tuples that are put into a buffer, e.g., for csv the number of lines read
 * @param numberOfBuffersToProduce: the number of buffers to produce
 * @param physicalStreamName: name of the stream created by this source
 * @param logicalStreamName: name of the logical steam where this physical stream contributes to
 */
struct PhysicalStreamConfig {

  public:
    static PhysicalStreamConfigPtr create(std::string sourceType  = "DefaultSource", std::string sourceConfig = "1", uint32_t sourceFrequency = 0,
                                          uint32_t numberOfTuplesToProducePerBuffer = 1, uint32_t numberOfBuffersToProduce = 0,
                                          std::string physicalStreamName = "default_physical", std::string logicalStreamName = "default_logical");

  private:
    explicit PhysicalStreamConfig(std::string sourceType, std::string sourceConfig,
                                  size_t sourceFrequency, size_t numberOfTuplesToProducePerBuffer, size_t numberOfBuffersToProduce,
                                  std::string physicalStreamName,
                                  std::string logicalStreamName);

    std::string toString();

    std::string sourceType;
    std::string sourceConfig;
    size_t sourceFrequency;
    size_t numberOfTuplesToProducePerBuffer;
    size_t numberOfBuffersToProduce;
    std::string physicalStreamName;
    std::string logicalStreamName;
};

}// namespace NES
#endif /* INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_ */
