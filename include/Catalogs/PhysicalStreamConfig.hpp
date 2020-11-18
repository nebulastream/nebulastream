/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
    static PhysicalStreamConfigPtr create(std::string sourceType = "DefaultSource", std::string sourceConfig = "1",
                                          uint32_t sourceFrequency = 1, uint32_t numberOfTuplesToProducePerBuffer = 1,
                                          uint32_t numberOfBuffersToProduce = 1,
                                          std::string physicalStreamName = "default_physical",
                                          std::string logicalStreamName = "default_logical", bool endlessRepeat = false,
                                          bool skipHeader = false);

    /**
     * @brief Get the source type
     * @return string representing source type
     */
    const std::string& getSourceType() const;

    /**
     * @brief get source config
     * @return string representing source config
     */
    const std::string& getSourceConfig() const;

    /**
     * @brief get source frequency
     * @return returns the source frequency
     */
    uint32_t getSourceFrequency() const;

    /**
     * @brief get the number of tuples to produce in a buffer
     * @return returns the number of tuples to produce in a buffer
     */
    uint32_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief get the number of buffers to produce
     * @return returns the number of buffers to produce
     */
    uint32_t getNumberOfBuffersToProduce() const;

    /**
     * @brief get physical stream name
     * @return physical stream name
     */
    const std::string getPhysicalStreamName() const;

    /**
     * @brief get logical stream name
     * @return logical stream name
     */
    const std::string getLogicalStreamName() const;

    std::string toString();

    /**
     * @brief getter/setter endlessRepeat
     */
    bool isEndlessRepeat() const;
    void setEndlessRepeat(bool endlessRepeat);

    bool getSkipHeader() const;

  private:
    explicit PhysicalStreamConfig(std::string sourceType, std::string sourceConfig, size_t sourceFrequency,
                                  size_t numberOfTuplesToProducePerBuffer, size_t numberOfBuffersToProduce,
                                  std::string physicalStreamName, std::string logicalStreamName, bool endlessRepeat,
                                  bool skipHeader);

    std::string sourceType;
    std::string sourceConfig;
    uint32_t sourceFrequency;
    uint32_t numberOfTuplesToProducePerBuffer;
    uint32_t numberOfBuffersToProduce;
    std::string physicalStreamName;
    std::string logicalStreamName;
    bool endlessRepeat;
    bool skipHeader;
};

}// namespace NES
#endif /* INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_ */
