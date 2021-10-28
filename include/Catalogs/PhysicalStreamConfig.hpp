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

#include <Catalogs/AbstractPhysicalStreamConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
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
 * @param logicalStreamName: vector of logical steam names where this physical stream contributes to
 */
class PhysicalStreamConfig : public AbstractPhysicalStreamConfig {

  public:
    static PhysicalStreamConfigPtr createEmpty();
    static PhysicalStreamConfigPtr create(SourceConfigPtr sourceConfig);

    /**
     * @brief Get the source type
     * @return string representing source type
     */
    const std::string getSourceType() override;

    /**
     * @brief get source config
     * @return string representing source config
     */
    const std::string getSourceConfig() const;

    /**
     * @brief get source frequency
     * @return returns the source frequency
     */
    std::chrono::milliseconds getSourceFrequency() const;

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
    const std::string getPhysicalStreamName() override;

    /**
     * @brief sets the physical stream name
     * @param newName of the stream
     */
    void setPhysicalStreamName(const std::string& newName) override;

    /**
     * @brief get a vector of logical stream names
     * @return logical stream name
     */
    const std::vector<std::string> getLogicalStreamNames() override;

    /**
     * Utility method to add one new logicalStreamName to an existing PhysicalStreamConfig
     * (only used in the test cases)
     * @param logicalStreamName
     */
    void addLogicalStreamName(std::string logicalStreamName);

    const std::string toString() override;

    bool getSkipHeader() const;

    SourceConfigPtr toSourceConfig() override;

    SourceDescriptorPtr build(SchemaPtr, std::string) override;

    void setSourceFrequency(uint32_t sourceFrequency);
    void setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer);
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

  protected:
    explicit PhysicalStreamConfig(SourceConfigPtr sourceConfig);

    std::string sourceType;
    std::string sourceConfig;
    std::chrono::milliseconds sourceFrequency;
    uint32_t numberOfTuplesToProducePerBuffer;
    uint32_t numberOfBuffersToProduce;

    std::string physicalStreamName;
    std::vector<std::string> logicalStreamName;
    bool skipHeader;
};

}// namespace NES
#endif /* INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_ */
