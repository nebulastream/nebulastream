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

#ifndef NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_
#define NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_

#include <Catalogs/AbstractPhysicalStreamConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <memory>
#include <string>

namespace NES {

class PhysicalStreamConfig;
using PhysicalStreamConfigPtr = std::shared_ptr<PhysicalStreamConfig>;

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
class PhysicalStreamConfig : public AbstractPhysicalStreamConfig {

  public:
    static PhysicalStreamConfigPtr createEmpty();
    static PhysicalStreamConfigPtr create(const SourceConfigPtr& sourceConfig);

    ~PhysicalStreamConfig() noexcept override = default;

    /**
     * @brief Get the source type
     * @return string representing source type
     */
    std::string getSourceType() override;

    /**
     * @brief get source config
     * @return string representing source config
     */
    [[nodiscard]] std::string getSourceConfig() const;

    /**
     * @brief get source frequency
     * @return returns the source frequency
     */
    [[nodiscard]] std::chrono::milliseconds getSourceFrequency() const;

    /**
     * @brief get the number of tuples to produce in a buffer
     * @return returns the number of tuples to produce in a buffer
     */
    [[nodiscard]] uint32_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief get the number of buffers to produce
     * @return returns the number of buffers to produce
     */
    [[nodiscard]] uint32_t getNumberOfBuffersToProduce() const;

    /**
     * @brief get physical stream name
     * @return physical stream name
     */
    std::string getPhysicalStreamName() override;

    /**
     * @brief get logical stream name
     * @return logical stream name
     */
    std::string getLogicalStreamName() override;

    std::string toString() override;

    [[nodiscard]] bool getSkipHeader() const;

    SourceDescriptorPtr build(SchemaPtr) override;

    void setSourceFrequency(uint32_t sourceFrequency);
    void setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer);
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

  protected:
    explicit PhysicalStreamConfig(const SourceConfigPtr& sourceConfig);

    std::string sourceType;
    std::string sourceConfig;
    std::chrono::milliseconds sourceFrequency;
    uint32_t numberOfTuplesToProducePerBuffer;
    uint32_t numberOfBuffersToProduce;

    std::string physicalStreamName;
    std::string logicalStreamName;
    bool skipHeader;
};

}// namespace NES
#endif  // NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_
