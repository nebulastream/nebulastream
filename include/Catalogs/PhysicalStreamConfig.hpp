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
#include <Configurations/Sources/SourceConfig.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <memory>
#include <string>

namespace NES {

class PhysicalStreamConfig;
using PhysicalStreamConfigPtr = std::shared_ptr<PhysicalStreamConfig>;

/**
 * @brief this struct covers the information about the attached sensor
 * @param sourceConf: parameter for the data source, e.g., numberOfProducedBuffer or file path
 */
class PhysicalStreamConfig : public AbstractPhysicalStreamConfig {

  public:
    static PhysicalStreamConfigPtr createEmpty();
    static PhysicalStreamConfigPtr create(const Configurations::SourceConfigPtr& sourceConfig);

    ~PhysicalStreamConfig() noexcept override = default;

    /**
     * @brief get sourceConfig
     * @return returns the source configuration
     */
    [[nodiscard]] Configurations::SourceConfigPtr getSourceConfig() const;

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

    /**
     * @brief get source type
     * @return source type
     */
    std::string getSourceType() override;

    std::string toString() override;

    SourceDescriptorPtr build(SchemaPtr) override;

  protected:
    explicit PhysicalStreamConfig(const Configurations::SourceConfigPtr& sourceConfig);

    Configurations::SourceConfigPtr sourceConfig;
    uint32_t numberOfTuplesToProducePerBuffer;
    uint32_t numberOfBuffersToProduce;
    std::string physicalStreamName;
    std::string logicalStreamName;
    std::string sourceType;
};

}// namespace NES
#endif// NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_
