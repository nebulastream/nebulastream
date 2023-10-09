/*
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

#ifndef NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_ARROWSOURCETYPE_HPP_
#define NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_ARROWSOURCETYPE_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <string>

namespace NES {

class ArrowSourceType;
using ArrowSourceTypePtr = std::shared_ptr<ArrowSourceType>;

/**
 * @brief Configuration object for Arrow source config
 * define configurations for a Arrow IPC file source, i.e. this source reads from data from an Arrow IPC file
 */
class ArrowSourceType : public PhysicalSourceType {
  public:
    ~ArrowSourceType() noexcept override = default;

    /**
     * @brief create a ArrowSourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return ArrowSourceTypePtr
     */
    static ArrowSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a ArrowSourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return ArrowSourceTypePtr
     */
    static ArrowSourceTypePtr create(Yaml::Node yamlConfig);

    /**
     * @brief create a default ArrowSourceTypePtr object
     * @return ArrowSourceTypePtr
     */
    static ArrowSourceTypePtr create();

    /**
     * @brief creates a string representation of the source
     * @return
     */
    std::string toString() override;

    /**
     * Checks equality
     * @param other arrowSourceType to check for equality
     * @return true if equal, false otherwise
     */
    bool equal(PhysicalSourceTypePtr const& other) override;

    void reset() override;

    /**
     * @brief Get file path, needed for: ArrowSource, BinarySource
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getFilePath() const;

    /**
     * @brief Set file path, needed for: ArrowSource, BinarySource
     */
    void setFilePath(const std::string filePath);

    /**
     * @brief gets a ConfigurationOption object with sourceGatheringInterval
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getGatheringInterval() const;

    /**
     * @brief set the value for sourceGatheringInterval with the appropriate data format
     */
    void setGatheringInterval(const uint32_t sourceGatheringIntervalValue);

    /**
     * @brief gets a ConfigurationOption object with numberOfBuffersToProduce
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getNumberOfBuffersToProduce() const;

    /**
     * @brief set the value for numberOfBuffersToProduce with the appropriate data format
     */
    void setNumberOfBuffersToProduce(const uint32_t numberOfBuffersToProduce);

    /**
     * @brief gets a ConfigurationOption object with numberOfTuplesToProducePerBuffer
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief Get gathering mode
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<GatheringMode>> getGatheringMode() const;

    /**
     * @brief Set gathering mode
     */
    void setGatheringMode(std::string inputGatheringMode);

    /**
     * @brief Sets the gathering mode given as GatheringMode
     * @param inputGatheringMode
     */
    void setGatheringMode(GatheringMode inputGatheringMode);

    /**
     * @brief set the value for numberOfTuplesToProducePerBuffer with the appropriate data format
     */
    void setNumberOfTuplesToProducePerBuffer(const uint32_t numberOfTuplesToProducePerBuffer);

  private:
    /**
     * @brief constructor to create a new Arrow source config object initialized with values from sourceConfigMap
     */
    explicit ArrowSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Arrow source config object initialized with values from sourceConfigMap
     */
    explicit ArrowSourceType(Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new Arrow source config object initialized with default values
     */
    ArrowSourceType();

    Configurations::StringConfigOption filePath;
    Configurations::IntConfigOption numberOfBuffersToProduce;
    Configurations::IntConfigOption numberOfTuplesToProducePerBuffer;
    Configurations::IntConfigOption sourceGatheringInterval;
    Configurations::GatheringModeConfigOption gatheringMode;
};

}// namespace NES
#endif// NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_ARROWSOURCETYPE_HPP_
