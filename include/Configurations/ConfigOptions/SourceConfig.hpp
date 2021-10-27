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

#ifndef NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_SOURCE_CONFIG_HPP_
#define NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_SOURCE_CONFIG_HPP_

#include <map>
#include <string>

namespace NES {

class SourceConfig;
using SourceConfigPtr = std::shared_ptr<SourceConfig>;

template<class T>
class ConfigOption;
using IntConfigOption = std::shared_ptr<ConfigOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigOption<bool>>;

/**
 * @brief Configuration object for source config
 */
class SourceConfig {

  public:
    static SourceConfigPtr create();

    /**
     * @brief overwrite the default configurations with those loaded from a yaml file
     * @param filePath file path to the yaml file
     */
    void overwriteConfigWithYAMLFileInput(const std::string& filePath);

    /**
     * @brief overwrite the default and the yaml file configurations with command line input
     * @param inputParams map with key=command line parameter and value = value
     */
    void overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams);

    /**
     * @brief resets all options to default values
     */
    void resetSourceOptions();

    /**
     * @brief gets a ConfigOption object with sourceType
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getSourceType() const;

    /**
     * @brief set the value for sourceType with the appropriate data format
     */
    void setSourceType(std::string sourceTypeValue);

    /**
     * @brief gets a ConfigOption object with sourceConfig
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getSourceConfig() const;

    /**
     * @brief set the value for sourceConfig with the appropriate data format
     */
    void setSourceConfig(std::string sourceConfigValue);

    /**
     * @brief gets a ConfigOption object with sourceFrequency
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getSourceFrequency() const;

    /**
     * @brief set the value for sourceFrequency with the appropriate data format
     */
    void setSourceFrequency(uint32_t sourceFrequencyValue);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersToProduce
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getNumberOfBuffersToProduce() const;

    /**
     * @brief set the value for numberOfBuffersToProduce with the appropriate data format
     */
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    /**
     * @brief gets a ConfigOption object with numberOfTuplesToProducePerBuffer
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief set the value for numberOfTuplesToProducePerBuffer with the appropriate data format
     */
    void setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer);

    /**
     * @brief gets a ConfigOption object with physicalStreamName
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getPhysicalStreamName() const;

    /**
     * @brief set the value for physicalStreamName with the appropriate data format
     */
    void setPhysicalStreamName(std::string physicalStreamName);

    /**
     * @brief gets a ConfigOption object with logicalStreamName
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getLogicalStreamName() const;

    /**
     * @brief set the value for logicalStreamName with the appropriate data format
     */
    void setLogicalStreamName(std::string logicalStreamName);

    /**
     * @brief gets a ConfigOption object with skipHeader
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<bool>> getSkipHeader() const;

    /**
     * @brief set the value for skipHeader with the appropriate data format
     */
    void setSkipHeader(bool skipHeader);

  private:
    /**
     * @brief constructor to create a new source option object initialized with default values as set below
     */
    SourceConfig();
    StringConfigOption sourceType;
    StringConfigOption sourceConfig;
    IntConfigOption sourceFrequency;
    IntConfigOption numberOfBuffersToProduce;
    IntConfigOption numberOfTuplesToProducePerBuffer;
    StringConfigOption physicalStreamName;
    StringConfigOption logicalStreamName;
    BoolConfigOption skipHeader;
};
}// namespace NES
#endif  // NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_SOURCE_CONFIG_HPP_
