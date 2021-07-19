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

#ifndef NES_SOURCECONFIG_HPP
#define NES_SOURCECONFIG_HPP

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class SourceConfig;
typedef std::shared_ptr<SourceConfig> SourceConfigPtr;

template<class T>
class ConfigOption;
typedef std::shared_ptr<ConfigOption<uint32_t>> IntConfigOption;
typedef std::shared_ptr<ConfigOption<std::string>> StringConfigOption;
typedef std::shared_ptr<ConfigOption<bool>> BoolConfigOption;
typedef std::shared_ptr<ConfigOption<std::vector<std::string>>> VectorStringConfigOption;

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
     * @brief overwrite the config with configuration parsed from the string
     * @param configString the string containing the configuration
     */
    void overwriteConfigWithStringInput(const std::string& configString);

    /**
     * @brief serializes the configuration to a JSON string representation
     * @return a JSON string
     */
    std::string toJson();

    /**
     * @brief resets all options to default values
     */
    void resetSourceOptions();

    /**
     * @brief gets a ConfigOption object with sourceType
     */
    const std::shared_ptr<ConfigOption<std::string>> getSourceType() const;

    /**
     * @brief set the value for sourceType with the appropriate data format
     */
    void setSourceType(std::string sourceTypeValue);

    /**
     * @brief gets a ConfigOption object with sourceConfig
     */
    const std::shared_ptr<ConfigOption<std::string>> getSourceConfig() const;

    /**
     * @brief set the value for sourceConfig with the appropriate data format
     */
    void setSourceConfig(std::string sourceConfigValue);

    /**
     * @brief gets a ConfigOption object with sourceFrequency
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getSourceFrequency() const;

    /**
     * @brief set the value for sourceFrequency with the appropriate data format
     */
    void setSourceFrequency(uint32_t sourceFrequencyValue);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersToProduce
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getNumberOfBuffersToProduce() const;

    /**
     * @brief set the value for numberOfBuffersToProduce with the appropriate data format
     */
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    /**
     * @brief gets a ConfigOption object with numberOfTuplesToProducePerBuffer
     */
    const std::shared_ptr<ConfigOption<uint32_t>> getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief set the value for numberOfTuplesToProducePerBuffer with the appropriate data format
     */
    void setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer);

    /**
     * @brief gets a ConfigOption object with physicalStreamName
     */
    const std::shared_ptr<ConfigOption<std::string>> getPhysicalStreamName() const;

    /**
     * @brief set the value for physicalStreamName with the appropriate data format
     */
    void setPhysicalStreamName(std::string physicalStreamName);

    /**
     * @brief gets a ConfigOption object with a vector of logicalStreamNames
     */
    const std::shared_ptr<ConfigOption<std::vector<std::string>>> getLogicalStreamName() const;

    /**
     * @brief set the value for logicalStreamName with the appropriate data format
     */
    void setLogicalStreamName(std::vector<std::string> logicalStreamName);

    /**
     * @brief set the value for logicalStreamName with the appropriate data format (expects a single string with the names separated by comma)
     */
    void setLogicalStreamName(std::string logicalStreamName);

    /**
     * @brief add the logicalStreamName to the vector of logicalStreamNames
     */
    void addLogicalStreamName(std::string logicalStreamName);

    /**
     * @brief gets a ConfigOption object with skipHeader
     */
    const std::shared_ptr<ConfigOption<bool>> getSkipHeader() const;

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
    VectorStringConfigOption logicalStreamName;
    BoolConfigOption skipHeader;
};
}// namespace NES
#endif//NES_SOURCECONFIG_HPP
