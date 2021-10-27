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
#include <memory>
#include <string>

namespace NES {

namespace Configurations {

class SourceConfig;
using SourceConfigPtr = std::shared_ptr<SourceConfig>;

template<class T>
class ConfigOption;
using FloatConfigOption = std::shared_ptr<ConfigOption<float>>;
using IntConfigOption = std::shared_ptr<ConfigOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigOption<bool>>;

/**
 * @brief Configuration object for source config
 */
class SourceConfig : public std::enable_shared_from_this<SourceConfig> {

  public:
    /**
     * @brief constructor to create a new source option object initialized with values from sourceConfigMap
     * @param sourceConfigMap with input params
     * @param sourceType type of source from where object was initialized
     */
    explicit SourceConfig(std::map<std::string, std::string> sourceConfigMap, std::string _sourceType);

    /**
     * @brief constructor to create a new source option object initialized with default values as set below
     * @param sourceType type of source from where object was initialized
     */
    explicit SourceConfig(std::string _sourceType);

    virtual ~SourceConfig() = default;

    /**
     * @brief resets all options to default values
     */
    virtual void resetSourceOptions() = 0;

    /**
     * @brief resets all options to default values
     * @param sourceType also reset source type to current source type object
     */
    virtual void resetSourceOptions(std::string _sourceType);

    /**
     * @brief prints the current source configuration (name: current value)
     */
    virtual std::string toString();

    /**
     * @brief gets a ConfigOption object with sourceType
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getSourceType() const;

    /**
     * @brief set the value for sourceType with the appropriate data format
     */
    void setSourceType(std::string sourceTypeValue);

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
     * @brief Get input data format
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getInputFormat() const;

    /**
     * @brief Set input data format
     */
    void setInputFormat(std::string inputFormat);

    /**
     * @brief Get storage layout, true = row layout, false = column layout
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<bool>> getRowLayout() const;

    /**
     * @brief Set storage layout, true = row layout, false = column layout
     */
    void setRowLayout(bool rowLayout);

    /**
     * @brief Checks if the current Source is of type SourceConfig
     * @tparam SourceConfig
     * @return bool true if Source is of SourceConfig
     */
    template<class SourceConfig>
    bool instanceOf() {
        if (dynamic_cast<SourceConfig*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the Source to a SourceConfigType
    * @tparam SourceConfigType
    * @return returns a shared pointer of the SourceConfigType
    */
    template<class SourceConfig>
    std::shared_ptr<SourceConfig> as() {
        if (instanceOf<SourceConfig>()) {
            return std::dynamic_pointer_cast<SourceConfig>(this->shared_from_this());
        }
        throw std::logic_error("Node:: we performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(SourceConfig).name());
        return nullptr;
    }

  private:
    IntConfigOption numberOfBuffersToProduce;
    IntConfigOption numberOfTuplesToProducePerBuffer;
    StringConfigOption physicalStreamName;
    StringConfigOption logicalStreamName;
    IntConfigOption sourceFrequency;
    BoolConfigOption rowLayout;
    StringConfigOption inputFormat;
    StringConfigOption sourceType;
};
}// namespace Configurations
}// namespace NES
#endif// NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_SOURCE_CONFIG_HPP_
