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

#ifndef NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_SOURCE_TYPE_CONFIG_HPP_
#define NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_SOURCE_TYPE_CONFIG_HPP_

#include <map>
#include <memory>
#include <string>
#include <Util/yaml/rapidyaml.hpp>

namespace NES {

namespace Configurations {

class SourceTypeConfig;
using SourceTypeConfigPtr = std::shared_ptr<SourceTypeConfig>;

template<class T>
class ConfigurationOption;
using FloatConfigOption = std::shared_ptr<ConfigurationOption<float>>;
using IntConfigOption = std::shared_ptr<ConfigurationOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigurationOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigurationOption<bool>>;

/**
 * @brief Configuration object for source config
 */
class SourceTypeConfig : public std::enable_shared_from_this<SourceTypeConfig> {

  public:

    /**
     * @brief constructor to create a new source option object initialized with default values as set below
     * @param sourceType type of source from where object was initialized
     */
    explicit SourceTypeConfig(std::string _sourceType);

    virtual ~SourceTypeConfig() = default;

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
     * Checks equality
     * @param other sourceConfig ot check equality for
     * @return true if equal, false otherwise
     */
    virtual bool equal(SourceTypeConfigPtr const& other);

    /**
     * @brief gets a ConfigurationOption object with sourceType
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getSourceType() const;

    /**
     * @brief set the value for sourceType with the appropriate data format
     */
    void setSourceType(std::string sourceTypeValue);



  private:
    StringConfigOption sourceType;
};
}// namespace Configurations
}// namespace NES
#endif// NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_SOURCE_TYPE_CONFIG_HPP_
