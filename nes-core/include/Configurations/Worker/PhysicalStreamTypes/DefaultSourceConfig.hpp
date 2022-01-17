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

#ifndef NES_DEFAULTSOURCETYPECONFIG_HPP
#define NES_DEFAULTSOURCETYPECONFIG_HPP

#include <Configurations/Worker/PhysicalStreamConfig/PhysicalStreamTypeConfiguration.hpp>
#include <map>
#include <string>

namespace NES {

namespace Configurations {

class DefaultSourceTypeConfig;
using DefaultSourceTypeConfigPtr = std::shared_ptr<DefaultSourceTypeConfig>;

/**
 * @brief Configuration object for default source config
 * A simple source with default data created inside NES, useful for testing
 */
class DefaultSourceTypeConfig : public PhysicalStreamTypeConfiguration {

  public:
    /**
     * @brief create a DefaultSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return DefaultSourceConfigPtr
     */
    static DefaultSourceTypeConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a DefaultSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return DefaultSourceConfigPtr
     */
    static DefaultSourceTypeConfigPtr create(ryml::NodeRef sourcTypeConfig);

    /**
     * @brief create defaultSourceConfig with default values
     * @return defaultSourceConfig with default values
     */
    static DefaultSourceTypeConfigPtr create();
    /**
     * @brief resets all Source configuration to default values
     */
    void resetSourceOptions() override;
    /**
     * @brief creates a string representation of the source
     * @return string object
     */
    std::string toString() override;

    /**
     * Checks equality
     * @param other sourceConfig ot check equality for
     * @return true if equal, false otherwise
     */
    bool equal(PhysicalStreamTypeConfigurationPtr const& other) override;

    /**
     * @brief gets a ConfigurationOption object with numberOfBuffersToProduce
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<uint32_t>> getNumberOfBuffersToProduce() const;

    /**
     * @brief set the value for numberOfBuffersToProduce with the appropriate data format
     */
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    /**
     * @brief gets a ConfigurationOption object with sourceFrequency
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<uint32_t>> getSourceFrequency() const;

    /**
     * @brief set the value for sourceFrequency with the appropriate data format
     */
    void setSourceFrequency(uint32_t sourceFrequencyValue);

  private:
    /**
     * @brief constructor to create a new Default source config object using the sourceConfigMap for configs
     */
    explicit DefaultSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Default source config object using the sourceConfigMap for configs
     */
    explicit DefaultSourceTypeConfig(ryml::NodeRef sourceTypeConfig);

    /**
     * @brief constructor to create a new Default source config object initialized with default values
     */
    DefaultSourceTypeConfig();
    IntConfigOption numberOfBuffersToProduce;
    IntConfigOption sourceFrequency;

};
}// namespace Configurations
}// namespace NES
#endif