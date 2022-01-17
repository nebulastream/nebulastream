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

#ifndef NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_HPP_
#define NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_HPP_

#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfig.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <Util/yaml/rapidyaml.hpp>

namespace NES {

namespace Configurations {

/*
 * Constant config strings to read specific values from yaml or command line input
 */

const std::string PHYSICAL_STREAM_NAME_CONFIG = "physicalStreamName";
const std::string LOGICAL_STREAM_NAME_CONFIG = "logicalStreamName";

class PhysicalStreamTypeConfig;
using PhysicalStreamTypeConfigPtr = std::shared_ptr<PhysicalStreamTypeConfig>;

template<class T>
class ConfigurationOption;
using StringConfigOption = std::shared_ptr<ConfigurationOption<std::string>>;

/**
 * @brief Configuration object for source config
 */
class PhysicalStreamTypeConfig : public std::enable_shared_from_this<PhysicalStreamTypeConfig> {

  public:

    /**
     * @brief create a PhysicalStreamConfigPtr object
     * @return PhysicalStreamConfigPtr
     */
    static PhysicalStreamTypeConfigPtr create(const std::map<std::string, std::string>& inputParams);

    /**
     * @brief create a PhysicalStreamConfigPtr object
     * @return PhysicalStreamConfigPtr
     */
    static PhysicalStreamTypeConfigPtr create(ryml::NodeRef physicalStreamConfigNode);

    /**
     * @brief create a PhysicalStreamConfigPtr object
     * @return PhysicalStreamConfigPtr
     */
    static PhysicalStreamTypeConfigPtr create(std::string sourceType);

    /**
     * @brief create a PhysicalStreamConfigPtr object
     * @return PhysicalStreamConfigPtr
     */
    static PhysicalStreamTypeConfigPtr create();

    ~PhysicalStreamTypeConfig() = default;

    /**
     * @brief resets all options to default values
     */
    void resetPhysicalStreamOptions();

    /**
     * @brief prints the current source configuration (name: current value)
     */
    std::string toString();

    /**
     * Checks equality
     * @param other sourceConfig ot check equality for
     * @return true if equal, false otherwise
     */
    bool equal(PhysicalStreamTypeConfigPtr const& other);

    /**
     * @brief gets a ConfigurationOption object with physicalStreamName
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getPhysicalStreamName() const;

    /**
     * @brief set the value for physicalStreamName with the appropriate data format
     */
    void setPhysicalStreamName(std::string physicalStreamName);

    /**
     * @brief gets a ConfigurationOption object with logicalStreamName
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getLogicalStreamName() const;

    /**
     * @brief set the value for logicalStreamName with the appropriate data format
     */
    void setLogicalStreamName(std::string logicalStreamName);

    const SourceTypeConfigPtr& getSourceTypeConfig() const;
    void setSourceTypeConfig(const SourceTypeConfigPtr& sourceTypeConfigValue);

  private:
    /**
     * @brief constructor to create a new source option object initialized with values from sourceConfigMap
     */
    explicit PhysicalStreamTypeConfig(const std::map<std::string, std::string>& inputParams);

    /**
     * @brief constructor to create a new source option object initialized with values from sourceConfigMap
     */
    explicit PhysicalStreamTypeConfig(ryml::NodeRef physicalStreamConfigNode);

    /**
     * @brief constructor to create a new source option object initialized with values from sourceConfigMap
     */
    explicit PhysicalStreamTypeConfig(std::string sourceType);

    /**
     * @brief constructor to create a new source option object initialized with values from sourceConfigMap
     */
    explicit PhysicalStreamTypeConfig();

    StringConfigOption physicalStreamName;
    StringConfigOption logicalStreamName;
    SourceTypeConfigPtr sourceTypeConfig;

};
}// namespace Configurations
}// namespace NES
#endif// NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_HPP_
