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

class PhysicalStreamConfig;
using PhysicalStreamConfigPtr = std::shared_ptr<PhysicalStreamConfig>;

template<class T>
class ConfigOption;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;

/**
 * @brief Configuration object for source config
 */
class PhysicalStreamConfig : public std::enable_shared_from_this<PhysicalStreamConfig> {

  public:
    /**
     * @brief create a PhysicalStreamConfigPtr object
     * @return PhysicalStreamConfigPtr
     */
    static PhysicalStreamConfigPtr create();

    /**
     * @brief constructor to create a new source option object initialized with values from sourceConfigMap
     */
    explicit PhysicalStreamConfig();


    ~PhysicalStreamConfig() = default;

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
    bool equal(PhysicalStreamConfigPtr const& other);

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

    const SourceTypeConfigPtr& getSourceTypeConfig() const;
    void setSourceTypeConfig(const SourceTypeConfigPtr& sourceTypeConfig);

    /**
     * @brief Checks if the current Source is of type SourceConfig
     * @tparam SourceConfig
     * @return bool true if Source is of SourceConfig
     */
    template<class PhysicalStreamConfig>
    bool instanceOf() {
        if (dynamic_cast<PhysicalStreamConfig*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the Source to a SourceConfigType
    * @tparam SourceConfigType
    * @return returns a shared pointer of the SourceConfigType
    */
    template<class PhysicalStreamConfig>
    std::shared_ptr<PhysicalStreamConfig> as() {
        if (instanceOf<PhysicalStreamConfig>()) {
            return std::dynamic_pointer_cast<PhysicalStreamConfig>(this->shared_from_this());
        }
        throw std::logic_error("Node:: we performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(PhysicalStreamConfig).name());
        return nullptr;
    }

  private:
    StringConfigOption physicalStreamName;
    StringConfigOption logicalStreamName;
    SourceTypeConfigPtr sourceTypeConfig;

};
}// namespace Configurations
}// namespace NES
#endif// NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_HPP_
