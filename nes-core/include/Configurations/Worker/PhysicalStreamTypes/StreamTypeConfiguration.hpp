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

#include <Configurations/ConfigurationOption.hpp>
#include <Util/yaml/rapidyaml.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NES {

namespace Configurations {

enum StreamType { CSV, BINARY, OPC, MQTT, KAFKA, SENSE };

class StreamTypeConfiguration;
using PhysicalStreamTypePtr = std::shared_ptr<StreamTypeConfiguration>;

/**
 * @brief Configuration object for source config
 */
class StreamTypeConfiguration : public std::enable_shared_from_this<StreamTypeConfiguration> {

  public:
    /**
     * @brief create a PhysicalStreamConfigPtr object
     * @return PhysicalStreamConfigPtr
     */
    static PhysicalStreamTypePtr create(const std::map<std::string, std::string>& inputParams);

    /**
     * @brief create a PhysicalStreamConfigPtr object
     * @return PhysicalStreamConfigPtr
     */
    static PhysicalStreamTypePtr create(ryml::NodeRef yamlConfigs);

    /**
     * @brief create a PhysicalStreamConfigPtr object
     * @return PhysicalStreamConfigPtr
     */
    static PhysicalStreamTypePtr create(std::string streamType);

    ~StreamTypeConfiguration() = default;

    /**
     * @brief resets all options to default values
     */
    virtual void resetPhysicalStreamOptions() = 0;

    /**
     * @brief prints the current source configuration (name: current value)
     */
    virtual std::string toString() = 0;

    /**
     * Checks equality
     * @param other sourceConfig ot check equality for
     * @return true if equal, false otherwise
     */
    virtual bool equal(PhysicalStreamTypePtr const& other) = 0;

    /**
     * @brief Return stream type
     * @return enum representing stream type
     */
    virtual StreamType getStreamType() = 0;

  private:
    /**
     * @brief constructor to create a new source option object initialized with values from sourceConfigMap
     */
    explicit StreamTypeConfiguration(const std::map<std::string, std::string>& inputParams);

    /**
     * @brief constructor to create a new source option object initialized with values from sourceConfigMap
     */
    explicit StreamTypeConfiguration(ryml::NodeRef yamlConfigs);

    /**
     * @brief constructor to create a new source option object initialized with default values
     */
    explicit StreamTypeConfiguration(std::string streamType);
};
}// namespace Configurations
}// namespace NES
#endif// NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_HPP_
