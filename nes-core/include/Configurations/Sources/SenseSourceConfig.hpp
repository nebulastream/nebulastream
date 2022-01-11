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

#ifndef NES_SENSESOURCETYPECONFIG_HPP
#define NES_SENSESOURCETYPECONFIG_HPP

#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfig.hpp>
#include <map>
#include <string>

namespace NES {

namespace Configurations {

class SenseSourceTypeConfig;
using SenseSourceTypeConfigPtr = std::shared_ptr<SenseSourceTypeConfig>;

/**
* @brief Configuration object for source config
*/
class SenseSourceTypeConfig : public SourceTypeConfig {

  public:
    /**
     * @brief create a SenseSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return SenseSourceConfigPtr
     */
    static SenseSourceConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a SenseSourceConfigPtr object
     * @return SenseSourceConfigPtr
     */
    static SenseSourceConfigPtr create();

    ~SenseSourceTypeConfig() override = default;

    /**
     * @brief resets alls Source configuration to default values
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
    bool equal(SourceTypeConfigPtr const& other) override;

    /**
     * @brief Get udsf
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getUdfs() const;

    /**
     * @brief Set udsf
     */
    void setUdfs(std::string udfs);

  private:
    /**
     * @brief constructor to create a new Sense source config object initialized with values form sourceConfigMap
     */
    explicit SenseSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Sense source config object initialized with default values
     */
    SenseSourceTypeConfig();

    StringConfigOption udfs;
};
}// namespace Configurations
}// namespace NES
#endif