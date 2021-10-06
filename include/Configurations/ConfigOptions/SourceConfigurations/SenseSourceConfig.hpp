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

#ifndef NES_SENSESOURCECONFIG_HPP
#define NES_SENSESOURCECONFIG_HPP

#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfig.hpp>
#include <map>
#include <string>

namespace NES {

class SenseSourceConfig;
using SenseSourceConfigPtr = std::shared_ptr<SenseSourceConfig>;

template<class T>
class ConfigOption;
using FloatConfigOption = std::shared_ptr<ConfigOption<float>>;
using IntConfigOption = std::shared_ptr<ConfigOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigOption<bool>>;

/**
* @brief Configuration object for source config
*/
class SenseSourceConfig : public SourceConfig {

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

    ~SenseSourceConfig() override = default;

    /**
     * @brief resets alls Source configuration to default values
     */
    void resetSourceOptions() override;
    /**
     * @brief creates a string representation of the source
     * @return
     */
    std::string toString() override;

    /**
     * @brief Get udsf
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getUdsf() const;

    /**
     * @brief Set udsf
     */
    void setUdsf(std::string udsf);

  private:

    /**
     * @brief constructor to create a new Sense source config object initialized with values form sourceConfigMap
     */
    explicit SenseSourceConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Sense source config object initialized with default values
     */
    SenseSourceConfig();

    StringConfigOption udsf;
};
}// namespace NES
#endif