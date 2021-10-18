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

#ifndef NES_NOSOURCECONFIG_HPP
#define NES_NOSOURCECONFIG_HPP

#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfig.hpp>
#include <map>
#include <string>

namespace NES {

class NoSourceConfig;
using NoSourceConfigPtr = std::shared_ptr<NoSourceConfig>;

template<class T>
class ConfigOption;
using FloatConfigOption = std::shared_ptr<ConfigOption<float>>;
using IntConfigOption = std::shared_ptr<ConfigOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigOption<bool>>;

/**
* @brief Configuration object for source config
*/
class NoSourceConfig : public SourceConfig {

  public:
    /**
     * @brief create a NoSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return NoSourceConfigPtr
     */
    static NoSourceConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create defaultSourceConfig with default values
     * @return defaultSourceConfig with default values
     */
    static NoSourceConfigPtr create();
    /**
     * @brief resets all Source configuration to default values
     */
    void resetSourceOptions() override;
    /**
     * @brief creates a string representation of the source
     * @return string object
     */
    std::string toString() override;

  private:
    /**
     * @brief constructor to create a new Default source config object using the sourceConfigMap for configs
     */
    explicit NoSourceConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Default source config object initialized with default values
     */
    NoSourceConfig();
};
}// namespace NES
#endif