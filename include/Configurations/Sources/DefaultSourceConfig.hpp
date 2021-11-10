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

#ifndef NES_DEFAULTSOURCECONFIG_HPP
#define NES_DEFAULTSOURCECONFIG_HPP

#include <Configurations/Sources/SourceConfig.hpp>
#include <map>
#include <string>

namespace NES {

namespace Configurations {

class DefaultSourceConfig;
using DefaultSourceConfigPtr = std::shared_ptr<DefaultSourceConfig>;

/**
 * @brief Configuration object for default source config
 * A simple source with default data created inside NES, useful for testing
 */
class DefaultSourceConfig : public SourceConfig {

  public:
    /**
     * @brief create a DefaultSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return DefaultSourceConfigPtr
     */
    static DefaultSourceConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create defaultSourceConfig with default values
     * @return defaultSourceConfig with default values
     */
    static DefaultSourceConfigPtr create();
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
    explicit DefaultSourceConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Default source config object initialized with default values
     */
    DefaultSourceConfig();
};
}// namespace Configurations
}// namespace NES
#endif