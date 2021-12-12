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

#ifndef NES_MATERIALIZEDVIEWSOURCECONFIG_HPP
#define NES_MATERIALIZEDVIEWSOURCECONFIG_HPP

#include <Configurations/Sources/SourceConfig.hpp>

namespace NES {

namespace Configurations {

class MaterializedViewSourceConfig;
using MaterializedViewSourceConfigPtr = std::shared_ptr<MaterializedViewSourceConfig>;

/**
 * @brief ...
 */
class MaterializedViewSourceConfig : public SourceConfig {

public:
    /**
     * @brief
     */
    static MaterializedViewSourceConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief
     */
    static MaterializedViewSourceConfigPtr create();

    /**
     * @brief resets all source configuration to default values
     */
    void resetSourceOptions() override;

    /**
     * @brief creates a string representation of the source
     * @return string object
     */
    std::string toString() override;

    /**
     * @brief get id of materialized view to use
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::uint32_t>> getId() const;

    /**
     * @brief set id of materialized view to use
     */
    void setId(uint32_t id);

private:
    /**
     * @brief constructor to create a new materialized view source config object initialized with values from sourceConfigMap
     */
    explicit MaterializedViewSourceConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new materialized view source config object initialized with default values as set below
     */
    MaterializedViewSourceConfig();

    IntConfigOption id;
};
}// namespace Configurations
}// namespace NES
#endif //NES_MATERIALIZEDVIEWSOURCECONFIG_HPP