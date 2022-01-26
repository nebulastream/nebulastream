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

#ifndef NES_MATERIALIZEDVIEWSOURCETYPE_HPP
#define NES_MATERIALIZEDVIEWSOURCETYPE_HPP

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Configurations/ConfigurationOption.hpp>

namespace NES {

namespace Configurations {

namespace Experimental::MaterializedView {

class MaterializedViewSourceType;
using MaterializedViewSourceTypePtr = std::shared_ptr<MaterializedViewSourceType>;

/**
 * @brief Configuration object for materialized view source config
 */
class MaterializedViewSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a MaterializedViewSourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return MaterializedViewSourceTypePtr
     */
    static MaterializedViewSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a MaterializedViewSourceTypePtr object with default values
     * @return MaterializedViewSourceTypePtr
     */
    static MaterializedViewSourceTypePtr create();

    /**
     * @brief resets all source configuration to default values
     */
    void reset() override;

    /**
     * @brief creates a string representation of the source
     * @return string object
     */
    std::string toString() override;

    /**
     * @brief get id of materialized view to use
     */
    [[nodiscard]] uint32_t getId() const;

    /**
     * @brief set id of materialized view to use
     */
    void setId(uint32_t id);

    bool equal(const PhysicalSourceTypePtr& other) override;

  private:
    /**
     * @brief constructor to create a new materialized view source config object initialized with values from sourceConfigMap
     */
    explicit MaterializedViewSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new materialized view source config object initialized with default values as set below
     */
    MaterializedViewSourceType();

    IntConfigOption id;
};
}// namespace Experimental::MaterializedView
}// namespace Configurations
}// namespace NES
#endif//NES_MATERIALIZEDVIEWSOURCETYPE_HPP