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

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/rapidyaml.hpp>
#include <map>
#include <string>

namespace NES {

class SenseSourceType;
using SenseSourceTypePtr = std::shared_ptr<SenseSourceType>;

/**
* @brief Configuration object for source config
*/
class SenseSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a SenseSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return SenseSourceConfigPtr
     */
    static SenseSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a SenseSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return SenseSourceConfigPtr
     */
    static SenseSourceTypePtr create(ryml::NodeRef sourcTypeConfig);

    /**
     * @brief create a SenseSourceConfigPtr object
     * @return SenseSourceConfigPtr
     */
    static SenseSourceTypePtr create();

    ~SenseSourceType() = default;

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    /**
     * @brief Get udsf
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getUdfs() const;

    /**
     * @brief Set udsf
     */
    void setUdfs(std::string udfs);

    void reset() override;

  private:
    /**
     * @brief constructor to create a new Sense source config object initialized with values form sourceConfigMap
     */
    SenseSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Sense source config object initialized with values form sourceConfigMap
     */
    SenseSourceType(ryml::NodeRef sourcTypeConfig);

    /**
     * @brief constructor to create a new Sense source config object initialized with default values
     */
    SenseSourceType();

    Configurations::StringConfigOption udfs;
};
}// namespace NES
#endif