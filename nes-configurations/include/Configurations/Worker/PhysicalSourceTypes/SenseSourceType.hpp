/*
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

#ifndef NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_SENSESOURCETYPE_HPP_
#define NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_SENSESOURCETYPE_HPP_

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>
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
    static SenseSourceTypePtr
    create(const std::string& logicalSourceName, const std::string& physicalSourceName, std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a SenseSourceConfigPtr object
     * @param yamlConfig inputted config options
     * @return SenseSourceConfigPtr
     */
    static SenseSourceTypePtr create(const std::string& logicalSourceName, const std::string& physicalSourceName, Yaml::Node yamlConfig);

    /**
     * @brief create a SenseSourceConfigPtr object
     * @return SenseSourceConfigPtr
     */
    static SenseSourceTypePtr create(const std::string& logicalSourceName, const std::string& physicalSourceName);

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
    void setUdfs(const std::string& udfs);

    void reset() override;

  private:
    /**
     * @brief constructor to create a new Sense source config object initialized with values form sourceConfigMap
     */
    SenseSourceType(const std::string& logicalSourceName,
                    const std::string& physicalSourceName,
                    std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Sense source config object initialized with values form sourceConfigMap
     */
    SenseSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName, Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new Sense source config object initialized with default values
     */
    SenseSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName);

    Configurations::StringConfigOption udfs;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_SENSESOURCETYPE_HPP_
