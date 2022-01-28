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

#ifndef NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_DEFAULTSOURCETYPE_HPP_
#define NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_DEFAULTSOURCETYPE_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <memory>
#include <string>

namespace NES {

class DefaultSourceType;
using DefaultSourceTypePtr = std::shared_ptr<DefaultSourceType>;

/**
 * @brief Configuration object for default source config
 * A simple source with default data created inside NES, useful for testing
 */
class DefaultSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a DefaultSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return DefaultSourceConfigPtr
     */
    static DefaultSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a DefaultSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return DefaultSourceConfigPtr
     */
    static DefaultSourceTypePtr create(Yaml::Node yamlConfig);

    /**
     * @brief create defaultSourceConfig with default values
     * @return defaultSourceConfig with default values
     */
    static DefaultSourceTypePtr create();

    const Configurations::IntConfigOption& getNumberOfBuffersToProduce() const;

    const Configurations::IntConfigOption& getSourceFrequency() const;

    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    void setSourceFrequency(uint32_t numberOfBuffersToProduce);

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

  private:
    /**
     * @brief constructor to create a new Default source config object using the sourceConfigMap for physicalSources
     * @param sourceConfigMap: the source configuration map
     */
    explicit DefaultSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Default source config object using the sourceConfigMap for physicalSources
     * @param sourceTypeConfig: the configuration for yaml
     */
    explicit DefaultSourceType(Yaml::Node sourceTypeConfig);

    /**
     * @brief constructor to create a new Default source config object initialized with default values
     */
    DefaultSourceType();

    Configurations::IntConfigOption numberOfBuffersToProduce;
    Configurations::IntConfigOption sourceFrequency;
};
}// namespace NES
#endif  // NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_DEFAULTSOURCETYPE_HPP_
