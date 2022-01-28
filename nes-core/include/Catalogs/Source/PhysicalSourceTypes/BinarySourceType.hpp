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

#ifndef NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_BINARYSOURCETYPE_HPP_
#define NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_BINARYSOURCETYPE_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <string>

namespace NES {

class BinarySourceType;
using BinarySourceTypePtr = std::shared_ptr<BinarySourceType>;

/**
 * @brief Configuration object for binary source
 * A binary source reads data from a binary file
 */
class BinarySourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a BinarySourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return BinarySourceTypePtr
     */
    static BinarySourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a BinarySourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return BinarySourceTypePtr
     */
    static BinarySourceTypePtr create(Yaml::Node yamlConfig);

    /**
     * @brief create a BinarySourceTypePtr object with default values
     * @return BinarySourceTypePtr
     */
    static BinarySourceTypePtr create();

    ~BinarySourceType() = default;

    std::string toString() override;

    bool equal(PhysicalSourceTypePtr const& other) override;

    void reset() override;

    /**
     * @brief Get file path
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getFilePath() const;

    /**
     * @brief Set file path
     */
    void setFilePath(std::string filePath);

  private:
    /**
     * @brief constructor to create a new Binary source config object initialized with values from sourceConfigMap
     */
    explicit BinarySourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Binary source config object initialized with values from sourceConfigMap
     */
    explicit BinarySourceType(Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new Binary source config object initialized with default values as set below
     */
    BinarySourceType();

    Configurations::StringConfigOption filePath;
};
}// namespace NES
#endif  // NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_BINARYSOURCETYPE_HPP_
