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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCETYPES_OPCSOURCETYPE_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCETYPES_OPCSOURCETYPE_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <string>

namespace NES {

class OPCSourceType;
using OPCSourceTypePtr = std::shared_ptr<OPCSourceType>;

/**
 * @brief Configuration object for OPC source config
 * connect to an OPC server and read data from there
 */
class OPCSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a OPCSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return OPCSourceConfigPtr
     */
    static OPCSourceTypePtr create(const std::string& logicalSourceName, const std::string& physicalSourceName,std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a OPCSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return OPCSourceConfigPtr
     */
    static OPCSourceTypePtr create(const std::string& logicalSourceName, const std::string& physicalSourceName,const Yaml::Node& yamlConfig);

    /**
     * @brief create a OPCSourceConfigPtr object
     * @return OPCSourceConfigPtr
     */
    static OPCSourceTypePtr create(const std::string& logicalSourceName, const std::string& physicalSourceName);

    ~OPCSourceType() = default;

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::uint32_t>> getNamespaceIndex() const;

    /**
     * @brief Set namespaceIndex for node
     */
    void setNamespaceIndex(uint32_t namespaceIndex);

    /**
     * @brief Get node identifier
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getNodeIdentifier() const;

    /**
     * @brief Set node identifier
     */
    void setNodeIdentifier(const std::string& nodeIdentifier);

    /**
     * @brief Get userName
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getUserName() const;

    /**
     * @brief Set userName
     */
    void setUserName(const std::string& userName);

    /**
     * @brief Get password
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getPassword() const;

    /**
     * @brief Set password
     */
    void setPassword(const std::string& password);

  private:
    /**
     * @brief constructor to create a new OPC source config object initialized with values form sourceConfigMap
     */
    explicit OPCSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName,std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new OPC source config object initialized with values form sourceConfigMap
     */
    explicit OPCSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName,Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new OPC source config object initialized with default values
     */
    OPCSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName);

    Configurations::IntConfigOption namespaceIndex;
    Configurations::StringConfigOption nodeIdentifier;
    Configurations::StringConfigOption userName;
    Configurations::StringConfigOption password;
};
}// namespace NES
#endif  // NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCETYPES_OPCSOURCETYPE_HPP_
