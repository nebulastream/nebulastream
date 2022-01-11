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

#ifndef NES_OPCSOURCETYPECONFIG_HPP
#define NES_OPCSOURCETYPECONFIG_HPP

#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfig.hpp>
#include <map>
#include <string>

namespace NES {

namespace Configurations {

class OPCSourceTypeConfig;
using OPCSourceTypeConfigPtr = std::shared_ptr<OPCSourceTypeConfig>;

/**
 * @brief Configuration object for OPC source config
 * connect to an OPC server and read data from there
 */
class OPCSourceTypeConfig : public SourceTypeConfig {

  public:
    /**
     * @brief create a OPCSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return OPCSourceConfigPtr
     */
    static OPCSourceTypeConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a OPCSourceConfigPtr object
     * @return OPCSourceConfigPtr
     */
    static OPCSourceTypeConfigPtr create();

    ~OPCSourceTypeConfig() override = default;

    /**
     * @brief resets alls Source configuration to default values
     */
    void resetSourceOptions() override;
    /**
     * @brief creates a string representation of the source
     * @return string object
     */
    std::string toString() override;

    /**
     * Checks equality
     * @param other sourceConfig ot check equality for
     * @return true if equal, false otherwise
     */
    bool equal(SourceTypeConfigPtr const& other) override;

    [[nodiscard]] std::shared_ptr<ConfigOption<std::uint32_t>> getNamespaceIndex() const;

    /**
     * @brief Set namespaceIndex for node
     */
    void setNamespaceIndex(uint32_t namespaceIndex);

    /**
     * @brief Get node identifier
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getNodeIdentifier() const;

    /**
     * @brief Set node identifier
     */
    void setNodeIdentifier(std::string nodeIdentifier);

    /**
     * @brief Get userName
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getUserName() const;

    /**
     * @brief Set userName
     */
    void setUserName(std::string userName);

    /**
     * @brief Get password
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getPassword() const;

    /**
     * @brief Set password
     */
    void setPassword(std::string password);

  private:
    /**
     * @brief constructor to create a new OPC source config object initialized with values form sourceConfigMap
     */
    explicit OPCSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new OPC source config object initialized with default values
     */
    OPCSourceTypeConfig();

    IntConfigOption namespaceIndex;
    StringConfigOption nodeIdentifier;
    StringConfigOption userName;
    StringConfigOption password;
};
}// namespace Configurations
}// namespace NES
#endif