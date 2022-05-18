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

#ifndef NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_TCPSOURCETYPE_HPP
#define NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_TCPSOURCETYPE_HPP

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES {

class TCPSourceType;
using TCPSourceTypePtr = std::shared_ptr<TCPSourceType>;

class TCPSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a TCPSourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return TCPSourceTypePtr
     */
    static TCPSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a TCPSourceTypePtr object
     * @param yamlConfig inputted config options
     * @return TCPSourceTypePtr
     */
    static TCPSourceTypePtr create(Yaml::Node yamlConfig);

    /**
     * @brief create a TCPSourceTypePtr object with default values
     * @return TCPSourceTypePtr
     */
    static TCPSourceTypePtr create();

    /**
     * @brief converts configs to string
     * @return string of configs
     */
    std::string toString() override;

    /**
     * @brief checks if two config objects are equal
     * @param other other config object
     * @return true if equal, false otherwise
     */
    bool equal(const PhysicalSourceTypePtr& other) override;

    /**
     * @brief set config obtions to default
     */
    void reset() override;

    /**
     * @brief set ip adress
     * @param url new ip address
     */
    void setUrl(std::string url);

    /**
     * @brief get ip address
     * @return ip adress
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getUrl() const;

  private:
    /**
     * @brief constructor to create a new TCP source type object initialized with values from sourceConfigMap
     * @param sourceConfigMap inputted config options
     */
    explicit TCPSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new TCP source type object initialized with values from yamlConfig
     * @param yamlConfig inputted config options
     */
    explicit TCPSourceType(Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new TCP source type object initialized with default values
     */
    TCPSourceType();

    Configurations::StringConfigOption url;
};
}// namespace NES
#endif//NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_TCPSOURCETYPE_HPP
