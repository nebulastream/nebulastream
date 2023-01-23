// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//         https://www.apache.org/licenses/LICENSE-2.0
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// *

#ifndef NES_LORAWANPROXYSOURCETYPE_HPP
#define NES_LORAWANPROXYSOURCETYPE_HPP

#include <Util/yaml/Yaml.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Common/Identifiers.hpp>
#include <EndDeviceProtocol.pb.h>
namespace NES {
class LoRaWANProxySourceType;
using LoRaWANProxySourceTypePtr = std::shared_ptr<LoRaWANProxySourceType>;

class LoRaWANProxySourceType: public PhysicalSourceType {
  public:
    /**
     * @brief create an LoRaWANProxySourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return LoRaWANProxySourceTypePtr
     */
    static LoRaWANProxySourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create an LoRaWANProxySourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return LoRaWANProxySourceTypePtr
     */
    static LoRaWANProxySourceTypePtr create(const Yaml::Node& yamlConfig);

    /**
     * @brief create an LoRaWANProxySourceTypePtr object with default values
     * @return LoRaWANProxySourceTypePtr
     */
    static LoRaWANProxySourceTypePtr create();

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

    [[nodiscard]] std::shared_ptr <Configurations::ConfigurationOption<std::string>> getNetworkStack();
    void setNetworkStack(std::string networkStack);
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getUrl();
    void setUrl( std::string url);
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getUserName();
    void setUserName( std::string userName);
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getPassword();
    void setPassword( std::string password);
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getAppId();
    void setAppId( std::string appId);
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::vector<std::string>>> getSensorFields();
    void setSensorFields (std::vector<std::string>);
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::vector<std::string>>> getDeviceEUIs();
    void setDeviceEUIs (std::vector<std::string>);

    void addSerializedQuery(QueryId, std::shared_ptr<EndDeviceProtocol::Query>);
    void removeSerializedQuery(QueryId);
    std::shared_ptr<std::map<QueryId,EndDeviceProtocol::Query>> getSerializedQueries();

    private:

    /**
     * @brief constructor to create a new LoRaWAN proxysource config object initialized with values from sourceConfigMap
     * @param sourceConfigMap inputted config options
     */
    explicit LoRaWANProxySourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new LoRaWAN proxysource config object initialized with values from yamlConfig
     * @param yamlConfig inputted config options
     */
    explicit LoRaWANProxySourceType(Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new LoRaWAN proxysource config object initialized with default values as set below
     */
    LoRaWANProxySourceType();
    
    Configurations::StringConfigOption networkStack;
    Configurations::StringConfigOption url;
    Configurations::StringConfigOption userName;
    Configurations::StringConfigOption password;
    Configurations::StringConfigOption appId;
    std::shared_ptr<Configurations::ConfigurationOption<std::vector<std::string>>> deviceEUIs;
    std::shared_ptr<Configurations::ConfigurationOption<std::vector<std::string>>> sensorFields;
    std::shared_ptr<std::map<QueryId,EndDeviceProtocol::Query>> queries;


};

}// namespace NES

#endif//NES_LORAWANPROXYSOURCETYPE_HPP
