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

#ifndef NES_MQTTSOURCETYPECONFIG_HPP
#define NES_MQTTSOURCETYPECONFIG_HPP

#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfig.hpp>
#include <map>
#include <string>

namespace NES {

namespace Configurations {

class MQTTSourceTypeConfig;
using MQTTSourceTypeConfigPtr = std::shared_ptr<MQTTSourceTypeConfig>;

/**
 * @brief Configuration object for MQTT source config
 * Connect to an MQTT broker and read data from there
 */
class MQTTSourceTypeConfig : public SourceTypeConfig {

  public:
    /**
     * @brief create a MQTTSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return MQTTSourceConfigPtr
     */
    static MQTTSourceTypeConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a MQTTSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return MQTTSourceConfigPtr
     */
    static MQTTSourceTypeConfigPtr create(ryml::NodeRef sourcTypeConfig);

    /**
     * @brief create a MQTTSourceConfigPtr object with default values
     * @return MQTTSourceConfigPtr
     */
    static MQTTSourceTypeConfigPtr create();

    /**
     * @brief resets all Source configuration to default values
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

    /**
     * @brief Get url to connect
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getUrl() const;

    /**
     * @brief Set url to connect to
     */
    void setUrl(std::string url);

    /**
     * @brief Get clientId
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getClientId() const;

    /**
     * @brief Set clientId
     */
    void setClientId(std::string clientId);

    /**
     * @brief Get userName
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getUserName() const;

    /**
     * @brief Set userName
     */
    void setUserName(std::string userName);

    /**
     * @brief Get topic to listen to
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getTopic() const;

    /**
     * @brief Set topic to listen to
     */
    void setTopic(std::string topic);

    /**
     * @brief Get quality of service
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<uint32_t>> getQos() const;

    /**
     * @brief Set quality of service
     */
    void setQos(uint32_t qos);

    /**
     * @brief Get cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session)
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<bool>> getCleanSession() const;

    /**
     * @brief Set cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session)
     */
    void setCleanSession(bool cleanSession);

    /**
     * @brief Get tupleBuffer flush interval in milliseconds
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<float>> getFlushIntervalMS() const;

    /**
     * @brief Set tupleBuffer flush interval in milliseconds
     */
    void setFlushIntervalMS(float flushIntervalMs);

    /**
     * @brief Get input data format
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getInputFormat() const;

    /**
     * @brief Set input data format
     */
    void setInputFormat(std::string inputFormat);

  private:
    /**
     * @brief constructor to create a new MQTT source config object initialized with values from sourceConfigMap
     */
    explicit MQTTSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new MQTT source config object initialized with values from sourceConfigMap
     */
    explicit MQTTSourceTypeConfig(ryml::NodeRef sourcTypeConfig);

    /**
     * @brief constructor to create a new MQTT source config object initialized with default values as set below
     */
    MQTTSourceTypeConfig();

    StringConfigOption url;
    StringConfigOption clientId;
    StringConfigOption userName;
    StringConfigOption topic;
    IntConfigOption qos;
    BoolConfigOption cleanSession;
    FloatConfigOption flushIntervalMS;
    StringConfigOption inputFormat;

};
}// namespace Configurations
}// namespace NES
#endif