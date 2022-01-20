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

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/rapidyaml.hpp>
#include <map>
#include <string>

namespace NES {

class MQTTSourceType;
using MQTTSourceTypePtr = std::shared_ptr<MQTTSourceType>;

/**
 * @brief Configuration object for MQTT source config
 * Connect to an MQTT broker and read data from there
 */
class MQTTSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a MQTTSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return MQTTSourceConfigPtr
     */
    static MQTTSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a MQTTSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return MQTTSourceConfigPtr
     */
    static MQTTSourceTypePtr create(ryml::NodeRef ymlConfig);

    /**
     * @brief create a MQTTSourceConfigPtr object with default values
     * @return MQTTSourceConfigPtr
     */
    static MQTTSourceTypePtr create();

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

    /**
     * @brief Get url to connect
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getUrl() const;

    /**
     * @brief Set url to connect to
     */
    void setUrl(std::string url);

    /**
     * @brief Get clientId
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getClientId() const;

    /**
     * @brief Set clientId
     */
    void setClientId(std::string clientId);

    /**
     * @brief Get userName
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getUserName() const;

    /**
     * @brief Set userName
     */
    void setUserName(std::string userName);

    /**
     * @brief Get topic to listen to
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getTopic() const;

    /**
     * @brief Set topic to listen to
     */
    void setTopic(std::string topic);

    /**
     * @brief Get quality of service
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getQos() const;

    /**
     * @brief Set quality of service
     */
    void setQos(uint32_t qos);

    /**
     * @brief Get cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session)
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<bool>> getCleanSession() const;

    /**
     * @brief Set cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session)
     */
    void setCleanSession(bool cleanSession);

    /**
     * @brief Get tupleBuffer flush interval in milliseconds
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<float>> getFlushIntervalMS() const;

    /**
     * @brief Set tupleBuffer flush interval in milliseconds
     */
    void setFlushIntervalMS(float flushIntervalMs);

    /**
     * @brief Get input data format
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getInputFormat() const;

    /**
     * @brief Set input data format
     */
    void setInputFormat(std::string inputFormat);

  private:
    /**
     * @brief constructor to create a new MQTT source config object initialized with values from sourceConfigMap
     */
    explicit MQTTSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new MQTT source config object initialized with values from sourceConfigMap
     */
    explicit MQTTSourceType(ryml::NodeRef yamlConfig);

    /**
     * @brief constructor to create a new MQTT source config object initialized with default values as set below
     */
    MQTTSourceType();

    Configurations::StringConfigOption url;
    Configurations::StringConfigOption clientId;
    Configurations::StringConfigOption userName;
    Configurations::StringConfigOption topic;
    Configurations::IntConfigOption qos;
    Configurations::BoolConfigOption cleanSession;
    Configurations::FloatConfigOption flushIntervalMS;
    Configurations::StringConfigOption inputFormat;
};
}// namespace NES
#endif