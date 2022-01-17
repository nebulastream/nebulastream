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

#ifndef NES_KAFKASOURCETYPECONFIG_HPP
#define NES_KAFKASOURCETYPECONFIG_HPP

#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfig.hpp>
#include <map>
#include <string>

namespace NES {

namespace Configurations {

class KafkaSourceTypeConfig;
using KafkaSourceTypeConfigPtr = std::shared_ptr<KafkaSourceTypeConfig>;

/**
 * @brief Configuration object for Kafka source config
 * Connect to a kafka broker and read data form there
 */
class KafkaSourceTypeConfig : public SourceTypeConfig {

  public:
    /**
     * @brief create a KafkaSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return KafkaSourceConfigPtr
     */
    static KafkaSourceTypeConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a KafkaSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return KafkaSourceConfigPtr
     */
    static KafkaSourceTypeConfigPtr create(ryml::NodeRef sourcTypeConfig);

    /**
     * @brief create a KafkaSourceConfigPtr object
     * @return KafkaSourceConfigPtr
     */
    static KafkaSourceTypeConfigPtr create();

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

    /**
     * @brief Get broker string
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getBrokers() const;

    /**
     * @brief Set broker string
     */
    void setBrokers(std::string brokers);

    /**
     * @brief Get auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<uint32_t>> getAutoCommit() const;

    /**
     * @brief Set auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    void setAutoCommit(uint32_t autoCommit);

    /**
     * @brief get groupId
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getGroupId() const;

    /**
      * @brief set groupId
      */
    void setGroupId(std::string groupId);

    /**
     * @brief Get topic to listen to
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getTopic() const;

    /**
     * @brief Set topic to listen to
     */
    void setTopic(std::string topic);

    /**
     * @brief Get connection time out for source, needed for: KafkaSource
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<uint32_t>> getConnectionTimeout() const;

    /**
     * @brief Set connection time out for source, needed for: KafkaSource
     */
    void setConnectionTimeout(uint32_t connectionTimeout);

  private:
    /**
     * @brief constructor to create a new Kafka source config object initialized with values from sourceConfigMap
     */
    explicit KafkaSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Kafka source config object initialized with values from sourceConfigMap
     */
    explicit KafkaSourceTypeConfig(ryml::NodeRef sourcTypeConfig);

    /**
     * @brief constructor to create a new Kafka source config object initialized with default values
     */
    KafkaSourceTypeConfig();

    StringConfigOption brokers;
    IntConfigOption autoCommit;
    StringConfigOption groupId;
    StringConfigOption topic;
    IntConfigOption connectionTimeout;
};
}// namespace Configurations
}// namespace NES
#endif