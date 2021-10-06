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

#ifndef NES_KAFKASOURCECONFIG_HPP
#define NES_KAFKASOURCECONFIG_HPP

#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfig.hpp>
#include <map>
#include <string>

namespace NES {

class KafkaSourceConfig;
using KafkaSourceConfigPtr = std::shared_ptr<KafkaSourceConfig>;

template<class T>
class ConfigOption;
using FloatConfigOption = std::shared_ptr<ConfigOption<float>>;
using IntConfigOption = std::shared_ptr<ConfigOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigOption<bool>>;

/**
* @brief Configuration object for source config
*/

class KafkaSourceConfig : public SourceConfig {

  public:
    /**
     * @brief create a KafkaSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return KafkaSourceConfigPtr
     */
    static KafkaSourceConfigPtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a KafkaSourceConfigPtr object
     * @return KafkaSourceConfigPtr
     */
    static KafkaSourceConfigPtr create();

    /**
     * @brief resets alls Source configuration to default values
     */
    void resetSourceOptions() override;
    /**
     * @brief creates a string representation of the source
     * @return
     */
    std::string toString() override;

    /**
     * @brief Get broker string
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getBrokers() const;

    /**
     * @brief Set broker string
     */
    void setBrokers(std::string brokers);

    /**
     * @brief Get auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getAutoCommit() const;

    /**
     * @brief Set auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    void setAutoCommit(uint32_t autoCommit);

    /**
     * @brief get groupId
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getGroupId() const;

    /**
      * @brief set groupId
      */
    void setGroupId(std::string groupId);

    /**
     * @brief Get topic to listen to
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getTopic() const;

    /**
     * @brief Set topic to listen to
     */
    void setTopic(std::string topic);

    /**
     * @brief Get connection time out for source, needed for: KafkaSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getConnectionTimeout() const;

    /**
     * @brief Set connection time out for source, needed for: KafkaSource
     */
    void setConnectionTimeout(uint32_t connectionTimeout);

  private:
    /**
     * @brief constructor to create a new Kafka source config object initialized with values from sourceConfigMap
     */
    explicit KafkaSourceConfig(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Kafka source config object initialized with default values
     */
    KafkaSourceConfig();

    StringConfigOption brokers;
    IntConfigOption autoCommit;
    StringConfigOption groupId;
    StringConfigOption topic;
    IntConfigOption connectionTimeout;
};
}// namespace NES
#endif