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

#ifndef NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_KAFKASOURCETYPE_HPP_
#define NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_KAFKASOURCETYPE_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <string>

namespace NES {

class KafkaSourceType;
using KafkaSourceTypePtr = std::shared_ptr<KafkaSourceType>;

/**
 * @brief Configuration object for Kafka source config
 * Connect to a kafka broker and read data form there
 */
class KafkaSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a KafkaSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return KafkaSourceConfigPtr
     */
    static KafkaSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a KafkaSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return KafkaSourceConfigPtr
     */
    static KafkaSourceTypePtr create(Yaml::Node yamlConfig);

    /**
     * @brief create a KafkaSourceConfigPtr object
     * @return KafkaSourceConfigPtr
     */
    static KafkaSourceTypePtr create();

    std::string toString() override;

    bool equal(PhysicalSourceTypePtr const& other) override;

    void reset() override;

    /**
     * @brief Get broker string
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getBrokers() const;

    /**
     * @brief Set broker string
     */
    void setBrokers(std::string brokers);

    /**
     * @brief Get auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getAutoCommit() const;

    /**
     * @brief Set auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    void setAutoCommit(uint32_t autoCommit);

    /**
     * @brief get groupId
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getGroupId() const;

    /**
      * @brief set groupId
      */
    void setGroupId(std::string groupId);

    /**
     * @brief Get topic to listen to
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getTopic() const;

    /**
     * @brief Set topic to listen to
     */
    void setTopic(std::string topic);

    /**
     * @brief Get connection time out for source, needed for: KafkaSource
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getConnectionTimeout() const;

    /**
     * @brief Set connection time out for source, needed for: KafkaSource
     */
    void setConnectionTimeout(uint32_t connectionTimeout);

  private:
    /**
     * @brief constructor to create a new Kafka source config object initialized with values from sourceConfigMap
     */
    explicit KafkaSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new Kafka source config object initialized with values from sourceConfigMap
     */
    explicit KafkaSourceType(Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new Kafka source config object initialized with default values
     */
    KafkaSourceType();

    Configurations::StringConfigOption brokers;
    Configurations::IntConfigOption autoCommit;
    Configurations::StringConfigOption groupId;
    Configurations::StringConfigOption topic;
    Configurations::IntConfigOption connectionTimeout;
};
}// namespace NES
#endif  // NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_KAFKASOURCETYPE_HPP_
