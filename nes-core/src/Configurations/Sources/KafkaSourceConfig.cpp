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

#include <Configurations/ConfigOption.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/KafkaSourceTypeConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

KafkaSourceTypeConfigPtr KafkaSourceTypeConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<KafkaSourceTypeConfig>(KafkaSourceTypeConfig(std::move(sourceConfigMap)));
}

KafkaSourceTypeConfigPtr KafkaSourceTypeConfig::create() { return std::make_shared<KafkaSourceTypeConfig>(KafkaSourceTypeConfig()); }

KafkaSourceTypeConfig::KafkaSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceTypeConfig(sourceConfigMap, KAFKA_SOURCE_CONFIG),
      brokers(ConfigOption<std::string>::create(BROKERS_CONFIG, "", "brokers")),
      autoCommit(ConfigOption<uint32_t>::create(
          AUTO_COMMIT_CONFIG,
          1,
          "auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource")),
      groupId(ConfigOption<std::string>::create(GROUP_ID_CONFIG,
                                                "",
                                                "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(ConfigOption<std::string>::create(TOPIC_CONFIG, "", "topic to listen to")),
      connectionTimeout(ConfigOption<uint32_t>::create(CONNECTION_TIMEOUT_CONFIG,
                                                       10,
                                                       "connection time out for source, needed for: KafkaSource")) {
    NES_INFO("KafkaSourceConfig: Init source config object with values from sourceConfigMap.");

    /*if (sourceConfigMap.find(KAFKA_SOURCE_BROKERS_CONFIG) != sourceConfigMap.end()) {
        brokers->setValue(sourceConfigMap.find(KAFKA_SOURCE_BROKERS_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no brokers defined! Please define brokers.");
    }
    if (sourceConfigMap.find(KAFKA_SOURCE_AUTO_COMMIT_CONFIG) != sourceConfigMap.end()) {
        autoCommit->setValue(std::stoi(sourceConfigMap.find(KAFKA_SOURCE_AUTO_COMMIT_CONFIG)->second));
    }
    if (sourceConfigMap.find(KAFKA_SOURCE_GROUP_ID_CONFIG) != sourceConfigMap.end()) {
        groupId->setValue(sourceConfigMap.find(KAFKA_SOURCE_GROUP_ID_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no groupId defined! Please define groupId.");
    }
    if (sourceConfigMap.find(KAFKA_SOURCE_TOPIC_CONFIG) != sourceConfigMap.end()) {
        topic->setValue(sourceConfigMap.find(KAFKA_SOURCE_TOPIC_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no topic defined! Please define topic.");
    }
    if (sourceConfigMap.find(KAFKA_SOURCE_CONNECTION_TIMEOUT_CONFIG) != sourceConfigMap.end()) {
        connectionTimeout->setValue(std::stoi(sourceConfigMap.find(KAFKA_SOURCE_CONNECTION_TIMEOUT_CONFIG)->second));
    }*/
}

KafkaSourceTypeConfig::KafkaSourceTypeConfig()
    : SourceTypeConfig(KAFKA_SOURCE_CONFIG), brokers(ConfigOption<std::string>::create(BROKERS_CONFIG, "", "brokers")),
      autoCommit(ConfigOption<uint32_t>::create(
          AUTO_COMMIT_CONFIG,
          1,
          "auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource")),
      groupId(ConfigOption<std::string>::create(GROUP_ID_CONFIG,
                                                "testGroup",
                                                "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(ConfigOption<std::string>::create(TOPIC_CONFIG, "testTopic", "topic to listen to")),
      connectionTimeout(ConfigOption<uint32_t>::create(CONNECTION_TIMEOUT_CONFIG,
                                                       10,
                                                       "connection time out for source, needed for: KafkaSource")) {
    NES_INFO("KafkaSourceConfig: Init source config object with default values.");
}

void KafkaSourceTypeConfig::resetSourceOptions() {
    setBrokers(brokers->getDefaultValue());
    setAutoCommit(autoCommit->getDefaultValue());
    setGroupId(groupId->getDefaultValue());
    setTopic(topic->getDefaultValue());
    setConnectionTimeout(connectionTimeout->getDefaultValue());
    SourceTypeConfig::resetSourceOptions(KAFKA_SOURCE_CONFIG);
}

std::string KafkaSourceTypeConfig::toString() {
    std::stringstream ss;
    ss << brokers->toStringNameCurrentValue();
    ss << autoCommit->toStringNameCurrentValue();
    ss << groupId->toStringNameCurrentValue();
    ss << topic->toStringNameCurrentValue();
    ss << connectionTimeout->toStringNameCurrentValue();
    ss << SourceTypeConfig::toString();
    return ss.str();
}

bool KafkaSourceTypeConfig::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<KafkaSourceTypeConfig>()) {
        return false;
    }
    auto otherSourceConfig = other->as<KafkaSourceTypeConfig>();
    return SourceTypeConfig::equal(other) && brokers->getValue() == otherSourceConfig->brokers->getValue()
        && autoCommit->getValue() == otherSourceConfig->autoCommit->getValue()
        && groupId->getValue() == otherSourceConfig->groupId->getValue()
        && topic->getValue() == otherSourceConfig->topic->getValue()
        && connectionTimeout->getValue() == otherSourceConfig->connectionTimeout->getValue();
}

StringConfigOption KafkaSourceTypeConfig::getBrokers() const { return brokers; }

IntConfigOption KafkaSourceTypeConfig::getAutoCommit() const { return autoCommit; }

StringConfigOption KafkaSourceTypeConfig::getGroupId() const { return groupId; }

StringConfigOption KafkaSourceTypeConfig::getTopic() const { return topic; }

IntConfigOption KafkaSourceTypeConfig::getConnectionTimeout() const { return connectionTimeout; }

void KafkaSourceTypeConfig::setBrokers(std::string brokersValue) { brokers->setValue(std::move(brokersValue)); }

void KafkaSourceTypeConfig::setAutoCommit(uint32_t autoCommitValue) { autoCommit->setValue(autoCommitValue); }

void KafkaSourceTypeConfig::setGroupId(std::string groupIdValue) { groupId->setValue(std::move(groupIdValue)); }

void KafkaSourceTypeConfig::setTopic(std::string topicValue) { topic->setValue(std::move(topicValue)); }

void KafkaSourceTypeConfig::setConnectionTimeout(uint32_t connectionTimeoutValue) {
    connectionTimeout->setValue(connectionTimeoutValue);
}
}// namespace Configurations
}// namespace NES