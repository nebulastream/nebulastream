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

#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

KafkaSourceTypePtr KafkaSourceType::create(ryml::NodeRef yamlConfig) {
    return std::make_shared<KafkaSourceType>(KafkaSourceType(std::move(yamlConfig)));
}

KafkaSourceTypePtr KafkaSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<KafkaSourceType>(KafkaSourceType(std::move(sourceConfigMap)));
}

KafkaSourceTypePtr KafkaSourceType::create() { return std::make_shared<KafkaSourceType>(KafkaSourceType()); }

KafkaSourceType::KafkaSourceType(std::map<std::string, std::string> sourceConfigMap) : KafkaSourceType() {
    NES_INFO("KafkaSourceType: Init default CSV source config object with values from command line args.");

    if (sourceConfigMap.find(Configurations::BROKERS_CONFIG) != sourceConfigMap.end()) {
        brokers->setValue(sourceConfigMap.find(Configurations::BROKERS_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no brokers defined! Please define brokers.");
    }
    if (sourceConfigMap.find(Configurations::AUTO_COMMIT_CONFIG) != sourceConfigMap.end()) {
        autoCommit->setValue(std::stoi(sourceConfigMap.find(Configurations::AUTO_COMMIT_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::GROUP_ID_CONFIG) != sourceConfigMap.end()) {
        groupId->setValue(sourceConfigMap.find(Configurations::GROUP_ID_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no groupId defined! Please define groupId.");
    }
    if (sourceConfigMap.find(Configurations::TOPIC_CONFIG) != sourceConfigMap.end()) {
        topic->setValue(sourceConfigMap.find(Configurations::TOPIC_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceConfig:: no topic defined! Please define topic.");
    }
    if (sourceConfigMap.find(Configurations::CONNECTION_TIMEOUT_CONFIG) != sourceConfigMap.end()) {
        connectionTimeout->setValue(std::stoi(sourceConfigMap.find(Configurations::CONNECTION_TIMEOUT_CONFIG)->second));
    }
}

KafkaSourceType::KafkaSourceType(ryml::NodeRef yamlConfig) : KafkaSourceType() {
    NES_INFO("KafkaSourceType: Init default CSV source config object with values from YAML file.");

    if (yamlConfig.find_child(ryml::to_csubstr(Configurations::BROKERS_CONFIG)).has_val()) {
        brokers->setValue(yamlConfig.find_child(ryml::to_csubstr(Configurations::BROKERS_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceType:: no brokers defined! Please define brokers.");
    }
    if (yamlConfig.find_child(ryml::to_csubstr(Configurations::AUTO_COMMIT_CONFIG)).has_val()) {
        autoCommit->setValue(std::stoi(yamlConfig.find_child(ryml::to_csubstr(Configurations::AUTO_COMMIT_CONFIG)).val().str));
    }
    if (yamlConfig.find_child(ryml::to_csubstr(Configurations::GROUP_ID_CONFIG)).has_val()) {
        groupId->setValue(yamlConfig.find_child(ryml::to_csubstr(Configurations::GROUP_ID_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceType:: no groupId defined! Please define groupId.");
    }
    if (yamlConfig.find_child(ryml::to_csubstr(Configurations::TOPIC_CONFIG)).has_val()) {
        topic->setValue(yamlConfig.find_child(ryml::to_csubstr(Configurations::TOPIC_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("KafkaSourceType:: no topic defined! Please define topic.");
    }
    if (yamlConfig.find_child(ryml::to_csubstr(Configurations::CONNECTION_TIMEOUT_CONFIG)).has_val()) {
        connectionTimeout->setValue(
            std::stoi(yamlConfig.find_child(ryml::to_csubstr(Configurations::CONNECTION_TIMEOUT_CONFIG)).val().str));
    }
}

KafkaSourceType::KafkaSourceType()
    : PhysicalSourceType(KAFKA_SOURCE),
      brokers(Configurations::ConfigurationOption<std::string>::create(Configurations::BROKERS_CONFIG, "", "brokers")),
      autoCommit(Configurations::ConfigurationOption<uint32_t>::create(
          Configurations::AUTO_COMMIT_CONFIG,
          1,
          "auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource")),
      groupId(Configurations::ConfigurationOption<std::string>::create(
          Configurations::GROUP_ID_CONFIG,
          "testGroup",
          "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(Configurations::ConfigurationOption<std::string>::create(Configurations::TOPIC_CONFIG,
                                                                     "testTopic",
                                                                     "topic to listen to")),
      connectionTimeout(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::CONNECTION_TIMEOUT_CONFIG,
                                                                10,
                                                                "connection time out for source, needed for: KafkaSource")) {
    NES_INFO("KafkaSourceType: Init source config object with default values.");
}

std::string KafkaSourceType::toString() {
    std::stringstream ss;
    ss << "KafkaSourceType =>  {\n";
    ss << Configurations::BROKERS_CONFIG + ":" + brokers->toStringNameCurrentValue();
    ss << Configurations::AUTO_COMMIT_CONFIG + ":" + autoCommit->toStringNameCurrentValue();
    ss << Configurations::GROUP_ID_CONFIG + ":" + groupId->toStringNameCurrentValue();
    ss << Configurations::TOPIC_CONFIG + ":" + topic->toStringNameCurrentValue();
    ss << Configurations::CONNECTION_TIMEOUT_CONFIG + ":" + connectionTimeout->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool KafkaSourceType::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<KafkaSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<KafkaSourceType>();
    return SourceTypeConfig::equal(other) && brokers->getValue() == otherSourceConfig->brokers->getValue()
        && autoCommit->getValue() == otherSourceConfig->autoCommit->getValue()
        && groupId->getValue() == otherSourceConfig->groupId->getValue()
        && topic->getValue() == otherSourceConfig->topic->getValue()
        && connectionTimeout->getValue() == otherSourceConfig->connectionTimeout->getValue();
}

Configurations::StringConfigOption KafkaSourceType::getBrokers() const { return brokers; }

Configurations::IntConfigOption KafkaSourceType::getAutoCommit() const { return autoCommit; }

Configurations::StringConfigOption KafkaSourceType::getGroupId() const { return groupId; }

Configurations::StringConfigOption KafkaSourceType::getTopic() const { return topic; }

Configurations::IntConfigOption KafkaSourceType::getConnectionTimeout() const { return connectionTimeout; }

void KafkaSourceType::setBrokers(std::string brokersValue) { brokers->setValue(std::move(brokersValue)); }

void KafkaSourceType::setAutoCommit(uint32_t autoCommitValue) { autoCommit->setValue(autoCommitValue); }

void KafkaSourceType::setGroupId(std::string groupIdValue) { groupId->setValue(std::move(groupIdValue)); }

void KafkaSourceType::setTopic(std::string topicValue) { topic->setValue(std::move(topicValue)); }

void KafkaSourceType::setConnectionTimeout(uint32_t connectionTimeoutValue) {
    connectionTimeout->setValue(connectionTimeoutValue);
}
}// namespace NES