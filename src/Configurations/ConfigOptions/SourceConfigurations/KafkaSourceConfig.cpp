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
#include <Configurations/ConfigOptions/SourceConfigurations/KafkaSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {
KafkaSourceConfigPtr KafkaSourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<KafkaSourceConfig>(KafkaSourceConfig(std::move(sourceConfigMap)));
}

KafkaSourceConfigPtr KafkaSourceConfig::create() {
    return std::make_shared<KafkaSourceConfig>(KafkaSourceConfig());
}

KafkaSourceConfig::KafkaSourceConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceConfig(std::move(sourceConfigMap)), brokers(ConfigOption<std::string>::create("brokers", "", "brokers")),
      autoCommit(ConfigOption<uint32_t>::create(
          "autoCommit",
          1,
          "auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource")),
      groupId(ConfigOption<std::string>::create("groupId",
                                                "testGroup",
                                                "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(ConfigOption<std::string>::create("topic", "testTopic", "topic to listen to")),
      connectionTimeout(
          ConfigOption<uint32_t>::create("connectionTimeout", 10, "connection time out for source, needed for: KafkaSource")) {
    NES_INFO("KafkaSourceConfig: Init source config object with values from sourceConfigMap.");

    if (sourceConfigMap.find("KafkaSourceBrokers") != sourceConfigMap.end()) {
        brokers->setValue(sourceConfigMap.find("KafkaSourceBrokers")->second);
    }
    if (sourceConfigMap.find("KafkaSourceAutoCommit") != sourceConfigMap.end()) {
        autoCommit->setValue(std::stoi(sourceConfigMap.find("KafkaSourceAutoCommit")->second));
    }
    if (sourceConfigMap.find("KafkaSourceGroupId") != sourceConfigMap.end()) {
        groupId->setValue(sourceConfigMap.find("KafkaSourceGroupId")->second);
    }
    if (sourceConfigMap.find("KafkaSourceTopic") != sourceConfigMap.end()) {
        topic->setValue(sourceConfigMap.find("KafkaSourceTopic")->second);
    }
    if (sourceConfigMap.find("KafkaSourceConnectionTimeout") != sourceConfigMap.end()) {
        connectionTimeout->setValue(std::stoi(sourceConfigMap.find("KafkaSourceConnectionTimeout")->second));
    }

}

KafkaSourceConfig::KafkaSourceConfig()
    : SourceConfig(), brokers(ConfigOption<std::string>::create("brokers", "", "brokers")),
      autoCommit(ConfigOption<uint32_t>::create(
          "autoCommit",
          1,
          "auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource")),
      groupId(ConfigOption<std::string>::create("groupId",
                                                "testGroup",
                                                "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(ConfigOption<std::string>::create("topic", "testTopic", "topic to listen to")),
      connectionTimeout(
          ConfigOption<uint32_t>::create("connectionTimeout", 10, "connection time out for source, needed for: KafkaSource")) {
    NES_INFO("KafkaSourceConfig: Init source config object with default values.");

}

void KafkaSourceConfig::resetSourceOptions() {
    setBrokers(brokers->getDefaultValue());
    setAutoCommit(autoCommit->getDefaultValue());
    setGroupId(groupId->getDefaultValue());
    setTopic(topic->getDefaultValue());
    setConnectionTimeout(connectionTimeout->getDefaultValue());
    SourceConfig::resetSourceOptions();
}

std::string KafkaSourceConfig::toString() {
    std::stringstream ss;
    ss << brokers->toStringNameCurrentValue();
    ss << autoCommit->toStringNameCurrentValue();
    ss << groupId->toStringNameCurrentValue();
    ss << topic->toStringNameCurrentValue();
    ss << connectionTimeout->toStringNameCurrentValue();
    ss << SourceConfig::toString();
    return ss.str();
}

StringConfigOption KafkaSourceConfig::getBrokers() const {return brokers;}

IntConfigOption KafkaSourceConfig::getAutoCommit() const { return autoCommit; }

StringConfigOption KafkaSourceConfig::getGroupId() const { return groupId; }

StringConfigOption KafkaSourceConfig::getTopic() const { return topic; }

IntConfigOption KafkaSourceConfig::getConnectionTimeout() const { return connectionTimeout; }

void KafkaSourceConfig::setBrokers(std::string brokersValue) { brokers->setValue(std::move(brokersValue)); }

void KafkaSourceConfig::setAutoCommit(uint32_t autoCommitValue) { autoCommit->setValue(autoCommitValue); }

void KafkaSourceConfig::setGroupId(std::string groupIdValue) { groupId->setValue(std::move(groupIdValue)); }

void KafkaSourceConfig::setTopic(std::string topicValue) { topic->setValue(std::move(topicValue)); }

void KafkaSourceConfig::setConnectionTimeout(uint32_t connectionTimeoutValue) { connectionTimeout->setValue(connectionTimeoutValue); }

}// namespace NES