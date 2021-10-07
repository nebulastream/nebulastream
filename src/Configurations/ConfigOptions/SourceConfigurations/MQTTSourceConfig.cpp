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
#include <Configurations/ConfigOptions/SourceConfigurations/MQTTSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {
MQTTSourceConfigPtr MQTTSourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<MQTTSourceConfig>(MQTTSourceConfig(std::move(sourceConfigMap)));
}

MQTTSourceConfigPtr MQTTSourceConfig::create() {
    return std::make_shared<MQTTSourceConfig>(MQTTSourceConfig());
}

MQTTSourceConfig::MQTTSourceConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceConfig(sourceConfigMap),
      url(ConfigOption<std::string>::create("url",
                                            "tcp://127.0.0.1:1885",
                                            "url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource")),
      clientId(ConfigOption<std::string>::create("clientId",
                                                 "testClient",
                                                 "clientId, needed for: MQTTSource (needs to be unique for each connected "
                                                 "MQTTSource), KafkaSource (use this for groupId)")),
      userName(ConfigOption<std::string>::create("userName",
                                                 "testUser",
                                                 "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(ConfigOption<std::string>::create("topic",
                                              "demoTownSensorData",
                                              "topic to listen to, needed for: MQTTSource, KafkaSource")),
      qos(ConfigOption<uint32_t>::create("qos", 2, "quality of service, needed for: MQTTSource")),
      cleanSession(
          ConfigOption<bool>::create("cleanSession",
                                     true,
                                     "cleanSession true = clean up session after client loses connection, false = keep data for "
                                     "client after connection loss (persistent session), needed for: MQTTSource")) {
    NES_INFO("NesSourceConfig: Init source config object with new values.");

    if (sourceConfigMap.find("MQTTSourceUrl") != sourceConfigMap.end()) {
        url->setValue(sourceConfigMap.find("MQTTSourceUrl")->second);
    }
    if (sourceConfigMap.find("MQTTSourceClientId") != sourceConfigMap.end()) {
        clientId->setValue(sourceConfigMap.find("MQTTSourceClientId")->second);
    }
    if (sourceConfigMap.find("MQTTSourceUserName") != sourceConfigMap.end()) {
        userName->setValue(sourceConfigMap.find("MQTTSourceUserName")->second);
    }
    if (sourceConfigMap.find("MQTTSourceTopic") != sourceConfigMap.end()) {
        topic->setValue(sourceConfigMap.find("MQTTSourceTopic")->second);
    }
    if (sourceConfigMap.find("MQTTSourceQos") != sourceConfigMap.end()) {
        qos->setValue(std::stoi(sourceConfigMap.find("MQTTSourceQos")->second));
    }
    if (sourceConfigMap.find("MQTTSourceCleanSession") != sourceConfigMap.end()) {
        cleanSession->setValue((sourceConfigMap.find("MQTTSourceCleanSession")->second == "true"));
    }

}

MQTTSourceConfig::MQTTSourceConfig()
    : SourceConfig(),
      url(ConfigOption<std::string>::create("url",
                                            "ws://127.0.0.1:9001",
                                            "url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource")),
      clientId(ConfigOption<std::string>::create("clientId",
                                                 "testClient",
                                                 "clientId, needed for: MQTTSource (needs to be unique for each connected "
                                                 "MQTTSource), KafkaSource (use this for groupId)")),
      userName(ConfigOption<std::string>::create("userName",
                                                 "testUser",
                                                 "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(ConfigOption<std::string>::create("topic",
                                              "demoTownSensorData",
                                              "topic to listen to, needed for: MQTTSource, KafkaSource")),
      qos(ConfigOption<uint32_t>::create("qos", 2, "quality of service, needed for: MQTTSource")),
      cleanSession(
          ConfigOption<bool>::create("cleanSession",
                                     true,
                                     "cleanSession true = clean up session after client loses connection, false = keep data for "
                                     "client after connection loss (persistent session), needed for: MQTTSource")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

void MQTTSourceConfig::resetSourceOptions() {
    setUrl(url->getDefaultValue());
    setClientId(clientId->getDefaultValue());
    setUserName(userName->getDefaultValue());
    setTopic(topic->getDefaultValue());
    setQos(qos->getDefaultValue());
    setCleanSession(cleanSession->getDefaultValue());
    SourceConfig::resetSourceOptions();
}

std::string MQTTSourceConfig::toString() {
    std::stringstream ss;
    ss << clientId->toStringNameCurrentValue();
    ss << userName->toStringNameCurrentValue();
    ss << topic->toStringNameCurrentValue();
    ss << qos->toStringNameCurrentValue();
    ss << cleanSession->toStringNameCurrentValue();
    ss << SourceConfig::toString();
    return ss.str();
}

StringConfigOption MQTTSourceConfig::getUrl() const { return url; }

StringConfigOption MQTTSourceConfig::getClientId() const { return clientId; }

StringConfigOption MQTTSourceConfig::getUserName() const { return userName; }

StringConfigOption MQTTSourceConfig::getTopic() const { return topic; }

IntConfigOption MQTTSourceConfig::getQos() const { return qos; }

BoolConfigOption MQTTSourceConfig::getCleanSession() const { return cleanSession; }

void MQTTSourceConfig::setUrl(std::string urlValue) { url->setValue(std::move(urlValue)); }

void MQTTSourceConfig::setClientId(std::string clientIdValue) { clientId->setValue(std::move(clientIdValue)); }

void MQTTSourceConfig::setUserName(std::string userNameValue) { userName->setValue(std::move(userNameValue)); }

void MQTTSourceConfig::setTopic(std::string topicValue) { topic->setValue(std::move(topicValue)); }

void MQTTSourceConfig::setQos(uint32_t qosValue) { qos->setValue(qosValue); }

void MQTTSourceConfig::setCleanSession(bool cleanSessionValue) { cleanSession->setValue(cleanSessionValue); }

}// namespace NES