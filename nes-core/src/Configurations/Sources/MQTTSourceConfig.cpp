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
#include <Configurations/Worker/PhysicalStreamConfig/MQTTSourceTypeConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

MQTTSourceTypeConfigPtr MQTTSourceTypeConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<MQTTSourceTypeConfig>(MQTTSourceTypeConfig(std::move(sourceConfigMap)));
}

MQTTSourceTypeConfigPtr MQTTSourceTypeConfig::create() { return std::make_shared<MQTTSourceTypeConfig>(MQTTSourceTypeConfig()); }

MQTTSourceTypeConfig::MQTTSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceTypeConfig(sourceConfigMap, MQTT_SOURCE_CONFIG),
      url(ConfigOption<std::string>::create(URL_CONFIG,
                                            "",
                                            "url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource")),
      clientId(ConfigOption<std::string>::create(CLIENT_ID_CONFIG,
                                                 "",
                                                 "clientId, needed for: MQTTSource (needs to be unique for each connected "
                                                 "MQTTSource), KafkaSource (use this for groupId)")),
      userName(ConfigOption<std::string>::create(USER_NAME_CONFIG,
                                                 "",
                                                 "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(ConfigOption<std::string>::create(TOPIC_CONFIG, "", "topic to listen to, needed for: MQTTSource, KafkaSource")),
      qos(ConfigOption<uint32_t>::create(QOS_CONFIG, 2, "quality of service, needed for: MQTTSource")),
      cleanSession(
          ConfigOption<bool>::create(CLEAN_SESSION_CONFIG,
                                     true,
                                     "cleanSession true = clean up session after client loses connection, false = keep data for "
                                     "client after connection loss (persistent session), needed for: MQTTSource")),
      flushIntervalMS(ConfigOption<float>::create("flushIntervalMS", -1, "tupleBuffer flush interval in milliseconds")),
      inputFormat(ConfigOption<std::string>::create(INPUT_FORMAT_CONFIG, "JSON", "input data format")) {
    NES_INFO("NesSourceConfig: Init source config object with new values.");

    /*if (sourceConfigMap.find(MQTT_SOURCE_URL_CONFIG) != sourceConfigMap.end()) {
        url->setValue(sourceConfigMap.find(MQTT_SOURCE_URL_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no Url defined! Please define a Url.");
    }
    if (sourceConfigMap.find(MQTT_SOURCE_CLIENT_ID_CONFIG) != sourceConfigMap.end()) {
        clientId->setValue(sourceConfigMap.find(MQTT_SOURCE_CLIENT_ID_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no ClientId defined! Please define a ClientId.");
    }
    if (sourceConfigMap.find(MQTT_SOURCE_USER_NAME_CONFIG) != sourceConfigMap.end()) {
        userName->setValue(sourceConfigMap.find(MQTT_SOURCE_USER_NAME_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no UserName defined! Please define a UserName.");
    }
    if (sourceConfigMap.find(MQTT_SOURCE_TOPIC_CONFIG) != sourceConfigMap.end()) {
        topic->setValue(sourceConfigMap.find(MQTT_SOURCE_TOPIC_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no topic defined! Please define a topic.");
    }
    if (sourceConfigMap.find(MQTT_SOURCE_QOS_CONFIG) != sourceConfigMap.end()) {
        qos->setValue(std::stoi(sourceConfigMap.find(MQTT_SOURCE_QOS_CONFIG)->second));
    }
    if (sourceConfigMap.find(MQTT_SOURCE_CLEAN_SESSION_CONFIG) != sourceConfigMap.end()) {
        cleanSession->setValue((sourceConfigMap.find(MQTT_SOURCE_CLEAN_SESSION_CONFIG)->second == "true"));
    }
    if (sourceConfigMap.find(MQTT_SOURCE_FLUSH_INTERVAL_MS_CONFIG) != sourceConfigMap.end()) {
        flushIntervalMS->setValue(std::stof(sourceConfigMap.find(MQTT_SOURCE_FLUSH_INTERVAL_MS_CONFIG)->second));
    }*/
}

MQTTSourceTypeConfig::MQTTSourceTypeConfig()
    : SourceTypeConfig(MQTT_SOURCE_CONFIG),
      url(ConfigOption<std::string>::create(URL_CONFIG,
                                            "ws://127.0.0.1:9001",
                                            "url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource")),
      clientId(ConfigOption<std::string>::create(CLIENT_ID_CONFIG,
                                                 "testClient",
                                                 "clientId, needed for: MQTTSource (needs to be unique for each connected "
                                                 "MQTTSource), KafkaSource (use this for groupId)")),
      userName(ConfigOption<std::string>::create(USER_NAME_CONFIG,
                                                 "testUser",
                                                 "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(ConfigOption<std::string>::create(TOPIC_CONFIG,
                                              "demoTownSensorData",
                                              "topic to listen to, needed for: MQTTSource, KafkaSource")),
      qos(ConfigOption<uint32_t>::create(QOS_CONFIG, 2, "quality of service, needed for: MQTTSource")),
      cleanSession(
          ConfigOption<bool>::create(CLEAN_SESSION_CONFIG,
                                     true,
                                     "cleanSession true = clean up session after client loses connection, false = keep data for "
                                     "client after connection loss (persistent session), needed for: MQTTSource")),
      flushIntervalMS(ConfigOption<float>::create("flushIntervalMS", -1, "tupleBuffer flush interval in milliseconds")),
      inputFormat(ConfigOption<std::string>::create(INPUT_FORMAT_CONFIG, "JSON", "input data format")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

void MQTTSourceTypeConfig::resetSourceOptions() {
    setUrl(url->getDefaultValue());
    setClientId(clientId->getDefaultValue());
    setUserName(userName->getDefaultValue());
    setTopic(topic->getDefaultValue());
    setQos(qos->getDefaultValue());
    setCleanSession(cleanSession->getDefaultValue());
    setFlushIntervalMS(flushIntervalMS->getDefaultValue());
    setInputFormat(inputFormat->getDefaultValue());
    SourceTypeConfig::resetSourceOptions(MQTT_SOURCE_CONFIG);
}

std::string MQTTSourceTypeConfig::toString() {
    std::stringstream ss;
    ss << clientId->toStringNameCurrentValue();
    ss << userName->toStringNameCurrentValue();
    ss << topic->toStringNameCurrentValue();
    ss << qos->toStringNameCurrentValue();
    ss << cleanSession->toStringNameCurrentValue();
    ss << flushIntervalMS->toStringNameCurrentValue();
    ss << inputFormat->toStringNameCurrentValue();
    ss << SourceTypeConfig::toString();
    return ss.str();
}

bool MQTTSourceTypeConfig::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<MQTTSourceTypeConfig>()) {
        return false;
    }
    auto otherSourceConfig = other->as<MQTTSourceTypeConfig>();
    return clientId->getValue() == otherSourceConfig->clientId->getValue()
        && userName->getValue() == otherSourceConfig->userName->getValue()
        && topic->getValue() == otherSourceConfig->topic->getValue() && qos->getValue() == otherSourceConfig->qos->getValue()
        && cleanSession->getValue() == otherSourceConfig->cleanSession->getValue()
        && flushIntervalMS->getValue() == otherSourceConfig->flushIntervalMS->getValue()
        && inputFormat->getValue() == otherSourceConfig->inputFormat->getValue() && SourceTypeConfig::equal(other);
}

StringConfigOption MQTTSourceTypeConfig::getUrl() const { return url; }

StringConfigOption MQTTSourceTypeConfig::getClientId() const { return clientId; }

StringConfigOption MQTTSourceTypeConfig::getUserName() const { return userName; }

StringConfigOption MQTTSourceTypeConfig::getTopic() const { return topic; }

IntConfigOption MQTTSourceTypeConfig::getQos() const { return qos; }

BoolConfigOption MQTTSourceTypeConfig::getCleanSession() const { return cleanSession; }

FloatConfigOption MQTTSourceTypeConfig::getFlushIntervalMS() const { return flushIntervalMS; }

StringConfigOption MQTTSourceTypeConfig::getInputFormat() const { return inputFormat; }

void MQTTSourceTypeConfig::setUrl(std::string urlValue) { url->setValue(std::move(urlValue)); }

void MQTTSourceTypeConfig::setClientId(std::string clientIdValue) { clientId->setValue(std::move(clientIdValue)); }

void MQTTSourceTypeConfig::setUserName(std::string userNameValue) { userName->setValue(std::move(userNameValue)); }

void MQTTSourceTypeConfig::setTopic(std::string topicValue) { topic->setValue(std::move(topicValue)); }

void MQTTSourceTypeConfig::setQos(uint32_t qosValue) { qos->setValue(qosValue); }

void MQTTSourceTypeConfig::setCleanSession(bool cleanSessionValue) { cleanSession->setValue(cleanSessionValue); }

void MQTTSourceTypeConfig::setFlushIntervalMS(float flushIntervalMs) { flushIntervalMS->setValue(flushIntervalMs); }

void MQTTSourceTypeConfig::setInputFormat(std::string inputFormatValue) { inputFormat->setValue(std::move(inputFormatValue)); }

}// namespace Configurations
}// namespace NES