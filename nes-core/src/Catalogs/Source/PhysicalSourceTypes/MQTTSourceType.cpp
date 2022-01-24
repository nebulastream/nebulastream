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

#include <Catalogs/Source/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

MQTTSourceTypePtr MQTTSourceType::create(ryml::NodeRef ymlConfig) {
    return std::make_shared<MQTTSourceType>(MQTTSourceType(std::move(ymlConfig)));
}

MQTTSourceTypePtr MQTTSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<MQTTSourceType>(MQTTSourceType(std::move(sourceConfigMap)));
}

MQTTSourceTypePtr MQTTSourceType::create() { return std::make_shared<MQTTSourceType>(MQTTSourceType()); }

MQTTSourceType::MQTTSourceType(std::map<std::string, std::string> sourceConfigMap) : MQTTSourceType() {
    NES_INFO("MQTTSourceConfig: Init default MQTT source config object with values from command line args.");

    if (sourceConfigMap.find(Configurations::URL_CONFIG) != sourceConfigMap.end()) {
        url->setValue(sourceConfigMap.find(Configurations::URL_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no Url defined! Please define a Url.");
    }
    if (sourceConfigMap.find(Configurations::CLIENT_ID_CONFIG) != sourceConfigMap.end()) {
        clientId->setValue(sourceConfigMap.find(Configurations::CLIENT_ID_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no ClientId defined! Please define a ClientId.");
    }
    if (sourceConfigMap.find(Configurations::USER_NAME_CONFIG) != sourceConfigMap.end()) {
        userName->setValue(sourceConfigMap.find(Configurations::USER_NAME_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no UserName defined! Please define a UserName.");
    }
    if (sourceConfigMap.find(Configurations::TOPIC_CONFIG) != sourceConfigMap.end()) {
        topic->setValue(sourceConfigMap.find(Configurations::TOPIC_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no topic defined! Please define a topic.");
    }
    if (sourceConfigMap.find(Configurations::QOS_CONFIG) != sourceConfigMap.end()) {
        qos->setValue(std::stoi(sourceConfigMap.find(Configurations::QOS_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::CLEAN_SESSION_CONFIG) != sourceConfigMap.end()) {
        cleanSession->setValue((sourceConfigMap.find(Configurations::CLEAN_SESSION_CONFIG)->second == "true"));
    }
    if (sourceConfigMap.find(Configurations::FLUSH_INTERVAL_MS_CONFIG) != sourceConfigMap.end()) {
        flushIntervalMS->setValue(std::stof(sourceConfigMap.find(Configurations::FLUSH_INTERVAL_MS_CONFIG)->second));
    }
}

MQTTSourceType::MQTTSourceType(ryml::NodeRef yamlConfig) : MQTTSourceType() {
    NES_INFO("MQTTSourceConfig: Init default MQTT source config object with values from YAML file.");

    if (yamlConfig.has_child(ryml::to_csubstr(Configurations::URL_CONFIG)) && yamlConfig.find_child(ryml::to_csubstr(Configurations::URL_CONFIG)).val() != nullptr) {
        url->setValue(yamlConfig.find_child(ryml::to_csubstr(Configurations::URL_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no Url defined! Please define a Url.");
    }
    if (yamlConfig.has_child(ryml::to_csubstr(Configurations::CLIENT_ID_CONFIG)) && yamlConfig.find_child(ryml::to_csubstr(Configurations::CLIENT_ID_CONFIG)).val() != nullptr) {
        clientId->setValue(yamlConfig.find_child(ryml::to_csubstr(Configurations::CLIENT_ID_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no ClientId defined! Please define a ClientId.");
    }
    if (yamlConfig.has_child(ryml::to_csubstr(Configurations::USER_NAME_CONFIG)) && yamlConfig.find_child(ryml::to_csubstr(Configurations::USER_NAME_CONFIG)).val() != nullptr) {
        userName->setValue(yamlConfig.find_child(ryml::to_csubstr(Configurations::USER_NAME_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no UserName defined! Please define a UserName.");
    }
    if (yamlConfig.has_child(ryml::to_csubstr(Configurations::TOPIC_CONFIG)) && yamlConfig.find_child(ryml::to_csubstr(Configurations::TOPIC_CONFIG)).val() != nullptr) {
        topic->setValue(yamlConfig.find_child(ryml::to_csubstr(Configurations::TOPIC_CONFIG)).val().str);
    } else {
        NES_THROW_RUNTIME_ERROR("MQTTSourceConfig:: no topic defined! Please define a topic.");
    }
    if (yamlConfig.has_child(ryml::to_csubstr(Configurations::QOS_CONFIG)) && yamlConfig.find_child(ryml::to_csubstr(Configurations::QOS_CONFIG)).val() != nullptr) {
        qos->setValue(std::stoi(yamlConfig.find_child(ryml::to_csubstr(Configurations::QOS_CONFIG)).val().str));
    }
    if (yamlConfig.has_child(ryml::to_csubstr(Configurations::CLEAN_SESSION_CONFIG)) && yamlConfig.find_child(ryml::to_csubstr(Configurations::CLEAN_SESSION_CONFIG)).val() != nullptr) {
        cleanSession->setValue(
            strcasecmp(yamlConfig.find_child(ryml::to_csubstr(Configurations::CLEAN_SESSION_CONFIG)).val().str, "true"));
    }
    if (yamlConfig.has_child(ryml::to_csubstr(Configurations::FLUSH_INTERVAL_MS_CONFIG)) && yamlConfig.find_child(ryml::to_csubstr(Configurations::FLUSH_INTERVAL_MS_CONFIG)).val() != nullptr) {
        flushIntervalMS->setValue(
            std::stof(yamlConfig.find_child(ryml::to_csubstr(Configurations::FLUSH_INTERVAL_MS_CONFIG)).val().str));
    }
    if (yamlConfig.has_child(ryml::to_csubstr(Configurations::INPUT_FORMAT_CONFIG)) &&  yamlConfig.find_child(ryml::to_csubstr(Configurations::INPUT_FORMAT_CONFIG)).val() != nullptr) {
        inputFormat->setValue(yamlConfig.find_child(ryml::to_csubstr(Configurations::INPUT_FORMAT_CONFIG)).val().str);
    }
}

MQTTSourceType::MQTTSourceType()
    : PhysicalSourceType(MQTT_SOURCE), url(Configurations::ConfigurationOption<std::string>::create(
                                           Configurations::URL_CONFIG,
                                           "ws://127.0.0.1:9001",
                                           "url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource")),
      clientId(Configurations::ConfigurationOption<std::string>::create(
          Configurations::CLIENT_ID_CONFIG,
          "testClient",
          "clientId, needed for: MQTTSource (needs to be unique for each connected "
          "MQTTSource), KafkaSource (use this for groupId)")),
      userName(Configurations::ConfigurationOption<std::string>::create(
          Configurations::USER_NAME_CONFIG,
          "testUser",
          "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      topic(Configurations::ConfigurationOption<std::string>::create(Configurations::TOPIC_CONFIG,
                                                                     "demoTownSensorData",
                                                                     "topic to listen to, needed for: MQTTSource, KafkaSource")),
      qos(Configurations::ConfigurationOption<uint32_t>::create(Configurations::QOS_CONFIG,
                                                                2,
                                                                "quality of service, needed for: MQTTSource")),
      cleanSession(Configurations::ConfigurationOption<bool>::create(
          Configurations::CLEAN_SESSION_CONFIG,
          true,
          "cleanSession true = clean up session after client loses connection, false = keep data for "
          "client after connection loss (persistent session), needed for: MQTTSource")),
      flushIntervalMS(Configurations::ConfigurationOption<float>::create("flushIntervalMS",
                                                                         -1,
                                                                         "tupleBuffer flush interval in milliseconds")),
      inputFormat(Configurations::ConfigurationOption<std::string>::create(Configurations::INPUT_FORMAT_CONFIG,
                                                                           "JSON",
                                                                           "input data format")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

std::string MQTTSourceType::toString() {
    std::stringstream ss;
    ss << "MQTTSourceType => {\n";
    ss << Configurations::CLIENT_ID_CONFIG + ":" + clientId->toStringNameCurrentValue();
    ss << Configurations::USER_NAME_CONFIG + ":" + userName->toStringNameCurrentValue();
    ss << Configurations::TOPIC_CONFIG + ":" + topic->toStringNameCurrentValue();
    ss << Configurations::QOS_CONFIG + ":" + qos->toStringNameCurrentValue();
    ss << Configurations::CLEAN_SESSION_CONFIG + ":" + cleanSession->toStringNameCurrentValue();
    ss << Configurations::FLUSH_INTERVAL_MS_CONFIG + ":" + flushIntervalMS->toStringNameCurrentValue();
    ss << Configurations::INPUT_FORMAT_CONFIG + ":" + inputFormat->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool MQTTSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<MQTTSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<MQTTSourceType>();
    return clientId->getValue() == otherSourceConfig->clientId->getValue()
        && userName->getValue() == otherSourceConfig->userName->getValue()
        && topic->getValue() == otherSourceConfig->topic->getValue() && qos->getValue() == otherSourceConfig->qos->getValue()
        && cleanSession->getValue() == otherSourceConfig->cleanSession->getValue()
        && flushIntervalMS->getValue() == otherSourceConfig->flushIntervalMS->getValue()
        && inputFormat->getValue() == otherSourceConfig->inputFormat->getValue();
}

Configurations::StringConfigOption MQTTSourceType::getUrl() const { return url; }

Configurations::StringConfigOption MQTTSourceType::getClientId() const { return clientId; }

Configurations::StringConfigOption MQTTSourceType::getUserName() const { return userName; }

Configurations::StringConfigOption MQTTSourceType::getTopic() const { return topic; }

Configurations::IntConfigOption MQTTSourceType::getQos() const { return qos; }

Configurations::BoolConfigOption MQTTSourceType::getCleanSession() const { return cleanSession; }

Configurations::FloatConfigOption MQTTSourceType::getFlushIntervalMS() const { return flushIntervalMS; }

Configurations::StringConfigOption MQTTSourceType::getInputFormat() const { return inputFormat; }

void MQTTSourceType::setUrl(std::string urlValue) { url->setValue(std::move(urlValue)); }

void MQTTSourceType::setClientId(std::string clientIdValue) { clientId->setValue(std::move(clientIdValue)); }

void MQTTSourceType::setUserName(std::string userNameValue) { userName->setValue(std::move(userNameValue)); }

void MQTTSourceType::setTopic(std::string topicValue) { topic->setValue(std::move(topicValue)); }

void MQTTSourceType::setQos(uint32_t qosValue) { qos->setValue(qosValue); }

void MQTTSourceType::setCleanSession(bool cleanSessionValue) { cleanSession->setValue(cleanSessionValue); }

void MQTTSourceType::setFlushIntervalMS(float flushIntervalMs) { flushIntervalMS->setValue(flushIntervalMs); }

void MQTTSourceType::setInputFormat(std::string inputFormatValue) { inputFormat->setValue(std::move(inputFormatValue)); }

void MQTTSourceType::reset() {
    setUrl(url->getDefaultValue());
    setClientId(clientId->getDefaultValue());
    setUserName(userName->getDefaultValue());
    setTopic(topic->getDefaultValue());
    setQos(qos->getDefaultValue());
    setCleanSession(cleanSession->getDefaultValue());
    setFlushIntervalMS(flushIntervalMS->getDefaultValue());
    setInputFormat(inputFormat->getDefaultValue());
}

}// namespace NES
