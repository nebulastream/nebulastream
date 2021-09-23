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
#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>
#include <utility>

namespace NES {

SourceConfig::SourceConfig(std::map<std::string, std::string> sourceConfigMap) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");

    for (auto it = sourceConfigMap.begin(); it != sourceConfigMap.end(); ++it) {
        if (it->first == "numberOfBuffersToProduce") {
            numberOfBuffersToProduce =
                ConfigOption<uint32_t>::create("numberOfBuffersToProduce", stoi(it->second), 1, "Number of buffers to produce.");
        } else {
            numberOfBuffersToProduce =
                ConfigOption<uint32_t>::create("numberOfBuffersToProduce", 1, "Number of buffers to produce.");
        }
        if (it->first == "numberOfTuplesToProducePerBuffer") {
            numberOfTuplesToProducePerBuffer = ConfigOption<uint32_t>::create("numberOfTuplesToProducePerBuffer",
                                                                              stoi(it->second),
                                                                              1,
                                                                              "Number of tuples to produce per buffer.");
        } else {
            numberOfTuplesToProducePerBuffer =
                ConfigOption<uint32_t>::create("numberOfTuplesToProducePerBuffer", 1, "Number of tuples to produce per buffer.");
        }
        if (it->first == "physicalStreamName") {
            physicalStreamName =
                ConfigOption<std::string>::create("physicalStreamName", it->second, "default_physical", "Physical name of the stream.");
        } else {
            physicalStreamName =
                ConfigOption<std::string>::create("physicalStreamName", "default_physical", "Physical name of the stream.");
        }
        if (it->first == "logicalStreamName") {
            logicalStreamName = ConfigOption<std::string>::create("logicalStreamName", it->second, "default_logical", "Logical name of the stream.");
        } else {
            logicalStreamName = ConfigOption<std::string>::create("logicalStreamName", "default_logical", "Logical name of the stream.");
        }
        if (it->first == "sourceFrequency") {
            sourceFrequency = ConfigOption<uint32_t>::create("sourceFrequency", stoi(it->second), 1, "Sampling frequency of the source.");
        } else {
            sourceFrequency = ConfigOption<uint32_t>::create("sourceFrequency", 1, "Sampling frequency of the source.");
        }
        if (it->first == "rowLayout") {
            rowLayout = ConfigOption<bool>::create("rowLayout",  (it->second == "true"), true, "storage layout, true = row layout, false = column layout");
        } else {
            rowLayout = ConfigOption<bool>::create("rowLayout", true, "storage layout, true = row layout, false = column layout");
        }
        if (it->first == "flushIntervalMS") {
            flushIntervalMS = ConfigOption<float>::create("flushIntervalMS",  stof(it->second), -1, "tupleBuffer flush interval in milliseconds");
        } else {
            flushIntervalMS = ConfigOption<float>::create("flushIntervalMS", -1, "tupleBuffer flush interval in milliseconds");
        }
        if (it->first == "inputFormat") {
            inputFormat = ConfigOption<std::string>::create("inputFormat", it->second, "JSON", "input data format");

        } else {
            inputFormat = ConfigOption<std::string>::create("inputFormat", "JSON", "input data format");
        }
        if (it->first == "sourceType") {
            sourceType =
                ConfigOption<std::string>::create("sourceType",
                                                  it->second,
                                                  "DefaultSource",
                                                  "Type of the Source (available options: DefaultSource, CSVSource, BinarySource).");
        } else {
            sourceType =
                ConfigOption<std::string>::create("sourceType",
                                                  "DefaultSource",
                                                  "Type of the Source (available options: DefaultSource, CSVSource, BinarySource).");
        }
    }


    udsf = ConfigOption<std::string>::create("udsf", "", "udsf, needed for: SenseSource");
    filePath = ConfigOption<std::string>::create("filePath",
                                                 "../tests/test_data/QnV_short.csv",
                                                 "file path, needed for: CSVSource, BinarySource");
    url = ConfigOption<std::string>::create("url",
                                            "ws://127.0.0.1:9001",
                                            "url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource");
    namespaceIndex = ConfigOption<uint32_t>::create("namespaceIndex", 1, "namespaceIndex for node, needed for: OPCSource");
    nodeIdentifier = ConfigOption<std::string>::create("nodeIdentifier", "the.answer", "node identifier, needed for: OPCSource");
    clientId = ConfigOption<std::string>::create("clientId",
                                                 "testClient",
                                                 "clientId, needed for: MQTTSource (needs to be unique for each connected "
                                                 "MQTTSource), KafkaSource (use this for groupId)");
    userName = ConfigOption<std::string>::create("userName",
                                                 "testUser",
                                                 "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource");
    password = ConfigOption<std::string>::create("password", "", "password, needed for: OPCSource");
    topic = ConfigOption<std::string>::create("topic",
                                              "demoTownSensorData",
                                              "topic to listen to, needed for: MQTTSource, KafkaSource");
    inputFormat = ConfigOption<std::string>::create("inputFormat", "JSON", "input data format");
    qos = ConfigOption<uint32_t>::create("qos", 2, "quality of service, needed for: MQTTSource");
    cleanSession =
        ConfigOption<bool>::create("cleanSession",
                                   true,
                                   "cleanSession true = clean up session after client loses connection, false = keep data for "
                                   "client after connection loss (persistent session), needed for: MQTTSource");

    connectionTimeout =
        ConfigOption<uint32_t>::create("connectionTimeout", 10, "connection time out for source, needed for: KafkaSource");
    autoCommit = ConfigOption<uint32_t>::create(
        "autoCommit",
        1,
        "auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource");
    skipHeader = ConfigOption<bool>::create("skipHeader", false, "Skip first line of the file.");
}

void SourceConfig::resetSourceOptions() {
    setSourceType(sourceType->getDefaultValue());
    setUdsf(udsf->getDefaultValue());
    setFilePath(filePath->getDefaultValue());
    setUrl(url->getDefaultValue());
    setNamespaceIndex(namespaceIndex->getDefaultValue());
    setNodeIdentifier(nodeIdentifier->getDefaultValue());
    setClientId(clientId->getDefaultValue());
    setUserName(userName->getDefaultValue());
    setPassword(password->getDefaultValue());
    setTopic(topic->getDefaultValue());
    setInputFormat(inputFormat->getDefaultValue());
    setQos(qos->getDefaultValue());
    setCleanSession(cleanSession->getDefaultValue());
    setFlushIntervalMS(flushIntervalMS->getDefaultValue());
    setRowLayout(rowLayout->getDefaultValue());
    setConnectionTimeout(connectionTimeout->getDefaultValue());
    setAutoCommit(autoCommit->getDefaultValue());
    setSourceFrequency(sourceFrequency->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer->getDefaultValue());
    setPhysicalStreamName(physicalStreamName->getDefaultValue());
    setLogicalStreamName(logicalStreamName->getDefaultValue());
    setSkipHeader(skipHeader->getDefaultValue());
}

std::string SourceConfig::toString() {
    std::stringstream ss;
    ss << sourceType->toStringNameCurrentValue();
    ss << udsf->toStringNameCurrentValue();
    ss << filePath->toStringNameCurrentValue();
    ss << namespaceIndex->toStringNameCurrentValue();
    ss << nodeIdentifier->toStringNameCurrentValue();
    ss << clientId->toStringNameCurrentValue();
    ss << userName->toStringNameCurrentValue();
    ss << password->toStringNameCurrentValue();
    ss << topic->toStringNameCurrentValue();
    ss << inputFormat->toStringNameCurrentValue();
    ss << qos->toStringNameCurrentValue();
    ss << cleanSession->toStringNameCurrentValue();
    ss << flushIntervalMS->toStringNameCurrentValue();
    ss << rowLayout->toStringNameCurrentValue();
    ss << connectionTimeout->toStringNameCurrentValue();
    ss << autoCommit->toStringNameCurrentValue();
    ss << sourceFrequency->toStringNameCurrentValue();
    ss << numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << numberOfTuplesToProducePerBuffer->toStringNameCurrentValue();
    ss << physicalStreamName->toStringNameCurrentValue();
    ss << logicalStreamName->toStringNameCurrentValue();
    ss << skipHeader->toStringNameCurrentValue();
    return ss.str();
}

StringConfigOption SourceConfig::getSourceType() const { return sourceType; }

StringConfigOption SourceConfig::getUdsf() const { return udsf; }

StringConfigOption SourceConfig::getUrl() const { return url; }

IntConfigOption SourceConfig::getNamespaceIndex() const { return namespaceIndex; }

StringConfigOption SourceConfig::getNodeIdentifier() const { return nodeIdentifier; }

StringConfigOption SourceConfig::getClientId() const { return clientId; }

StringConfigOption SourceConfig::getUserName() const { return userName; }

StringConfigOption SourceConfig::getPassword() const { return password; }

StringConfigOption SourceConfig::getTopic() const { return topic; }

StringConfigOption SourceConfig::getInputFormat() const { return inputFormat; }

IntConfigOption SourceConfig::getQos() const { return qos; }

BoolConfigOption SourceConfig::getCleanSession() const { return cleanSession; }

FloatConfigOption SourceConfig::getFlushIntervalMS() const { return flushIntervalMS; }

BoolConfigOption SourceConfig::getRowLayout() const { return rowLayout; }

IntConfigOption SourceConfig::getConnectionTimeout() const { return connectionTimeout; }

IntConfigOption SourceConfig::getAutoCommit() const { return autoCommit; }

IntConfigOption SourceConfig::getSourceFrequency() const { return sourceFrequency; }

IntConfigOption SourceConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

IntConfigOption SourceConfig::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

StringConfigOption SourceConfig::getPhysicalStreamName() const { return physicalStreamName; }

StringConfigOption SourceConfig::getLogicalStreamName() const { return logicalStreamName; }

void SourceConfig::setSourceType(std::string sourceTypeValue) { sourceType->setValue(std::move(sourceTypeValue)); }

void SourceConfig::setSourceFrequency(uint32_t sourceFrequencyValue) { sourceFrequency->setValue(sourceFrequencyValue); }

void SourceConfig::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void SourceConfig::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBufferValue) {
    numberOfTuplesToProducePerBuffer->setValue(numberOfTuplesToProducePerBufferValue);
}

void SourceConfig::setPhysicalStreamName(std::string physicalStreamNameValue) {
    physicalStreamName->setValue(std::move(physicalStreamNameValue));
}

void SourceConfig::setLogicalStreamName(std::string logicalStreamNameValue) {
    logicalStreamName->setValue(std::move(logicalStreamNameValue));
}

void SourceConfig::setUdsf(std::string udsfValue) { udsf->setValue(std::move(udsfValue)); }

void SourceConfig::setUrl(std::string urlValue) { url->setValue(std::move(urlValue)); }

void SourceConfig::setNamespaceIndex(uint32_t namespaceIndexValue) { namespaceIndex->setValue(namespaceIndexValue); }

void SourceConfig::setNodeIdentifier(std::string nodeIdentifierValue) {
    nodeIdentifier->setValue(std::move(nodeIdentifierValue));
}

void SourceConfig::setClientId(std::string clientIdValue) { clientId->setValue(std::move(clientIdValue)); }

void SourceConfig::setUserName(std::string userNameValue) { userName->setValue(std::move(userNameValue)); }

void SourceConfig::setPassword(std::string passwordValue) { password->setValue(std::move(passwordValue)); }

void SourceConfig::setTopic(std::string topicValue) { topic->setValue(std::move(topicValue)); }

void SourceConfig::setInputFormat(std::string inputFormatValue) { inputFormat->setValue(std::move(inputFormatValue)); }

void SourceConfig::setQos(uint32_t qosValue) { qos->setValue(qosValue); }

void SourceConfig::setCleanSession(bool cleanSessionValue) { cleanSession->setValue(cleanSessionValue); }

void SourceConfig::setFlushIntervalMS(float flushIntervalMs) { flushIntervalMS->setValue(flushIntervalMs); }

void SourceConfig::setRowLayout(bool rowLayoutValue) { rowLayout->setValue(rowLayoutValue); }

void SourceConfig::setConnectionTimeout(uint32_t connectionTimeoutValue) { connectionTimeout->setValue(connectionTimeoutValue); }

void SourceConfig::setAutoCommit(uint32_t autoCommitValue) { autoCommit->setValue(autoCommitValue); }

}// namespace NES