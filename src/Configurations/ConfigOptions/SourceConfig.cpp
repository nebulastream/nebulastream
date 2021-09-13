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
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>
#include <utility>

namespace NES {

SourceConfigPtr SourceConfig::create() { return std::make_shared<SourceConfig>(SourceConfig()); }

SourceConfig::SourceConfig() {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
    sourceType =
        ConfigOption<std::string>::create("sourceType",
                                          "DefaultSource",
                                          "Type of the Source (available options: DefaultSource, CSVSource, BinarySource).");
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
    flushIntervalMS = ConfigOption<float>::create("flushIntervalMS", -1, "tupleBuffer flush interval in milliseconds");
    rowLayout = ConfigOption<bool>::create("rowLayout", true, "storage layout, true = row layout, false = column layout");
    connectionTimeout =
        ConfigOption<uint32_t>::create("connectionTimeout", 10, "connection time out for source, needed for: KafkaSource");
    autoCommit = ConfigOption<uint32_t>::create(
        "autoCommit",
        1,
        "auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource");
    sourceFrequency = ConfigOption<uint32_t>::create("sourceFrequency", 1, "Sampling frequency of the source.");
    numberOfBuffersToProduce = ConfigOption<uint32_t>::create("numberOfBuffersToProduce", 1, "Number of buffers to produce.");
    numberOfTuplesToProducePerBuffer =
        ConfigOption<uint32_t>::create("numberOfTuplesToProducePerBuffer", 1, "Number of tuples to produce per buffer.");
    physicalStreamName =
        ConfigOption<std::string>::create("physicalStreamName", "default_physical", "Physical name of the stream.");
    logicalStreamName = ConfigOption<std::string>::create("logicalStreamName", "default_logical", "Logical name of the stream.");
    skipHeader = ConfigOption<bool>::create("skipHeader", false, "Skip first line of the file.");
}

void SourceConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("NesSourceConfig: Using config file with path: " << filePath << " .");
        Yaml::Node config;
        Yaml::Parse(config, filePath.c_str());
        try {
            if (!config["sourceType"].As<std::string>().empty() && config["sourceType"].As<std::string>() != "\n") {
                setSourceType(config["sourceType"].As<std::string>());
            }
            if (!config["udsf"].As<std::string>().empty() && config["udsf"].As<std::string>() != "\n"){
                setUdsf(config["udsf"].As<std::string>());
            }
            if (!config["filePath"].As<std::string>().empty() && config["filePath"].As<std::string>() != "\n"){
                setFilePath(config["filePath"].As<std::string>());
            }
            if (!config["url"].As<std::string>().empty() && config["url"].As<std::string>() != "\n"){
                setUrl(config["url"].As<std::string>());
            }
            if (!config["namespaceIndex"].As<std::string>().empty() && config["namespaceIndex"].As<std::string>() != "\n"){
                setNamespaceIndex(config["namespaceIndex"].As<uint32_t>());
            }
            if (!config["nodeIdentifier"].As<std::string>().empty() && config["nodeIdentifier"].As<std::string>() != "\n"){
                setNodeIdentifier(config["nodeIdentifier"].As<std::string>());
            }
            if (!config["clientId"].As<std::string>().empty() && config["clientId"].As<std::string>() != "\n"){
                setClientId(config["clientId"].As<std::string>());
            }
            if (!config["userName"].As<std::string>().empty() && config["userName"].As<std::string>() != "\n"){
                setUserName(config["userName"].As<std::string>());
            }
            if (!config["password"].As<std::string>().empty() && config["password"].As<std::string>() != "\n"){
                setPassword(config["password"].As<std::string>());
            }
            if (!config["topic"].As<std::string>().empty() && config["topic"].As<std::string>() != "\n"){
                setTopic(config["topic"].As<std::string>());
            }
            if (!config["inputFormat"].As<std::string>().empty() && config["inputFormat"].As<std::string>() != "\n"){
                setInputFormat(config["inputFormat"].As<std::string>());
            }
            if (!config["qos"].As<std::string>().empty() && config["qos"].As<std::string>() != "\n"){
                setQos(config["qos"].As<uint32_t>());
            }
            if (!config["cleanSession"].As<std::string>().empty() && config["cleanSession"].As<std::string>() != "\n"){
                setCleanSession(config["cleanSession"].As<bool>());
            }
            if (!config["flushIntervalMS"].As<std::string>().empty() && config["flushIntervalMS"].As<std::string>() != "\n"){
                setFlushIntervalMS(config["flushIntervalMS"].As<float>());
            }
            if (!config["rowLayout"].As<std::string>().empty() && config["rowLayout"].As<std::string>() != "\n"){
                setRowLayout(config["rowLayout"].As<bool>());
            }
            if (!config["connectionTimeout"].As<std::string>().empty() && config["connectionTimeout"].As<std::string>() != "\n"){
                setConnectionTimeout(config["connectionTimeout"].As<uint32_t>());
            }
            if (!config["autoCommit"].As<std::string>().empty() && config["autoCommit"].As<std::string>() != "\n"){
                setAutoCommit(config["autoCommit"].As<uint32_t>());
            }
            if (!config["sourceFrequency"].As<std::string>().empty() && config["sourceFrequency"].As<std::string>() != "\n") {
                setSourceFrequency(config["sourceFrequency"].As<uint16_t>());
            }
            if (!config["numberOfBuffersToProduce"].As<std::string>().empty()
                && config["numberOfBuffersToProduce"].As<std::string>() != "\n") {
                setNumberOfBuffersToProduce(config["numberOfBuffersToProduce"].As<uint64_t>());
            }
            if (!config["numberOfTuplesToProducePerBuffer"].As<std::string>().empty()
                && config["numberOfTuplesToProducePerBuffer"].As<std::string>() != "\n") {
                setNumberOfTuplesToProducePerBuffer(config["numberOfTuplesToProducePerBuffer"].As<uint16_t>());
            }
            if (!config["physicalStreamName"].As<std::string>().empty()
                && config["physicalStreamName"].As<std::string>() != "\n") {
                setPhysicalStreamName(config["physicalStreamName"].As<std::string>());
            }
            if (!config["logicalStreamName"].As<std::string>().empty() && config["logicalStreamName"].As<std::string>() != "\n") {
                setLogicalStreamName(config["logicalStreamName"].As<std::string>());
            }
            if (!config["skipHeader"].As<std::string>().empty() && config["skipHeader"].As<std::string>() != "\n") {
                setSkipHeader(config["skipHeader"].As<bool>());
            }
        } catch (std::exception& e) {
            NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from XAML file.");
            NES_WARNING("NesWorkerConfig: Keeping default values.");
            resetSourceOptions();
        }
        return;
    }
    NES_ERROR("NesWorkerConfig: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("NesWorkerConfig: Keeping default values for Coordinator Config.");
}

void SourceConfig::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    try {
        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            if (it->first == "--sourceType" && !it->second.empty()) {
                setSourceType(it->second);
            } else if (it->first == "--udsf" && !it->second.empty()) {
                setUdsf(it->second);
            } else if (it->first == "--filePath" && !it->second.empty()) {
                setFilePath(it->second);
            } else if (it->first == "--url" && !it->second.empty()) {
                setUrl(it->second);
            } else if (it->first == "--namespaceIndex" && !it->second.empty()) {
                setNamespaceIndex(stoi(it->second));
            } else if (it->first == "--nodeIdentifier" && !it->second.empty()) {
                setNodeIdentifier(it->second);
            } else if (it->first == "--clientId" && !it->second.empty()) {
                setClientId(it->second);
            } else if (it->first == "--userName" && !it->second.empty()) {
                setUserName(it->second);
            } else if (it->first == "--password" && !it->second.empty()) {
                setPassword(it->second);
            } else if (it->first == "--topic" && !it->second.empty()) {
                setTopic(it->second);
            } else if (it->first == "--inputFormat" && !it->second.empty()) {
                setInputFormat(it->second);
            } else if (it->first == "--qos" && !it->second.empty()) {
                setQos(stoi(it->second));
            } else if (it->first == "--cleanSession" && !it->second.empty()) {
                setCleanSession((it->second == "true"));
            } else if (it->first == "--flushIntervalMS" && !it->second.empty()) {
                setFlushIntervalMS(stof(it->second));
            } else if (it->first == "--rowLayout" && !it->second.empty()) {
                setRowLayout((it->second == "true"));
            } else if (it->first == "--connectionTimeout" && !it->second.empty()) {
                setConnectionTimeout(stoi(it->second));
            } else if (it->first == "--autoCommit" && !it->second.empty()) {
                setAutoCommit(stoi(it->second));
            } else if (it->first == "--sourceFrequency" && !it->second.empty()) {
                setSourceFrequency(stoi(it->second));
            } else if (it->first == "--numberOfBuffersToProduce" && !it->second.empty()) {
                setNumberOfBuffersToProduce(stoi(it->second));
            } else if (it->first == "--numberOfTuplesToProducePerBuffer" && !it->second.empty()) {
                setNumberOfTuplesToProducePerBuffer(stoi(it->second));
            } else if (it->first == "--physicalStreamName" && !it->second.empty()) {
                setPhysicalStreamName(it->second);
            } else if (it->first == "--logicalStreamName" && !it->second.empty()) {
                setLogicalStreamName(it->second);
            } else if (it->first == "--skipHeader" && !it->second.empty()) {
                setSkipHeader((it->second == "true"));
            } else {
                NES_WARNING("Unknow configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from command line. " << e.what());
        NES_WARNING("NesWorkerConfig: Keeping default values for Source.");
        resetSourceOptions();
    }
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

StringConfigOption SourceConfig::getFilePath() const { return filePath; }

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

BoolConfigOption SourceConfig::getSkipHeader() const { return skipHeader; }

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

void SourceConfig::setSkipHeader(bool skipHeaderValue) { skipHeader->setValue(skipHeaderValue); }

void SourceConfig::setUdsf(std::string udsfValue) { udsf->setValue(std::move(udsfValue)); }

void SourceConfig::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void SourceConfig::setUrl(std::string urlValue) { url->setValue(std::move(urlValue)); }

void SourceConfig::setNamespaceIndex(uint32_t namespaceIndexValue) { namespaceIndex->setValue(namespaceIndexValue); }

void SourceConfig::setNodeIdentifier(std::string nodeIdentifierValue) { nodeIdentifier->setValue(std::move(nodeIdentifierValue)); }

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