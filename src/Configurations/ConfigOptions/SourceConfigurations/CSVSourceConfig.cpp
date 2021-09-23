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
#include <Configurations/ConfigOptions/SourceConfigurations/CSVSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>
#include <string>
#include <utility>

namespace NES {
CSVSourceConfigPtr CSVSourceConfig::create() { return std::make_shared<CSVSourceConfig>(CSVSourceConfig()); }

CSVSourceConfig::CSVSourceConfig() : SourceConfig() {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
    filePath = ConfigOption<std::string>::create("filePath",
                                                 "../tests/test_data/QnV_short.csv",
                                                 "file path, needed for: CSVSource, BinarySource");
    skipHeader = ConfigOption<bool>::create("skipHeader", false, "Skip first line of the file.");
}

void CSVSourceConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("NesSourceConfig: Using config file with path: " << filePath << " .");
        Yaml::Node config;
        Yaml::Parse(config, filePath.c_str());
        try {
            if (!config["sourceType"].As<std::string>().empty() && config["sourceType"].As<std::string>() != "\n") {
                setSourceType(config["sourceType"].As<std::string>());
            }
            if (!config["udsf"].As<std::string>().empty() && config["udsf"].As<std::string>() != "\n") {
                setUdsf(config["udsf"].As<std::string>());
            }
            if (!config["filePath"].As<std::string>().empty() && config["filePath"].As<std::string>() != "\n") {
                setFilePath(config["filePath"].As<std::string>());
            }
            if (!config["url"].As<std::string>().empty() && config["url"].As<std::string>() != "\n") {
                setUrl(config["url"].As<std::string>());
            }
            if (!config["namespaceIndex"].As<std::string>().empty() && config["namespaceIndex"].As<std::string>() != "\n") {
                setNamespaceIndex(config["namespaceIndex"].As<uint32_t>());
            }
            if (!config["nodeIdentifier"].As<std::string>().empty() && config["nodeIdentifier"].As<std::string>() != "\n") {
                setNodeIdentifier(config["nodeIdentifier"].As<std::string>());
            }
            if (!config["clientId"].As<std::string>().empty() && config["clientId"].As<std::string>() != "\n") {
                setClientId(config["clientId"].As<std::string>());
            }
            if (!config["userName"].As<std::string>().empty() && config["userName"].As<std::string>() != "\n") {
                setUserName(config["userName"].As<std::string>());
            }
            if (!config["password"].As<std::string>().empty() && config["password"].As<std::string>() != "\n") {
                setPassword(config["password"].As<std::string>());
            }
            if (!config["topic"].As<std::string>().empty() && config["topic"].As<std::string>() != "\n") {
                setTopic(config["topic"].As<std::string>());
            }
            if (!config["inputFormat"].As<std::string>().empty() && config["inputFormat"].As<std::string>() != "\n") {
                setInputFormat(config["inputFormat"].As<std::string>());
            }
            if (!config["qos"].As<std::string>().empty() && config["qos"].As<std::string>() != "\n") {
                setQos(config["qos"].As<uint32_t>());
            }
            if (!config["cleanSession"].As<std::string>().empty() && config["cleanSession"].As<std::string>() != "\n") {
                setCleanSession(config["cleanSession"].As<bool>());
            }
            if (!config["flushIntervalMS"].As<std::string>().empty() && config["flushIntervalMS"].As<std::string>() != "\n") {
                setFlushIntervalMS(config["flushIntervalMS"].As<float>());
            }
            if (!config["rowLayout"].As<std::string>().empty() && config["rowLayout"].As<std::string>() != "\n") {
                setRowLayout(config["rowLayout"].As<bool>());
            }
            if (!config["connectionTimeout"].As<std::string>().empty() && config["connectionTimeout"].As<std::string>() != "\n") {
                setConnectionTimeout(config["connectionTimeout"].As<uint32_t>());
            }
            if (!config["autoCommit"].As<std::string>().empty() && config["autoCommit"].As<std::string>() != "\n") {
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

void CSVSourceConfig::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
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

void CSVSourceConfig::resetSourceOptions() {
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

std::string CSVSourceConfig::toString() {
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

StringConfigOption CSVSourceConfig::getFilePath() const { return filePath; }

BoolConfigOption CSVSourceConfig::getSkipHeader() const { return skipHeader; }

void CSVSourceConfig::setSkipHeader(bool skipHeaderValue) { skipHeader->setValue(skipHeaderValue); }

void CSVSourceConfig::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }
void CSVSourceConfig::overwriteConfigWithYAMLFileInput(const std::string& filePath) {
    SourceConfig::overwriteConfigWithYAMLFileInput(filePath);
}
void CSVSourceConfig::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    SourceConfig::overwriteConfigWithCommandLineInput(inputParams);
}
void CSVSourceConfig::resetSourceOptions() { SourceConfig::resetSourceOptions(); }
std::string CSVSourceConfig::toString() { return SourceConfig::toString(); }

}// namespace NES