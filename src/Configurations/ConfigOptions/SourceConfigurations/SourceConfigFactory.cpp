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

#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfigFactory.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>

namespace NES {

std::unique_ptr<SourceConfig>
NES::SourceConfigFactory::createSourceConfig(const std::map<std::string, std::string> commandLineParams, const int argc) {

    auto sourceConfigPath = commandLineParams.find("--sourceConfigPath");

    if (sourceConfigPath != commandLineParams.end()) {
        readYAMLFile(sourceConfigPath->second);
    }

    if (argc >= 1) {
        overwriteConfigWithCommandLineInput(commandLineParams);
    }

    switch (stringToConfigSourceType[sourceConfigMap["sourceType"]]) {
        case CSVSource: return CSVSourceConfig::create(sourceConfigMap);
        case MQTTSource: return MQTTSourceConfig::create(sourceConfigMap);
        case KafkaSource: return KafkaSourceConfig::create(sourceConfigMap);
        case OPCSource: return OPCSourceConfig::create(sourceConfigMap);
        case BinarySource: return BinarySourceConfig::create(sourceConfigMap);
        case SenseSource: return SenseSourceConfig::create(sourceConfigMap);
        default: return nullptr;
    }
}
void SourceConfigFactory::readYAMLFile(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("NesSourceConfigFactory: Using config file with path: " << filePath << " .");
        Yaml::Node config;
        Yaml::Parse(config, filePath.c_str());
        try {
            if (!config["numberOfBuffersToProduce"].As<std::string>().empty()
                && config["numberOfBuffersToProduce"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("numberOfBuffersToProduce",
                                                                           config["numberOfBuffersToProduce"].As<std::string>()));
            }
            if (!config["numberOfTuplesToProducePerBuffer"].As<std::string>().empty()
                && config["numberOfTuplesToProducePerBuffer"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("numberOfTuplesToProducePerBuffer",
                                       config["numberOfTuplesToProducePerBuffer"].As<std::string>()));
            }
            if (!config["physicalStreamName"].As<std::string>().empty()
                && config["physicalStreamName"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("physicalStreamName", config["physicalStreamName"].As<std::string>()));
            }
            if (!config["sourceFrequency"].As<std::string>().empty() && config["sourceFrequency"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("sourceFrequency", config["sourceFrequency"].As<std::string>()));
            }
            if (!config["logicalStreamName"].As<std::string>().empty() && config["logicalStreamName"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("logicalStreamName", config["logicalStreamName"].As<std::string>()));
            }
            if (!config["rowLayout"].As<std::string>().empty() && config["rowLayout"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("rowLayout", config["rowLayout"].As<std::string>()));
            }
            if (!config["flushIntervalMS"].As<std::string>().empty() && config["flushIntervalMS"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("flushIntervalMS", config["flushIntervalMS"].As<std::string>()));
            }
            if (!config["inputFormat"].As<std::string>().empty() && config["inputFormat"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("inputFormat", config["inputFormat"].As<std::string>()));
            }
            if (!config["sourceType"].As<std::string>().empty() && config["sourceType"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("sourceType", config["sourceType"].As<std::string>()));
            }
            if (!config["SenseSource"][0]["udsf"].As<std::string>().empty()
                && config["SenseSource"][0]["udsf"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("SenseSourceUdsf", config["SenseSource"][0]["udsf"].As<std::string>()));
            }
            if (!config["CSVSource"][0]["filePath"].As<std::string>().empty()
                && config["CSVSource"][0]["filePath"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("CSVSourceFilePath", config["CSVSource"][0]["filePath"].As<std::string>()));
            }
            if (!config["CSVSource"][0]["skipHeader"].As<std::string>().empty()
                && config["CSVSource"][0]["skipHeader"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("CSVSourceSkipHeader", config["CSVSource"][0]["skipHeader"].As<std::string>()));
            }
            if (!config["BinarySource"][0]["filePath"].As<std::string>().empty()
                && config["BinarySource"][0]["filePath"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("BinarySourceFilePath", config["BinarySource"][0]["filePath"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["url"].As<std::string>().empty()
                && config["MQTTSource"][0]["url"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("MQTTSourceUrl", config["MQTTSource"][0]["url"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["clientId"].As<std::string>().empty()
                && config["MQTTSource"][0]["clientId"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("MQTTSourceClientId", config["MQTTSource"][0]["clientId"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["userName"].As<std::string>().empty()
                && config["MQTTSource"][0]["userName"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("MQTTSourcUuserName", config["MQTTSource"][0]["userName"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["topic"].As<std::string>().empty()
                && config["MQTTSource"][0]["topic"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("MQTTSourceTopic", config["MQTTSource"][0]["topic"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["qos"].As<std::string>().empty()
                && config["MQTTSource"][0]["qos"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("MQTTSourceQos", config["MQTTSource"][0]["qos"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["cleanSession"].As<std::string>().empty()
                && config["MQTTSource"][0]["cleanSession"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("MQTTSourceCleanSession", config["MQTTSource"][0]["cleanSession"].As<std::string>()));
            }
            if (!config["KafkaSource"][0]["autoCommit"].As<std::string>().empty()
                && config["KafkaSource"][0]["autoCommit"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("KafkaSourceAutoCommit", config["KafkaSource"][0]["autoCommit"].As<std::string>()));
            }
            if (!config["KafkaSource"][0]["groupId"].As<std::string>().empty()
                && config["KafkaSource"][0]["groupId"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("KafkaSourceGroupId", config["KafkaSource"][0]["groupId"].As<std::string>()));
            }
            if (!config["KafkaSource"][0]["topic"].As<std::string>().empty()
                && config["KafkaSource"][0]["topic"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("KafkaSourceTopic", config["KafkaSource"][0]["topic"].As<std::string>()));
            }
            if (!config["KafkaSource"][0]["connectionTimeout"].As<std::string>().empty()
                && config["KafkaSource"][0]["connectionTimeout"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("KafkaSourceConnectionTimeout", config["KafkaSource"][0]["connectionTimeout"].As<std::string>()));
            }
            if (!config["OPCSource"][0]["namespaceIndex"].As<std::string>().empty()
                && config["OPCSource"][0]["namespaceIndex"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("OPCSourceNamespaceIndex", config["OPCSource"][0]["namespaceIndex"].As<std::string>()));
            }
            if (!config["OPCSource"][0]["nodeIdentifier"].As<std::string>().empty()
                && config["OPCSource"][0]["nodeIdentifier"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("OPCSourceNodeIdentifier", config["OPCSource"][0]["nodeIdentifier"].As<std::string>()));
            }
            if (!config["OPCSource"][0]["userName"].As<std::string>().empty()
                && config["OPCSource"][0]["userName"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("OPCSourceUserName", config["OPCSource"][0]["userName"].As<std::string>()));
            }
            if (!config["OPCSource"][0]["password"].As<std::string>().empty()
                && config["OPCSource"][0]["password"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("OPCSourcePassword", config["OPCSource"][0]["password"].As<std::string>()));
            }
        }
        catch (std::exception& e) {
            NES_ERROR("NesSourceConfigFactory: Error while initializing configuration parameters from XAML file.");
            NES_WARNING("NesSourceConfigFactory: Keeping default values.");
        }
    }
    NES_ERROR("NesSourceConfigFactory: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("NesSourceConfigFactory: Keeping default values for Source Config.");

}
void SourceConfigFactory::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& commandLineParams) {

    try {
        for (auto it = commandLineParams.begin(); it != commandLineParams.end(); ++it) {
            if (it->first == "--sourceType" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("sourceType", it->second);
            } else if (it->first == "--numberOfBuffersToProduce" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("numberOfBuffersToProduce", it->second);
            } else if (it->first == "--numberOfTuplesToProducePerBuffer" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("numberOfTuplesToProducePerBuffer", it->second);
            } else if (it->first == "--physicalStreamName" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("physicalStreamName", it->second);
            } else if (it->first == "--logicalStreamName" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("logicalStreamName", it->second);
            } else if (it->first == "--sourceFrequency" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("sourceFrequency", it->second);
            } else if (it->first == "--rowLayout" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("rowLayout", it->second);
            } else if (it->first == "--flushIntervalMS" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("flushIntervalMS", it->second);
            } else if (it->first == "--inputFormat" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("inputFormat", it->second);
            } else if (it->first == "--SenseSourceUdsf" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("SenseSourceUdsf", it->second);
            } else if (it->first == "--CSVSourceFilePath" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("CSVSourceFilePath", it->second);
            } else if (it->first == "--CSVSourceSkipHeader" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("CSVSourceSkipHeader", it->second);
            } else if (it->first == "--MQTTSourceUrl" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("MQTTSourceUrl", it->second);
            } else if (it->first == "--MQTTSourceClientId" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("MQTTSourceClientId", it->second);
            } else if (it->first == "--MQTTSourceUserName" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("MQTTSourceUserName", it->second);
            } else if (it->first == "--MQTTSourceTopic" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("MQTTSourceTopic", it->second);
            } else if (it->first == "--MQTTSourceQos" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("MQTTSourceQos", it->second);
            } else if (it->first == "--MQTTSourceCleanSession" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("MQTTSourceCleanSession", it->second);
            } else if (it->first == "--KafkaSourceAutoCommit" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("inputFormat", it->second);
            } else if (it->first == "--KafkaSourceGroupId" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("KafkaSourceGroupId", it->second);
            } else if (it->first == "--KafkaSourceTopic" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("KafkaSourceTopic", it->second);
            } else if (it->first == "--KafkaSourceConnectionTimeout" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("KafkaSourceConnectionTimeout", it->second);
            } else if (it->first == "--OPCSourceNamespaceIndex" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("MQTTSourceNamespaceIndex", it->second);
            } else if (it->first == "--OPCSourceNodeIdentifier" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("OPCSourceNodeIdentifier", it->second);
            } else if (it->first == "--OPCSourceUserName" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("MQTTSourceUserName", it->second);
            } else if (it->first == "--OPCSourcePassword" && !it->second.empty()) {
                sourceConfigMap.insert_or_assign("OPCSourcePassword", it->second);
            } else {
                NES_WARNING("Unknow configuration value :" << it->first);
            }
        }
    } catch (std::exception& e) {
        NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from command line. " << e.what());
        NES_WARNING("NesWorkerConfig: Keeping default values for Source.");
    }

}
}// namespace NES