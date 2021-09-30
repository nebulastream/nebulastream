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

#include <Configurations/ConfigOptions/SourceConfigurations/CSVSourceConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfigurations/MQTTSourceConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfigFactory.hpp>
#include <Configurations/ConfigOptions/SourceConfigurations/KafkaSourceConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfigurations/SenseSourceConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfigurations/BinarySourceConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfigurations/OPCSourceConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfigurations/NoSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <filesystem>

namespace NES {

std::shared_ptr<SourceConfig> SourceConfigFactory::createSourceConfig(const std::map<std::string, std::string>& commandLineParams,
                                                                      const int argc) {

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
        case NoSource: return NoSourceConfig::create(sourceConfigMap);
        default: return nullptr;
    }
}

std::shared_ptr<SourceConfig> SourceConfigFactory::createSourceConfig() { return NoSourceConfig::create(sourceConfigMap); }

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
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("numberOfTuplesToProducePerBuffer",
                                                        config["numberOfTuplesToProducePerBuffer"].As<std::string>()));
            }
            if (!config["physicalStreamName"].As<std::string>().empty()
                && config["physicalStreamName"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("physicalStreamName", config["physicalStreamName"].As<std::string>()));
            }
            if (!config["sourceFrequency"].As<std::string>().empty() && config["sourceFrequency"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("sourceFrequency", config["sourceFrequency"].As<std::string>()));
            }
            if (!config["logicalStreamName"].As<std::string>().empty() && config["logicalStreamName"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("logicalStreamName", config["logicalStreamName"].As<std::string>()));
            }
            if (!config["rowLayout"].As<std::string>().empty() && config["rowLayout"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("rowLayout", config["rowLayout"].As<std::string>()));
            }
            if (!config["flushIntervalMS"].As<std::string>().empty() && config["flushIntervalMS"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("flushIntervalMS", config["flushIntervalMS"].As<std::string>()));
            }
            if (!config["inputFormat"].As<std::string>().empty() && config["inputFormat"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("inputFormat", config["inputFormat"].As<std::string>()));
            }
            if (!config["sourceType"].As<std::string>().empty() && config["sourceType"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("sourceType", config["sourceType"].As<std::string>()));
            }
            if (!config["SenseSource"][0]["udsf"].As<std::string>().empty()
                && config["SenseSource"][0]["udsf"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("SenseSourceUdsf", config["SenseSource"][0]["udsf"].As<std::string>()));
            }
            if (!config["CSVSource"][0]["filePath"].As<std::string>().empty()
                && config["CSVSource"][0]["filePath"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("CSVSourceFilePath",
                                                                           config["CSVSource"][0]["filePath"].As<std::string>()));
            }
            if (!config["CSVSource"][0]["skipHeader"].As<std::string>().empty()
                && config["CSVSource"][0]["skipHeader"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("CSVSourceSkipHeader",
                                                        config["CSVSource"][0]["skipHeader"].As<std::string>()));
            }
            if (!config["BinarySource"][0]["filePath"].As<std::string>().empty()
                && config["BinarySource"][0]["filePath"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("BinarySourceFilePath",
                                                        config["BinarySource"][0]["filePath"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["url"].As<std::string>().empty()
                && config["MQTTSource"][0]["url"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("MQTTSourceUrl", config["MQTTSource"][0]["url"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["clientId"].As<std::string>().empty()
                && config["MQTTSource"][0]["clientId"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("MQTTSourceClientId",
                                                        config["MQTTSource"][0]["clientId"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["userName"].As<std::string>().empty()
                && config["MQTTSource"][0]["userName"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("MQTTSourcUuserName",
                                                        config["MQTTSource"][0]["userName"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["topic"].As<std::string>().empty()
                && config["MQTTSource"][0]["topic"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("MQTTSourceTopic", config["MQTTSource"][0]["topic"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["qos"].As<std::string>().empty()
                && config["MQTTSource"][0]["qos"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("MQTTSourceQos", config["MQTTSource"][0]["qos"].As<std::string>()));
            }
            if (!config["MQTTSource"][0]["cleanSession"].As<std::string>().empty()
                && config["MQTTSource"][0]["cleanSession"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("MQTTSourceCleanSession",
                                                        config["MQTTSource"][0]["cleanSession"].As<std::string>()));
            }
            if (!config["KafkaSource"][0]["autoCommit"].As<std::string>().empty()
                && config["KafkaSource"][0]["autoCommit"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("KafkaSourceAutoCommit",
                                                        config["KafkaSource"][0]["autoCommit"].As<std::string>()));
            }
            if (!config["KafkaSource"][0]["groupId"].As<std::string>().empty()
                && config["KafkaSource"][0]["groupId"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("KafkaSourceGroupId",
                                                        config["KafkaSource"][0]["groupId"].As<std::string>()));
            }
            if (!config["KafkaSource"][0]["topic"].As<std::string>().empty()
                && config["KafkaSource"][0]["topic"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("KafkaSourceTopic", config["KafkaSource"][0]["topic"].As<std::string>()));
            }
            if (!config["KafkaSource"][0]["connectionTimeout"].As<std::string>().empty()
                && config["KafkaSource"][0]["connectionTimeout"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("KafkaSourceConnectionTimeout",
                                                        config["KafkaSource"][0]["connectionTimeout"].As<std::string>()));
            }
            if (!config["OPCSource"][0]["namespaceIndex"].As<std::string>().empty()
                && config["OPCSource"][0]["namespaceIndex"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("OPCSourceNamespaceIndex",
                                                        config["OPCSource"][0]["namespaceIndex"].As<std::string>()));
            }
            if (!config["OPCSource"][0]["nodeIdentifier"].As<std::string>().empty()
                && config["OPCSource"][0]["nodeIdentifier"].As<std::string>() != "\n") {
                sourceConfigMap.insert(
                    std::pair<std::string, std::string>("OPCSourceNodeIdentifier",
                                                        config["OPCSource"][0]["nodeIdentifier"].As<std::string>()));
            }
            if (!config["OPCSource"][0]["userName"].As<std::string>().empty()
                && config["OPCSource"][0]["userName"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("OPCSourceUserName",
                                                                           config["OPCSource"][0]["userName"].As<std::string>()));
            }
            if (!config["OPCSource"][0]["password"].As<std::string>().empty()
                && config["OPCSource"][0]["password"].As<std::string>() != "\n") {
                sourceConfigMap.insert(std::pair<std::string, std::string>("OPCSourcePassword",
                                                                           config["OPCSource"][0]["password"].As<std::string>()));
            }
        } catch (std::exception& e) {
            NES_ERROR("NesSourceConfigFactory: Error while initializing configuration parameters from XAML file.");
            NES_WARNING("NesSourceConfigFactory: Keeping default values.");
        }
    }
    NES_ERROR("NesSourceConfigFactory: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("NesSourceConfigFactory: Keeping default values for Source Config.");
}
void SourceConfigFactory::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& commandLineParams) {

    try {
        if (commandLineParams.find("--sourceType") != commandLineParams.end()
            && !commandLineParams.find("--sourceType")->second.empty()) {
            sourceConfigMap.insert_or_assign("sourceType", commandLineParams.find("--sourceType")->second);
        }
        if (commandLineParams.find("--numberOfBuffersToProduce") != commandLineParams.end()
            && !commandLineParams.find("--numberOfBuffersToProduce")->second.empty()) {
            sourceConfigMap.insert_or_assign("numberOfBuffersToProduce",
                                             commandLineParams.find("--numberOfBuffersToProduce")->second);
        }
        if (commandLineParams.find("--numberOfTuplesToProducePerBuffer") != commandLineParams.end()
            && !commandLineParams.find("--numberOfTuplesToProducePerBuffer")->second.empty()) {
            sourceConfigMap.insert_or_assign("numberOfTuplesToProducePerBuffer",
                                             commandLineParams.find("--numberOfTuplesToProducePerBuffer")->second);
        }
        if (commandLineParams.find("--physicalStreamName") != commandLineParams.end()
            && !commandLineParams.find("--physicalStreamName")->second.empty()) {
            sourceConfigMap.insert_or_assign("physicalStreamName", commandLineParams.find("--physicalStreamName")->second);
        }
        if (commandLineParams.find("--logicalStreamName") != commandLineParams.end()
            && !commandLineParams.find("--logicalStreamName")->second.empty()) {
            sourceConfigMap.insert_or_assign("logicalStreamName", commandLineParams.find("--logicalStreamName")->second);
        }
        if (commandLineParams.find("--sourceFrequency") != commandLineParams.end()
            && !commandLineParams.find("--sourceFrequency")->second.empty()) {
            sourceConfigMap.insert_or_assign("sourceFrequency", commandLineParams.find("--sourceFrequency")->second);
        }
        if (commandLineParams.find("--rowLayout") != commandLineParams.end()
            && !commandLineParams.find("--rowLayout")->second.empty()) {
            sourceConfigMap.insert_or_assign("rowLayout", commandLineParams.find("--rowLayout")->second);
        }
        if (commandLineParams.find("--flushIntervalMS") != commandLineParams.end()
            && !commandLineParams.find("--flushIntervalMS")->second.empty()) {
            sourceConfigMap.insert_or_assign("flushIntervalMS", commandLineParams.find("--flushIntervalMS")->second);
        }
        if (commandLineParams.find("--inputFormat") != commandLineParams.end()
            && !commandLineParams.find("--inputFormat")->second.empty()) {
            sourceConfigMap.insert_or_assign("inputFormat", commandLineParams.find("--inputFormat")->second);
        }
        if (commandLineParams.find("--SenseSourceUdsf") != commandLineParams.end()
            && !commandLineParams.find("--SenseSourceUdsf")->second.empty()) {
            sourceConfigMap.insert_or_assign("SenseSourceUdsf", commandLineParams.find("--SenseSourceUdsf")->second);
        }
        if (commandLineParams.find("--CSVSourceFilePath") != commandLineParams.end()
            && !commandLineParams.find("--CSVSourceFilePath")->second.empty()) {
            sourceConfigMap.insert_or_assign("CSVSourceFilePath", commandLineParams.find("--CSVSourceFilePath")->second);
        }
        if (commandLineParams.find("--CSVSourceSkipHeader") != commandLineParams.end()
            && !commandLineParams.find("--CSVSourceSkipHeader")->second.empty()) {
            sourceConfigMap.insert_or_assign("CSVSourceSkipHeader", commandLineParams.find("--CSVSourceSkipHeader")->second);
        }
        if (commandLineParams.find("--MQTTSourceUrl") != commandLineParams.end()
            && !commandLineParams.find("--MQTTSourceUrl")->second.empty()) {
            sourceConfigMap.insert_or_assign("MQTTSourceUrl", commandLineParams.find("--MQTTSourceUrl")->second);
        }
        if (commandLineParams.find("--MQTTSourceClientId") != commandLineParams.end()
            && !commandLineParams.find("--MQTTSourceClientId")->second.empty()) {
            sourceConfigMap.insert_or_assign("MQTTSourceClientId", commandLineParams.find("--MQTTSourceClientId")->second);
        }
        if (commandLineParams.find("--MQTTSourceUserName") != commandLineParams.end()
            && !commandLineParams.find("--MQTTSourceUserName")->second.empty()) {
            sourceConfigMap.insert_or_assign("MQTTSourceUserName", commandLineParams.find("--MQTTSourceUserName")->second);
        }
        if (commandLineParams.find("--MQTTSourceTopic") != commandLineParams.end()
            && !commandLineParams.find("--MQTTSourceTopic")->second.empty()) {
            sourceConfigMap.insert_or_assign("MQTTSourceTopic", commandLineParams.find("--MQTTSourceTopic")->second);
        }
        if (commandLineParams.find("--MQTTSourceQos") != commandLineParams.end()
            && !commandLineParams.find("--MQTTSourceQos")->second.empty()) {
            sourceConfigMap.insert_or_assign("MQTTSourceQos", commandLineParams.find("--MQTTSourceQos")->second);
        }
        if (commandLineParams.find("--MQTTSourceCleanSession") != commandLineParams.end()
            && !commandLineParams.find("--MQTTSourceCleanSession")->second.empty()) {
            sourceConfigMap.insert_or_assign("MQTTSourceCleanSession",
                                             commandLineParams.find("--MQTTSourceCleanSession")->second);
        }
        if (commandLineParams.find("--inputFormat") != commandLineParams.end()
            && !commandLineParams.find("--inputFormat")->second.empty()) {
            sourceConfigMap.insert_or_assign("inputFormat", commandLineParams.find("--inputFormat")->second);
        }
        if (commandLineParams.find("--KafkaSourceGroupId") != commandLineParams.end()
            && !commandLineParams.find("--KafkaSourceGroupId")->second.empty()) {
            sourceConfigMap.insert_or_assign("KafkaSourceGroupId", commandLineParams.find("--KafkaSourceGroupId")->second);
        }
        if (commandLineParams.find("--KafkaSourceTopic") != commandLineParams.end()
            && !commandLineParams.find("--KafkaSourceTopic")->second.empty()) {
            sourceConfigMap.insert_or_assign("KafkaSourceTopic", commandLineParams.find("--KafkaSourceTopic")->second);
        }
        if (commandLineParams.find("--KafkaSourceConnectionTimeout") != commandLineParams.end()
            && !commandLineParams
                    .find("--"
                          "KafkaSourceConnectionTimeo"
                          "ut")
                    ->second.empty()) {
            sourceConfigMap.insert_or_assign("KafkaSourceConnectionTimeout",
                                             commandLineParams
                                                 .find("--"
                                                       "KafkaSourceConnectionTimeo"
                                                       "ut")
                                                 ->second);
        }
        if (commandLineParams.find("--MQTTSourceNamespaceIndex") != commandLineParams.end()
            && !commandLineParams
                    .find("--"
                          "MQTTSourceNamespaceInd"
                          "ex")
                    ->second.empty()) {
            sourceConfigMap.insert_or_assign("MQTTSourceNamespaceIndex",
                                             commandLineParams
                                                 .find("--"
                                                       "MQTTSourceNamespaceInd"
                                                       "ex")
                                                 ->second);
        }
        if (commandLineParams.find("--OPCSourceNodeIdentifier") != commandLineParams.end()
            && !commandLineParams
                    .find("--"
                          "OPCSourceNodeIdent"
                          "ifier")
                    ->second.empty()) {
            sourceConfigMap.insert_or_assign("OPCSourceNodeIdentifier",
                                             commandLineParams
                                                 .find("--"
                                                       "OPCSourceNodeIdent"
                                                       "ifier")
                                                 ->second);
        }
        if (commandLineParams.find("--MQTTSourceUserName") != commandLineParams.end()
            && !commandLineParams
                    .find("--"
                          "MQTTSourceUser"
                          "Name")
                    ->second.empty()) {
            sourceConfigMap.insert_or_assign("MQTTSourceUserName",
                                             commandLineParams
                                                 .find("--"
                                                       "MQTTSource"
                                                       "UserName")
                                                 ->second);
        }
        if (commandLineParams.find("--OPCSourcePassword") != commandLineParams.end()
            && !commandLineParams
                    .find("--"
                          "OPCSourceP"
                          "assword")
                    ->second.empty()) {
            sourceConfigMap.insert_or_assign("OPCSourcePasswor"
                                             "d",
                                             commandLineParams
                                                 .find("--"
                                                       "OPCSou"
                                                       "rcePas"
                                                       "sword")
                                                 ->second);
        }
    } catch (std::exception& e) {
        NES_ERROR("NesWorkerConfig: Error "
                  "while initializing "
                  "configuration parameters "
                  "from command line. "
                  << e.what());
        NES_WARNING("NesWorkerConfig: Keeping "
                    "default values for Source.");
    }
}

}// namespace NES