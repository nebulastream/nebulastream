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

#include <Configurations/Sources/BinarySourceConfig.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Sources/DefaultSourceConfig.hpp>
#include <Configurations/Sources/KafkaSourceConfig.hpp>
#include <Configurations/Sources/MQTTSourceConfig.hpp>
#include <Configurations/Sources/OPCSourceConfig.hpp>
#include <Configurations/Sources/SenseSourceConfig.hpp>
#include <Configurations/Sources/MaterializedViewSourceConfig.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
#include <Util/Logger.hpp>
#define RYML_SINGLE_HDR_DEFINE_NOW
#include <Util/yaml/rapidyaml-0.3.0.hpp>
#include <filesystem>
#include "Util/yaml/Yaml.hpp"

namespace NES {

namespace Configurations {

SourceConfigPtr SourceConfigFactory::createSourceConfig(const std::map<std::string, std::string>& commandLineParams, int argc) {

    auto sourceConfigPath = commandLineParams.find("--" + SOURCE_CONFIG_PATH_CONFIG);
    std::map<std::string, std::string> configurationMap;
    std::map<std::string, std::list<std::string>> configMap;

    if (sourceConfigPath != commandLineParams.end()) {
        configMap = readYAMLFile(sourceConfigPath->second);
    }

    if (argc >= 1) {
        configurationMap = overwriteConfigWithCommandLineInput(commandLineParams, configurationMap);
    }

    if (!configurationMap.contains(SOURCE_TYPE_CONFIG)) {
        DefaultSourceConfigPtr noSource = DefaultSourceConfig::create();
        noSource->setSourceType(NO_SOURCE_CONFIG);
        return noSource;
    }

    switch (stringToConfigSourceType[configurationMap.at(SOURCE_TYPE_CONFIG)]) {
        case CSVSource: return CSVSourceConfig::create(configurationMap);
        case MQTTSource: return MQTTSourceConfig::create(configurationMap);
        case KafkaSource: return KafkaSourceConfig::create(configurationMap);
        case OPCSource: return OPCSourceConfig::create(configurationMap);
        case BinarySource: return BinarySourceConfig::create(configurationMap);
        case SenseSource: return SenseSourceConfig::create(configurationMap);
        case MaterializedViewSource: return Experimental::MaterializedView::MaterializedViewSourceConfig::create(configurationMap);
        case DefaultSource: return DefaultSourceConfig::create(configurationMap);
        default:
            NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " + configurationMap.at(SOURCE_TYPE_CONFIG)
                                    + " not supported");
    }
}

SourceConfigPtr SourceConfigFactory::createSourceConfig(std::string sourceType) {

    switch (stringToConfigSourceType[sourceType]) {
        case CSVSource: return CSVSourceConfig::create();
        case MQTTSource: return MQTTSourceConfig::create();
        case KafkaSource: return KafkaSourceConfig::create();
        case OPCSource: return OPCSourceConfig::create();
        case BinarySource: return BinarySourceConfig::create();
        case SenseSource: return SenseSourceConfig::create();
        case MaterializedViewSource: return Experimental::MaterializedView::MaterializedViewSourceConfig::create();
        case DefaultSource: return DefaultSourceConfig::create();
        default: return nullptr;
    }
}

template<class CharContainer>
size_t file_get_contents1(const char *filename, CharContainer *v)
{
    ::FILE *fp = ::fopen(filename, "rb");
    C4_CHECK_MSG(fp != nullptr, "could not open file");
    ::fseek(fp, 0, SEEK_END);
    long sz = ::ftell(fp);
    v->resize(static_cast<typename CharContainer::size_type>(sz));
    if(sz)
    {
        ::rewind(fp);
        size_t ret = ::fread(&(*v)[0], 1, v->size(), fp);
        C4_CHECK(ret == (size_t)sz);
    }
    ::fclose(fp);
    return v->size();
}

/** load a file from disk into an existing CharContainer */
template<class CharContainer>
CharContainer file_get_contents(const char *filename)
{
    CharContainer cc;
    file_get_contents1(filename, &cc);
    return cc;
}

SourceConfigPtr SourceConfigFactory::createSourceConfig() { return DefaultSourceConfig::create(); }

std::map<std::string, std::list<std::string>> SourceConfigFactory::readYAMLFile(const std::string& filePath) {

    std::map<std::string, std::list<std::string>> configurationMap;

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        NES_INFO("NesSourceConfigFactory: Using config file with path: " << filePath << " .");
        //Yaml::Node config;
        std::cout << "where are you failing" << std::endl;
        //Yaml::Parse(config, filePath.c_str());
        std::cout << "whats the output" << std::endl;

        std::string contents = file_get_contents<std::string>(filePath.c_str());
        ryml::Tree tree = ryml::parse(ryml::to_csubstr(contents));

        std::cout << tree.rootref().last_child().find_child("CSVSource").first_child().find_child("skipHeader") << std::endl;

        for(ryml::NodeRef const& child : tree.rootref().children()){
            std::cout << "When do you break" << std::endl;
            std::cout << child.key() << std::endl;
        }

        for(ryml::NodeRef const& child : tree.rootref().last_child().find_child("CSVSource").first_child().children()){
            std::cout << "When do you break" << std::endl;
            std::cout << child.key() << std::endl;
            std::cout << child.val() << std::endl;
        }


        /*std::cout << config[SOURCE_TYPE_CONFIG].As<std::string>() << std::endl;
        std::cout << config[SOURCE_TYPE_CONFIG][1].As<std::string>() << std::endl;
        for (auto it = config[1].Begin();  it != config[1].End() ; it++) {
            std::cout << (*it).first << ": " << (*it).second.As<std::string>() << std::endl;
        }
        for (auto it = config[SOURCE_TYPE_CONFIG][CSV_SOURCE_CONFIG][1].Begin();  it != config[SOURCE_TYPE_CONFIG][CSV_SOURCE_CONFIG][1].End() ; it++) {
            std::cout << (*it).first << ": " << (*it).second.As<std::string>() << std::endl;
        }
        std::cout << config[SOURCE_TYPE_CONFIG][CSV_SOURCE_CONFIG].As<std::string>() << std::endl;
        std::cout << config[SOURCE_TYPE_CONFIG].PushFront().As<std::string>() << std::endl;
        std::cout << "does something happen?" << std::endl;
        for (auto it = config[SOURCE_TYPE_CONFIG][3].Begin();  it != config[SOURCE_TYPE_CONFIG][3].End() ; it++) {
            std::cout << (*it).first << ": " << (*it).second.As<std::string>() << std::endl;
        }*/
        try {
            /*if (!config[NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<std::string>().empty()
                && config[NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                                        config[NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<std::string>()));
            }
            if (!config[NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG].As<std::string>().empty()
                && config[NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                        config[NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG].As<std::string>()));
            }
            if (!config[PHYSICAL_STREAM_NAME_CONFIG].As<std::string>().empty()
                && config[PHYSICAL_STREAM_NAME_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(PHYSICAL_STREAM_NAME_CONFIG,
                                                        config[PHYSICAL_STREAM_NAME_CONFIG].As<std::string>()));
            }
            if (!config[SOURCE_FREQUENCY_CONFIG].As<std::string>().empty()
                && config[SOURCE_FREQUENCY_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(std::pair<std::string, std::string>(SOURCE_FREQUENCY_CONFIG,
                                                                            config[SOURCE_FREQUENCY_CONFIG].As<std::string>()));
            }
            if (!config[LOGICAL_STREAM_NAME_CONFIG].As<std::string>().empty()
                && config[LOGICAL_STREAM_NAME_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(LOGICAL_STREAM_NAME_CONFIG,
                                                        config[LOGICAL_STREAM_NAME_CONFIG].As<std::string>()));
            }
            if (!config[INPUT_FORMAT_CONFIG].As<std::string>().empty() && config[INPUT_FORMAT_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(INPUT_FORMAT_CONFIG, config[INPUT_FORMAT_CONFIG].As<std::string>()));
            }
            if (!config[SOURCE_TYPE_CONFIG].As<std::list<std::string>>().empty()) {
                for (auto const &i: config[SOURCE_TYPE_CONFIG].As<std::list<std::string>>()){
                    std::cout << i << std::endl;
                }
                configurationMap.insert(
                    std::pair<std::string, std::list<std::string>>(SOURCE_TYPE_CONFIG, config[SOURCE_TYPE_CONFIG].As<std::list<std::string>>()));
            }
            if (!config[SENSE_SOURCE_CONFIG][0][UDFS_CONFIG].As<std::string>().empty()
                && config[SENSE_SOURCE_CONFIG][0][UDFS_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::list<std::string>>(SENSE_SOURCE_CONFIG,
                                                        config[SENSE_SOURCE_CONFIG].As<std::list<std::string>>()));
            }
            if (!config[CSV_SOURCE_CONFIG][0][FILE_PATH_CONFIG].As<std::string>().empty()
                && config[CSV_SOURCE_CONFIG][0][FILE_PATH_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(CSV_SOURCE_FILE_PATH_CONFIG,
                                                        config[CSV_SOURCE_CONFIG][0][FILE_PATH_CONFIG].As<std::string>()));
            }
            if (!config[CSV_SOURCE_CONFIG][0][SKIP_HEADER_CONFIG].As<std::string>().empty()
                && config[CSV_SOURCE_CONFIG][0][SKIP_HEADER_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(CSV_SOURCE_SKIP_HEADER_CONFIG,
                                                        config[CSV_SOURCE_CONFIG][0][SKIP_HEADER_CONFIG].As<std::string>()));
            }
            if (!config[CSV_SOURCE_CONFIG][0][DELIMITER_CONFIG].As<std::string>().empty()
                && config[CSV_SOURCE_CONFIG][0][DELIMITER_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(CSV_SOURCE_DELIMITER_CONFIG,
                                                        config[CSV_SOURCE_CONFIG][0][DELIMITER_CONFIG].As<std::string>()));
            }
            if (!config[BINARY_SOURCE_CONFIG][0][FILE_PATH_CONFIG].As<std::string>().empty()
                && config[BINARY_SOURCE_CONFIG][0][FILE_PATH_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(BINARY_SOURCE_FILE_PATH_CONFIG,
                                                        config[BINARY_SOURCE_CONFIG][0][FILE_PATH_CONFIG].As<std::string>()));
            }
            if (!config[MQTT_SOURCE_CONFIG][0][URL_CONFIG].As<std::string>().empty()
                && config[MQTT_SOURCE_CONFIG][0][URL_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(MQTT_SOURCE_URL_CONFIG,
                                                        config[MQTT_SOURCE_CONFIG][0][URL_CONFIG].As<std::string>()));
            }
            if (!config[MQTT_SOURCE_CONFIG][0][CLIENT_ID_CONFIG].As<std::string>().empty()
                && config[MQTT_SOURCE_CONFIG][0][CLIENT_ID_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(MQTT_SOURCE_CLIENT_ID_CONFIG,
                                                        config[MQTT_SOURCE_CONFIG][0][CLIENT_ID_CONFIG].As<std::string>()));
            }
            if (!config[MQTT_SOURCE_CONFIG][0][USER_NAME_CONFIG].As<std::string>().empty()
                && config[MQTT_SOURCE_CONFIG][0][USER_NAME_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(MQTT_SOURCE_USER_NAME_CONFIG,
                                                        config[MQTT_SOURCE_CONFIG][0][USER_NAME_CONFIG].As<std::string>()));
            }
            if (!config[MQTT_SOURCE_CONFIG][0][TOPIC_CONFIG].As<std::string>().empty()
                && config[MQTT_SOURCE_CONFIG][0][TOPIC_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(MQTT_SOURCE_TOPIC_CONFIG,
                                                        config[MQTT_SOURCE_CONFIG][0][TOPIC_CONFIG].As<std::string>()));
            }
            if (!config[MQTT_SOURCE_CONFIG][0][QOS_CONFIG].As<std::string>().empty()
                && config[MQTT_SOURCE_CONFIG][0][QOS_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(MQTT_SOURCE_QOS_CONFIG,
                                                        config[MQTT_SOURCE_CONFIG][0][QOS_CONFIG].As<std::string>()));
            }
            if (!config[MQTT_SOURCE_CONFIG][0][CLEAN_SESSION_CONFIG].As<std::string>().empty()
                && config[MQTT_SOURCE_CONFIG][0][CLEAN_SESSION_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(MQTT_SOURCE_CLEAN_SESSION_CONFIG,
                                                        config[MQTT_SOURCE_CONFIG][0][CLEAN_SESSION_CONFIG].As<std::string>()));
            }
            if (!config[MQTT_SOURCE_CONFIG][0][FLUSH_INTERVAL_MS_CONFIG].As<std::string>().empty()
                && config[MQTT_SOURCE_CONFIG][0][FLUSH_INTERVAL_MS_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(std::pair<std::string, std::string>(
                    MQTT_SOURCE_FLUSH_INTERVAL_MS_CONFIG,
                    config[MQTT_SOURCE_CONFIG][0][FLUSH_INTERVAL_MS_CONFIG].As<std::string>()));
            }
            if (!config[KAFKA_SOURCE_CONFIG][0][BROKERS_CONFIG].As<std::string>().empty()
                && config[KAFKA_SOURCE_CONFIG][0][BROKERS_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(KAFKA_SOURCE_BROKERS_CONFIG,
                                                        config[KAFKA_SOURCE_CONFIG][0][BROKERS_CONFIG].As<std::string>()));
            }
            if (!config[KAFKA_SOURCE_CONFIG][0][AUTO_COMMIT].As<std::string>().empty()
                && config[KAFKA_SOURCE_CONFIG][0][AUTO_COMMIT].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(KAFKA_SOURCE_AUTO_COMMIT_CONFIG,
                                                        config[KAFKA_SOURCE_CONFIG][0][AUTO_COMMIT].As<std::string>()));
            }
            if (!config[KAFKA_SOURCE_CONFIG][0][GROUP_ID_CONFIG].As<std::string>().empty()
                && config[KAFKA_SOURCE_CONFIG][0][GROUP_ID_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(KAFKA_SOURCE_GROUP_ID_CONFIG,
                                                        config[KAFKA_SOURCE_CONFIG][0][GROUP_ID_CONFIG].As<std::string>()));
            }
            if (!config[KAFKA_SOURCE_CONFIG][0][TOPIC_CONFIG].As<std::string>().empty()
                && config[KAFKA_SOURCE_CONFIG][0][TOPIC_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(KAFKA_SOURCE_TOPIC_CONFIG,
                                                        config[KAFKA_SOURCE_CONFIG][0][TOPIC_CONFIG].As<std::string>()));
            }
            if (!config[KAFKA_SOURCE_CONFIG][0][CONNECTION_TIMEOUT_CONFIG].As<std::string>().empty()
                && config[KAFKA_SOURCE_CONFIG][0][CONNECTION_TIMEOUT_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(std::pair<std::string, std::string>(
                    KAFKA_SOURCE_CONNECTION_TIMEOUT_CONFIG,
                    config[KAFKA_SOURCE_CONFIG][0][CONNECTION_TIMEOUT_CONFIG].As<std::string>()));
            }
            if (!config[OPC_SOURCE_CONFIG][0][NAME_SPACE_INDEX_CONFIG].As<std::string>().empty()
                && config[OPC_SOURCE_CONFIG][0][NAME_SPACE_INDEX_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(OPC_SOURCE_NAME_SPACE_INDEX_CONFIG,
                                                        config[OPC_SOURCE_CONFIG][0][NAME_SPACE_INDEX_CONFIG].As<std::string>()));
            }
            if (!config[OPC_SOURCE_CONFIG][0][NODE_IDENTIFIER_CONFIG].As<std::string>().empty()
                && config[OPC_SOURCE_CONFIG][0][NODE_IDENTIFIER_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(OPC_SOURCE_NODE_IDENTIFIER_CONFIG,
                                                        config[OPC_SOURCE_CONFIG][0][NODE_IDENTIFIER_CONFIG].As<std::string>()));
            }
            if (!config[OPC_SOURCE_CONFIG][0][USER_NAME_CONFIG].As<std::string>().empty()
                && config[OPC_SOURCE_CONFIG][0][USER_NAME_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(OPC_SOURCE_USERNAME_CONFIG,
                                                        config[OPC_SOURCE_CONFIG][0][USER_NAME_CONFIG].As<std::string>()));
            }
            if (!config[OPC_SOURCE_CONFIG][0][PASSWORD_CONFIG].As<std::string>().empty()
                && config[OPC_SOURCE_CONFIG][0][PASSWORD_CONFIG].As<std::string>() != "\n") {
                configurationMap.insert(
                    std::pair<std::string, std::string>(OPC_SOURCE_PASSWORD_CONFIG,
                                                        config[OPC_SOURCE_CONFIG][0][PASSWORD_CONFIG].As<std::string>()));
            }*/
        } catch (std::exception& e) {
            NES_ERROR("NesSourceConfigFactory: Error while initializing configuration parameters from XAML file.");
            NES_WARNING("NesSourceConfigFactory: Keeping default values.");
        }
    } else {
        NES_ERROR("NesSourceConfigFactory: No file path was provided or file could not be found at " << filePath << ".");
        NES_WARNING("NesSourceConfigFactory: Keeping default values for Source Config.");
    }

    return configurationMap;
}
std::map<std::string, std::string>
SourceConfigFactory::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& commandLineParams,
                                                         std::map<std::string, std::string> configurationMap) {

    try {
        if (commandLineParams.find("--" + SOURCE_FREQUENCY_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + SOURCE_FREQUENCY_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(SOURCE_FREQUENCY_CONFIG,
                                              commandLineParams.find("--" + SOURCE_FREQUENCY_CONFIG)->second);
        }
        if (commandLineParams.find("--" + NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                              commandLineParams.find("--" + NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second);
        }
        if (commandLineParams.find("--" + NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(
                NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                commandLineParams.find("--" + NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)->second);
        }
        if (commandLineParams.find("--" + PHYSICAL_STREAM_NAME_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + PHYSICAL_STREAM_NAME_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(PHYSICAL_STREAM_NAME_CONFIG,
                                              commandLineParams.find("--" + PHYSICAL_STREAM_NAME_CONFIG)->second);
        }
        if (commandLineParams.find("--" + LOGICAL_STREAM_NAME_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + LOGICAL_STREAM_NAME_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(LOGICAL_STREAM_NAME_CONFIG,
                                              commandLineParams.find("--" + LOGICAL_STREAM_NAME_CONFIG)->second);
        }
        if (commandLineParams.find("--" + SOURCE_TYPE_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + SOURCE_TYPE_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(SOURCE_TYPE_CONFIG, commandLineParams.find("--" + SOURCE_TYPE_CONFIG)->second);
        }
        if (commandLineParams.find("--" + INPUT_FORMAT_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + INPUT_FORMAT_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(INPUT_FORMAT_CONFIG, commandLineParams.find("--" + INPUT_FORMAT_CONFIG)->second);
        }
        if (commandLineParams.find("--" + UDFS_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + UDFS_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(UDFS_CONFIG,
                                              commandLineParams.find("--" + UDFS_CONFIG)->second);
        }
        if (commandLineParams.find("--" + FILE_PATH_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + FILE_PATH_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(FILE_PATH_CONFIG,
                                              commandLineParams.find("--" + FILE_PATH_CONFIG)->second);
        }
        if (commandLineParams.find("--" + SKIP_HEADER_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + SKIP_HEADER_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(SKIP_HEADER_CONFIG,
                                              commandLineParams.find("--" + SKIP_HEADER_CONFIG)->second);
        }
        if (commandLineParams.find("--" + DELIMITER_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + DELIMITER_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(DELIMITER_CONFIG,
                                              commandLineParams.find("--" + DELIMITER_CONFIG)->second);
        }
        if (commandLineParams.find("--" + URL_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + URL_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(URL_CONFIG,
                                              commandLineParams.find("--" + URL_CONFIG)->second);
        }
        if (commandLineParams.find("--" + CLIENT_ID_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + CLIENT_ID_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(CLIENT_ID_CONFIG,
                                              commandLineParams.find("--" + CLIENT_ID_CONFIG)->second);
        }
        if (commandLineParams.find("--" + USER_NAME_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + USER_NAME_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(USER_NAME_CONFIG,
                                              commandLineParams.find("--" + USER_NAME_CONFIG)->second);
        }
        if (commandLineParams.find("--" + TOPIC_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + TOPIC_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(TOPIC_CONFIG,
                                              commandLineParams.find("--" + TOPIC_CONFIG)->second);
        }
        if (commandLineParams.find("--" + QOS_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + QOS_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(QOS_CONFIG,
                                              commandLineParams.find("--" + QOS_CONFIG)->second);
        }
        if (commandLineParams.find("--" + CLEAN_SESSION_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + CLEAN_SESSION_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(CLEAN_SESSION_CONFIG,
                                              commandLineParams.find("--" + CLEAN_SESSION_CONFIG)->second);
        }
        if (commandLineParams.find("--" + FLUSH_INTERVAL_MS_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + FLUSH_INTERVAL_MS_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(FLUSH_INTERVAL_MS_CONFIG,
                                              commandLineParams.find("--" + FLUSH_INTERVAL_MS_CONFIG)->second);
        }
        if (commandLineParams.find("--" + BROKERS_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + BROKERS_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(BROKERS_CONFIG,
                                              commandLineParams.find("--" + BROKERS_CONFIG)->second);
        }
        if (commandLineParams.find("--" + AUTO_COMMIT_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + AUTO_COMMIT_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(AUTO_COMMIT_CONFIG,
                                              commandLineParams.find("--" + AUTO_COMMIT_CONFIG)->second);
        }
        if (commandLineParams.find("--" + GROUP_ID_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + GROUP_ID_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(GROUP_ID_CONFIG,
                                              commandLineParams.find("--" + GROUP_ID_CONFIG)->second);
        }
        if (commandLineParams.find("--" + TOPIC_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + TOPIC_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(TOPIC_CONFIG,
                                              commandLineParams.find("--" + TOPIC_CONFIG)->second);
        }
        if (commandLineParams.find("--" + CONNECTION_TIMEOUT_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + CONNECTION_TIMEOUT_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(CONNECTION_TIMEOUT_CONFIG,
                                              commandLineParams.find("--" + CONNECTION_TIMEOUT_CONFIG)->second);
        }
        if (commandLineParams.find("--" + NODE_IDENTIFIER_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + NODE_IDENTIFIER_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(NODE_IDENTIFIER_CONFIG,
                                              commandLineParams.find("--" + NODE_IDENTIFIER_CONFIG)->second);
        }
        if (commandLineParams.find("--" + NAME_SPACE_INDEX_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + NAME_SPACE_INDEX_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(NAME_SPACE_INDEX_CONFIG,
                                              commandLineParams.find("--" + NAME_SPACE_INDEX_CONFIG)->second);
        }
        if (commandLineParams.find("--" + USER_NAME_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + USER_NAME_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(USER_NAME_CONFIG,
                                              commandLineParams.find("--" + USER_NAME_CONFIG)->second);
        }
        if (commandLineParams.find("--" + PASSWORD_CONFIG) != commandLineParams.end()
            && !commandLineParams.find("--" + PASSWORD_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(PASSWORD_CONFIG,
                                              commandLineParams.find("--" + PASSWORD_CONFIG)->second);
        }
        if (commandLineParams.find("--" + MATERIALIZED_VIEW_ID_CONFIG) != commandLineParams.end()
        && !commandLineParams.find("--" + MATERIALIZED_VIEW_ID_CONFIG)->second.empty()) {
            configurationMap.insert_or_assign(MATERIALIZED_VIEW_ID_CONFIG,
                                              commandLineParams.find("--" + MATERIALIZED_VIEW_ID_CONFIG)->second);
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

    return configurationMap;
}

}// namespace Configurations
}// namespace NES