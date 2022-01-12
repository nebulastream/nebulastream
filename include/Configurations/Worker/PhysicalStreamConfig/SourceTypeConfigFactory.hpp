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

#ifndef NES_SOURCETYPECONFIGFACTORY_HPP
#define NES_SOURCETYPECONFIGFACTORY_HPP

#include <Configurations/Worker/PhysicalStreamConfig/PhysicalStreamConfig.hpp>
#include <map>
#include <memory>
#include <string>
#include <list>

namespace NES {

namespace Configurations {

/**
 * enum with config objects
 */
enum ConfigSourceType { SenseSource, CSVSource, BinarySource, MQTTSource, KafkaSource, OPCSource, DefaultSource };

/**
 * enum string mapping for source config factory
 */
static std::map<std::string, ConfigSourceType> stringToConfigSourceType{{SENSE_SOURCE_CONFIG, SenseSource},
                                                                        {CSV_SOURCE_CONFIG, CSVSource},
                                                                        {BINARY_SOURCE_CONFIG, BinarySource},
                                                                        {MQTT_SOURCE_CONFIG, MQTTSource},
                                                                        {KAFKA_SOURCE_CONFIG, KafkaSource},
                                                                        {OPC_SOURCE_CONFIG, OPCSource},
                                                                        {DEFAULT_SOURCE_CONFIG, DefaultSource},
                                                                        {NO_SOURCE_CONFIG, DefaultSource}};

class SourceTypeConfigFactory {
  public:
    /**
     * @brief create source config with yaml file and/or command line params
     * @param commandLineParams command line params
     * @param argc number of command line params
     * @return source config object
     */
    static SourceTypeConfigPtr createSourceConfig(const std::map<std::string, std::string>& commandLineParams);

    /**
     * @brief create physical stream config with yaml file
     * @param physicalStreamConfig yaml elements from yaml file
     * @return physical stream config object
     */
    static SourceTypeConfigPtr createSourceConfig(ryml::NodeRef sourceTypeConfigNode, std::string sourceType);

    /**
     * @brief create default source config with default values of type sourceType
     * @param sourceType source type of source config object
     * @return source config object of type sourceType
     */
    static SourceTypeConfigPtr createSourceConfig(std::string _sourceType);

    /**
     * @brief create default source config
     * @return default source config object
     */
    static SourceTypeConfigPtr createSourceConfig();

  private:
    /**
     * @brief read YAML file for configurations
     * @param filePath path to yaml configuration file
     * @return configuration map with yaml file configs
     */
    static std::map<std::string, std::list<std::string>> readYAMLFile(const std::string& filePath);

    /**
     * @brief overwrites configurations with command line input params
     * @param commandLineParams
     * @param configurationMap map with current configurations from yaml file
     * @return map with configurations, overwritten if command line configs change yaml file configs
     */
    static std::map<std::string, std::string>
    overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& commandLineParams,
                                        std::map<std::string, std::string> configurationMap);
};
}// namespace Configurations
}// namespace NES

#endif//NES_SOURCETYPECONFIGFACTORY_HPP
