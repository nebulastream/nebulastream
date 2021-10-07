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

#ifndef NES_SOURCECONFIGFACTORY_HPP
#define NES_SOURCECONFIGFACTORY_HPP

#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfig.hpp>
#include <string>
#include <memory>
#include <map>

namespace NES {

enum ConfigSourceType {
    SenseSource,
    CSVSource,
    BinarySource,
    MQTTSource,
    KafkaSource,
    OPCSource, DefaultSource };

static std::map<std::string, ConfigSourceType> stringToConfigSourceType{
    {"SenseSource", SenseSource},
    {"CSVSource", CSVSource},
    {"BinarySource", BinarySource},
    {"MQTTSource", MQTTSource},
    {"KafkaSource", KafkaSource},
    {"OPCSource", OPCSource},
    {"NoSource", DefaultSource},
};

class SourceConfig;
using SourceConfigPtr = std::shared_ptr<SourceConfig>;

class SourceConfigFactory {
  public:
    static std::shared_ptr<SourceConfig> createSourceConfig(const std::map<std::string, std::string>& commandLineParams, int argc);
    static std::shared_ptr<SourceConfig> createSourceConfig();
    static std::map<std::string, std::string> readYAMLFile(const std::string& filePath);
    static std::map<std::string, std::string> overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& commandLineParams, std::map<std::string, std::string> configurationMap);

};

}

#endif//NES_SOURCECONFIGFACTORY_HPP
