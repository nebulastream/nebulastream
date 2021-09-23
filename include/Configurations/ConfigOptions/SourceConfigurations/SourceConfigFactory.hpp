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
    OPCSource
};

static std::map<std::string, ConfigSourceType> stringToConfigSourceType{
    {"SenseSource", SenseSource},
    {"CSVSource", CSVSource},
    {"BinarySource", BinarySource},
    {"MQTTSource", MQTTSource},
    {"KafkaSource", KafkaSource},
    {"OPCSource", OPCSource},
};

class SourceConfigFactory {
  public:
    static std::unique_ptr<SourceConfig> createSourceConfig(std::map<std::string, std::string> commandLineParams, int argc);
    static void readYAMLFile(const std::string& filePath);
    static void overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& commandLineParams);


  private:
    static std::map<std::string, std::string> sourceConfigMap;
};

}

#endif//NES_SOURCECONFIGFACTORY_HPP
