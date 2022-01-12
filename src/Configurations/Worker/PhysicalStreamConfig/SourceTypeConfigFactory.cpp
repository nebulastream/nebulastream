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

#include <Configurations/Worker/PhysicalStreamConfig/BinarySourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/CSVSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/DefaultSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/KafkaSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/MQTTSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/OPCSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/SenseSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfigFactory.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/rapidyaml.hpp>

namespace NES {

namespace Configurations {

SourceTypeConfigPtr
SourceTypeConfigFactory::createSourceConfig(const std::map<std::string, std::string>& commandLineParams) {

    if (!commandLineParams.contains(SOURCE_TYPE_CONFIG)) {
        DefaultSourceTypeConfigPtr noSource = DefaultSourceTypeConfig::create();
        noSource->setSourceType(NO_SOURCE_CONFIG);
        return noSource;
    }

    switch (stringToConfigSourceType[commandLineParams.at(SOURCE_TYPE_CONFIG)]) {
        case CSVSource: return CSVSourceTypeConfig::create(commandLineParams);
        case MQTTSource: return MQTTSourceTypeConfig::create(commandLineParams);
        case KafkaSource: return KafkaSourceTypeConfig::create(commandLineParams);
        case OPCSource: return OPCSourceTypeConfig::create(commandLineParams);
        case BinarySource: return BinarySourceTypeConfig::create(commandLineParams);
        case SenseSource: return SenseSourceTypeConfig::create(commandLineParams);
        case DefaultSource: return DefaultSourceTypeConfig::create(commandLineParams);
        default:
            NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " + commandLineParams.at(SOURCE_TYPE_CONFIG)
                                    + " not supported");
    }
}

SourceTypeConfigPtr SourceTypeConfigFactory::createSourceConfig(ryml::NodeRef sourceTypeConfigNode, std::string sourceType) {

    switch (stringToConfigSourceType[sourceType]) {
        case CSVSource: return CSVSourceTypeConfig::create(sourceTypeConfigNode);
        case MQTTSource: return MQTTSourceTypeConfig::create(sourceTypeConfigNode);
        case KafkaSource: return KafkaSourceTypeConfig::create(sourceTypeConfigNode);
        case OPCSource: return OPCSourceTypeConfig::create(sourceTypeConfigNode);
        case BinarySource: return BinarySourceTypeConfig::create(sourceTypeConfigNode);
        case SenseSource: return SenseSourceTypeConfig::create(sourceTypeConfigNode);
        case DefaultSource: return DefaultSourceTypeConfig::create(sourceTypeConfigNode);
        default:
            return DefaultSourceTypeConfig::create(sourceTypeConfigNode);
            NES_INFO("PhysicalStreamTypeConfig:: no source provided will create physical stream with default source");
    }
}

SourceTypeConfigPtr SourceTypeConfigFactory::createSourceConfig(std::string _sourceType) {

    switch (stringToConfigSourceType[_sourceType]) {
        case CSVSource: return CSVSourceTypeConfig::create();
        case MQTTSource: return MQTTSourceTypeConfig::create();
        case KafkaSource: return KafkaSourceTypeConfig::create();
        case OPCSource: return OPCSourceTypeConfig::create();
        case BinarySource: return BinarySourceTypeConfig::create();
        case SenseSource: return SenseSourceTypeConfig::create();
        case DefaultSource: return DefaultSourceTypeConfig::create();
        default: return nullptr;
    }
}

SourceTypeConfigPtr SourceTypeConfigFactory::createSourceConfig() { return DefaultSourceTypeConfig::create(); }

}// namespace Configurations
}// namespace NES