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

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/PhysicalStreamTypeConfiguration.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfigFactory.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

PhysicalStreamTypeConfigurationPtr PhysicalStreamTypeConfiguration::create(ryml::NodeRef physicalStreamConfigNode) {
    return std::make_shared<PhysicalStreamTypeConfiguration>(PhysicalStreamTypeConfiguration(physicalStreamConfigNode));
}

PhysicalStreamTypeConfiguration::PhysicalStreamTypeConfiguration(ryml::NodeRef physicalStreamConfigNode){
    NES_INFO("NesSourceConfig: Init physical stream config object with new values.");

    std::string sourceType = physicalStreamConfigNode.find_child(ryml::to_csubstr(SOURCE_TYPE_CONFIG)).val().str;

    sourceTypeConfig =
        SourceTypeConfigFactory::createSourceConfig(physicalStreamConfigNode.find_child(ryml::to_csubstr(sourceType)),
                                                    sourceType);
}

PhysicalStreamTypeConfigurationPtr
PhysicalStreamTypeConfiguration::create(const std::map<std::string, std::string>& inputParams) {
    return std::make_shared<PhysicalStreamTypeConfiguration>(PhysicalStreamTypeConfiguration(inputParams));
}

PhysicalStreamTypeConfiguration::PhysicalStreamTypeConfiguration(const std::map<std::string, std::string>& inputParams) {
    NES_INFO("NesSourceConfig: Init physical stream config object with new values.");

    if (inputParams.find("--" + PHYSICAL_STREAM_NAME_CONFIG) != inputParams.end()
        && !inputParams.find("--" + PHYSICAL_STREAM_NAME_CONFIG)->second.empty()) {
        physicalStreamName->setValue(inputParams.find("--" + PHYSICAL_STREAM_NAME_CONFIG)->second);
    }
    if (inputParams.find("--" + LOGICAL_STREAM_NAME_CONFIG) != inputParams.end()
        && !inputParams.find("--" + LOGICAL_STREAM_NAME_CONFIG)->second.empty()) {
        physicalStreamName->setValue(inputParams.find("--" + LOGICAL_STREAM_NAME_CONFIG)->second);
    }

    sourceTypeConfig = SourceTypeConfigFactory::createSourceConfig(inputParams);
}

PhysicalStreamTypeConfigurationPtr PhysicalStreamTypeConfiguration::create(std::string sourceType) {
    return std::make_shared<PhysicalStreamTypeConfiguration>(PhysicalStreamTypeConfiguration(sourceType));
}

PhysicalStreamTypeConfiguration::PhysicalStreamTypeConfiguration(std::string sourceType) {
    NES_INFO("NesSourceConfig: Init physical stream config object with new values.");

    sourceTypeConfig = SourceTypeConfigFactory::createSourceConfig(sourceType);
}

}// namespace Configurations
}// namespace NES