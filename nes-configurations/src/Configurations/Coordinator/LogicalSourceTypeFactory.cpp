/*
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
#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <Configurations/Coordinator/LogicalSourceTypeFactory.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Configurations {

LogicalSourceTypePtr LogicalSourceTypeFactory::createFromString(std::string,
                                                                std::map<std::string, std::string>& commandLineParams) {
    std::string logicalSourceName, fieldNodeName, fieldNodeType, fieldNodeNesType, fieldNodeLength;
    std::vector<SchemaFieldDetail> schemaFieldDetails;
    for (const auto& parameter : commandLineParams) {
        if (parameter.first == LOGICAL_SOURCE_SCHEMA_FIELDS_CONFIG && !parameter.second.empty()) {
            logicalSourceName = parameter.second;
        }

        if (parameter.first == LOGICAL_SOURCE_SCHEMA_FIELDS_CONFIG && !parameter.second.empty()) {
            // TODO: add issue for CLI parsing of array of values, currently we support only yaml
        }
    }

    if (logicalSourceName.empty()) {
        NES_WARNING("No logical source name is supplied for creating the logical source. Please supply "
                    "logical source name using --{}",
                    LOGICAL_SOURCE_NAME_CONFIG);
        return nullptr;
    }

    return LogicalSourceType::create(logicalSourceName, SchemaType::create(schemaFieldDetails));
}

LogicalSourceTypePtr LogicalSourceTypeFactory::createFromYaml(Yaml::Node& yamlConfig) {
    std::vector<LogicalSourceTypePtr> logicalSourceTypes;
    std::string logicalSourceName;

    std::vector<SchemaFieldDetail> schemaFieldDetails;

    if (!yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>().empty()
        && yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>() != "\n") {
        logicalSourceName = yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>();
    } else {
        NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Logical Source Name.");
    }

    if (yamlConfig[LOGICAL_SOURCE_SCHEMA_FIELDS_CONFIG].IsSequence()) {
        auto sequenceSize = yamlConfig[LOGICAL_SOURCE_SCHEMA_FIELDS_CONFIG].Size();
        for (uint64_t index = 0; index < sequenceSize; ++index) {
            auto currentFieldNode = yamlConfig[LOGICAL_SOURCE_SCHEMA_FIELDS_CONFIG][index];

            auto fieldNodeName = currentFieldNode[LOGICAL_SOURCE_SCHEMA_FIELD_NAME_CONFIG].As<std::string>();
            if (fieldNodeName.empty() || fieldNodeName == "\n") {
                NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Schema Field Name.");
            }

            auto fieldNodeType = currentFieldNode[LOGICAL_SOURCE_SCHEMA_FIELD_TYPE_CONFIG].As<std::string>();
            if (fieldNodeType.empty() || fieldNodeType == "\n") {
                NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Schema Field Type.");
            }

            auto fieldNodeLength = currentFieldNode[LOGICAL_SOURCE_SCHEMA_FIELD_TYPE_LENGTH].As<std::string>();

            schemaFieldDetails.emplace_back(fieldNodeName, fieldNodeType, fieldNodeLength);
        }
    } else {
        NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Logical Source Schema Fields.");
    }

    auto schemaType = SchemaType::create(schemaFieldDetails);
    return LogicalSourceType::create(logicalSourceName, schemaType);
}

}// namespace NES::Configurations