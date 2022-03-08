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

#include <API/Schema.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Coordinator/LogicalSourceFactory.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

namespace Configurations {

LogicalSourcePtr LogicalSourceFactory::createFromString(std::string ,
                                            std::map<std::string, std::string>& commandLineParams) {
    std::string logicalSourceName, fieldNodeName,
        fieldNodeType, fieldNodeNesType, fieldNodeLength;
    for (auto parameter : commandLineParams) {
        if (parameter.first == LOGICAL_SOURCE_SCHEMA_FIELDS_CONFIG && !parameter.second.empty()) {
            logicalSourceName = parameter.second;
        }

        if (parameter.first == LOGICAL_SOURCE_SCHEMA_FIELDS_CONFIG && !parameter.second.empty()) {
            // TODO: add issue for CLI parsing of array of values, currently we support only yaml
        }
    }

    if (logicalSourceName.empty()) {
        NES_WARNING("No logical source name is supplied for creating the logical source. Please supply "
                    "logical source name using --"
                    << LOGICAL_SOURCE_NAME_CONFIG);
        return nullptr;
    }

    return LogicalSource::create(logicalSourceName, Schema::create());
}

LogicalSourcePtr LogicalSourceFactory::createFromYaml(Yaml::Node& yamlConfig) {
    std::vector<LogicalSourcePtr> logicalSources;
    std::string logicalSourceName;
    SchemaPtr schema = Schema::create();

    if (!yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>().empty()
        && yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>() != "\n") {
        logicalSourceName = yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>();
    }else {
        NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Logical Source Name.");
    }

    // TODO: why do begin/end/++ iterator APIs of Yaml::Node not work (or why do they work as they are)?
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

            schema->addField(fieldNodeName, stringToFieldType(fieldNodeType, fieldNodeLength));
        }
    } else {
        NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Logical Source Schema Fields.");
    }

    return LogicalSource::create(logicalSourceName, schema);
}

// TODO: ask in review if this can be moved elsewhere
DataTypePtr LogicalSourceFactory::stringToFieldType(std::string fieldNodeType,
                                                    std::string fieldNodeLength) {
    if (fieldNodeType == "string") {
        if (fieldNodeLength.empty() || fieldNodeLength == "\n") {
            NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Schema Field Type.");
        }
        return DataTypeFactory::createFixedChar(std::stoi(fieldNodeLength));
    }

    if (fieldNodeType == "boolean") {
        return DataTypeFactory::createBoolean();
    }

    if (fieldNodeType == "int8") {
        return DataTypeFactory::createInt8();
    }

    if (fieldNodeType == "uint8") {
        return DataTypeFactory::createUInt8();
    }

    if (fieldNodeType == "int16") {
        return DataTypeFactory::createInt16();
    }

    if (fieldNodeType == "uint16") {
        return DataTypeFactory::createUInt16();
    }

    if (fieldNodeType == "int32") {
        return DataTypeFactory::createInt32();
    }

    if (fieldNodeType == "uint32") {
        return DataTypeFactory::createUInt32();
    }

    if (fieldNodeType == "int64") {
        return DataTypeFactory::createInt64();
    }

    if (fieldNodeType == "uint64") {
        return DataTypeFactory::createUInt64();
    }

    if (fieldNodeType == "float") {
        return DataTypeFactory::createFloat();
    }

    if (fieldNodeType == "double") {
        return DataTypeFactory::createDouble();
    }

    NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. " << fieldNodeType << " is not a proper Schema Field Type.");
}

}// namespace Configurations
}// namespace NES