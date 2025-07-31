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

#include <JsonOutputFormatter.hpp>

#include <map>
#include <ranges>
#include <string>
#include <variant>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/NESStrongTypeJson.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/SourceDescriptor.hpp>
#include <google/protobuf/message_lite.h>
#include <magic_enum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>

namespace NES
{

void to_json(nlohmann::json& jsonOutput, const ParserConfig& parserConfig)
{
    jsonOutput = nlohmann::json{
        {"type", parserConfig.parserType},
        {"field_delimiter", parserConfig.fieldDelimiter},
        {"tuple_delimiter", parserConfig.tupleDelimiter}};
}

void to_json(nlohmann::json& jsonOutput, const DataType& dataType)
{
    jsonOutput = magic_enum::enum_name(dataType.type);
}

void to_json(nlohmann::json& jsonOutput, const Identifier& identifier)
{
    jsonOutput = identifier;
}

void to_json(nlohmann::json& jsonOutput, const UnboundField& str)
{
    jsonOutput = nlohmann::json{{"name", str.getFullyQualifiedName()}, {"type", str.getDataType()}};
}

void to_json(nlohmann::json& jsonOutput, const UnboundOrderedSchema& schema)
{
    jsonOutput = nlohmann::json{std::vector{schema}};
}

void to_json(nlohmann::json& jsonOutput, const google::protobuf::MessageLite& windowInfos)
{
    jsonOutput = windowInfos.SerializeAsString();
}

void to_json(nlohmann::json& jsonOutput, const NES::DescriptorConfig::Config& config)
{
    std::vector<nlohmann::json> jsonEntries;
    const auto orderedConfig = config | std::ranges::to<std::map<std::string, DescriptorConfig::ConfigType>>();
    for (const auto& [key, val] : orderedConfig)
    {
        nlohmann::json jsonValue = std::visit([](auto&& arg) { return nlohmann::json(arg); }, val);
        jsonEntries.push_back(nlohmann::json{{key, jsonValue}});
    }
    jsonOutput = jsonEntries;
}

}

void nlohmann::adl_serializer<NES::EnumWrapper, void>::to_json(json& jsonOutput, const NES::EnumWrapper& enumWrapper)
{
    jsonOutput = enumWrapper.getValue();
}
