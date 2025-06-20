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

#pragma once
#include <array>
#include <cstddef>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/NESStrongTypeJson.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/SourceDescriptor.hpp>
#include <google/protobuf/message_lite.h>
#include <magic_enum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>

namespace nlohmann
{
/// For some reason, serializing the enum wrapper does not work with the to_json functions in our namespace
template <>
struct adl_serializer<NES::EnumWrapper>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonOutput, const NES::EnumWrapper& enumWrapper) { jsonOutput = enumWrapper.getValue(); }
};
}

namespace NES
{

inline void to_json(nlohmann::json& jsonOutput, const ParserConfig& parserConfig)
{
    jsonOutput = nlohmann::json{
        {"type", parserConfig.parserType},
        {"fieldDelimiter", parserConfig.fieldDelimiter},
        {"tupleDelimiter", parserConfig.tupleDelimiter}};
}

inline void to_json(nlohmann::json& jsonOutput, const DataType& dataType)
{
    jsonOutput = magic_enum::enum_name(dataType.type);
}

inline void to_json(nlohmann::json& jsonOutput, const Schema::Field& str)
{
    jsonOutput = nlohmann::json{{"name", str.name}, {"type", str.dataType}};
}

inline void to_json(nlohmann::json& jsonOutput, const Schema& schema)
{
    jsonOutput = nlohmann::json{schema.getFields()};
}

inline void to_json(nlohmann::json& jsonOutput, const google::protobuf::MessageLite& windowInfos)
{
    jsonOutput = windowInfos.SerializeAsString();
}

inline void to_json(nlohmann::json& jsonOutput, const NES::DescriptorConfig::Config& config)
{
    std::vector<nlohmann::json> jsonEntries;
    for (const auto& [key, val] : config)
    {
        nlohmann::json jsonValue = std::visit([](auto&& arg) { return nlohmann::json(arg); }, val);
        jsonEntries.push_back(nlohmann::json{{key, jsonValue}});
    }
    jsonOutput = jsonEntries;
}


}

namespace nlohmann
{

template <size_t N, typename... Ts>
struct adl_serializer<std::pair<std::array<std::string_view, N>, std::vector<std::tuple<Ts...>>>>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonOutput, const std::pair<std::array<std::string_view, N>, std::vector<std::tuple<Ts...>>>& resultTupleType)
    {
        auto columnNames = resultTupleType.first;
        std::vector<nlohmann::json> jsonRows;
        jsonRows.reserve(resultTupleType.second.size());
        for (const auto& row : resultTupleType.second)
        {
            json currentRow;
            [&]<size_t... Is>(std::index_sequence<Is...>)
            { ((currentRow[columnNames[Is]] = std::get<Is>(row), 0), ...); }(std::make_index_sequence<sizeof...(Ts)>());
            jsonRows.push_back(std::move(currentRow));
        }
        jsonOutput = jsonRows;
    }
};

}
