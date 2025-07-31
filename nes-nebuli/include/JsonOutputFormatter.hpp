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
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/NESStrongTypeJson.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/SourceDescriptor.hpp>
#include <google/protobuf/message_lite.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>

namespace nlohmann
{
/// For some reason, serializing the enum wrapper does not work with the to_json functions in our namespace
template <>
struct adl_serializer<NES::EnumWrapper>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonOutput, const NES::EnumWrapper& enumWrapper);
};
}

namespace NES
{

void to_json(nlohmann::json& jsonOutput, const ParserConfig& parserConfig);

void to_json(nlohmann::json& jsonOutput, const DataType& dataType);

void to_json(nlohmann::json& jsonOutput, const UnboundField& str);


void to_json(nlohmann::json& jsonOutput, const UnboundOrderedSchema& schema);

void to_json(nlohmann::json& jsonOutput, const google::protobuf::MessageLite& windowInfos);

void to_json(nlohmann::json& jsonOutput, const NES::DescriptorConfig::Config& config);

}

namespace nlohmann
{

template <size_t Extent>
struct adl_serializer<NES::IdentifierBase<Extent>>
{
    static void to_json(json& jsonOutput, const NES::IdentifierBase<Extent>& identifier)
    {
        jsonOutput = identifier.asCanonicalString();
    }
};

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
