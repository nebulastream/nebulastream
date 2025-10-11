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
#include <chrono>
#include <cstddef>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/NESStrongTypeJson.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/SourceDescriptor.hpp>
#include <Util/TypeTraits.hpp>
#include <google/protobuf/message_lite.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <QueryId.hpp>

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

void to_json(nlohmann::json& jsonOutput, const Schema::Field& str);

void to_json(nlohmann::json& jsonOutput, const Schema& schema);

void to_json(nlohmann::json& jsonOutput, const google::protobuf::MessageLite& windowInfos);

void to_json(nlohmann::json& jsonOutput, const NES::DescriptorConfig::Config& config);

}

namespace nlohmann
{

template <typename Clock, typename Duration>
struct adl_serializer<std::chrono::time_point<Clock, Duration>>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonObject, const std::chrono::time_point<Clock, Duration>& timepoint)
    {
        jsonObject["since_epoch"] = std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
        jsonObject["unit"] = "microseconds";
        jsonObject["formatted"] = fmt::format("{}", timepoint);
    }
};

template <typename EnumType>
requires std::is_enum_v<EnumType>
struct adl_serializer<EnumType>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonObject, const EnumType& enumValue) { jsonObject = magic_enum::enum_name(enumValue); }
};

template <>
struct adl_serializer<NES::QueryId>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonObject, const NES::QueryId& queryId) { jsonObject = queryId.getLocalQueryId().getRawValue(); }
};

template <typename T>
void serializeTupleField(json& target, std::string_view fieldName, const T& field)
{
    if constexpr (NES::Optional<T>)
    {
        if (field.has_value())
        {
            target[fieldName] = field.value();
        }
    }
    else
    {
        target[fieldName] = field;
    }
}

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
            { (serializeTupleField(currentRow, columnNames[Is], std::get<Is>(row)), ...); }(std::make_index_sequence<sizeof...(Ts)>());
            jsonRows.push_back(std::move(currentRow));
        }
        jsonOutput = jsonRows;
    }
};

}
