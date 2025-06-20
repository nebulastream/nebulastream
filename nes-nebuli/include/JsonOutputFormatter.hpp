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
#include <cstdint>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <StatementOutputAssembler.hpp>

namespace NES
{
/// Since nlohmanjson takes care of the serialization of std::vector, we only need to serialize the underlying tuple type
// template <AssemblembleStatementResult StatementResult>
// void to_json(nlohmann::json& jsonOutput, typename StatementOutputAssembler<StatementResult>::OutputRowType resultTupleType)
// {
// }
//
// template <>
// void to_json<CreateLogicalSourceStatementResult>(
//     nlohmann::json& jsonOutput, StatementOutputAssembler<CreateLogicalSourceStatementResult>::OutputRowType resultTupleType);

void to_json(nlohmann::json& jsonOutput, const ParserConfig& parserConfig)
{
    jsonOutput = nlohmann::json{
        {"type", parserConfig.parserType},
        {"fieldDelimiter", parserConfig.fieldDelimiter},
        {"tupleDelimiter", parserConfig.tupleDelimiter}};
}
void to_json(nlohmann::json& jsonOutput, const DataType& dataType)
{
    jsonOutput = magic_enum::enum_name(dataType.type);
}
void to_json(nlohmann::json& jsonOutput, const Schema::Field& str)
{
    jsonOutput = nlohmann::json{{"name", str.name}, {"type", str.dataType}};
}
void to_json(nlohmann::json& jsonOutput, const Schema& schema)
{
    jsonOutput = nlohmann::json{schema.getFields()};
}


}

namespace nlohmann
{

template <size_t N, typename... Ts>
struct adl_serializer<std::pair<std::array<std::string_view, N>, std::vector<std::tuple<Ts...>>>>
{
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