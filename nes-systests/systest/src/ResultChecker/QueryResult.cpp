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

#include <ResultChecker/QueryResult.hpp>

#include <expected>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Strings.hpp>

namespace NES
{

namespace
{
/// A header field is written as name:TYPE:NULLABILITY.
std::expected<UnqualifiedUnboundField, std::string> parseField(const std::string_view field)
{
    std::vector<std::string_view> parts;
    for (const auto subrange : std::ranges::split_view(field, ':'))
    {
        parts.emplace_back(trimWhiteSpaces(std::string_view(subrange)));
    }
    if (parts.size() != 3)
    {
        return std::unexpected(fmt::format("field '{}' is not written as name:TYPE:NULLABILITY", field));
    }

    const auto nullable = magic_enum::enum_cast<DataType::NULLABLE>(parts.at(2));
    if (not nullable)
    {
        return std::unexpected(fmt::format("field '{}' has an unknown nullability '{}'", field, parts.at(2)));
    }

    const auto type = magic_enum::enum_cast<DataType::Type>(parts.at(1));
    if (not type and toLowerCase(parts.at(1)) != "varsized")
    {
        return std::unexpected(fmt::format("field '{}' has an unknown type '{}'", field, parts.at(1)));
    }

    /// The header holds the name with the case that the sink wrote, so it parses as a quoted identifier to keep that case.
    return UnqualifiedUnboundField{
        Identifier::parse(fmt::format("\"{}\"", parts.at(0))),
        DataTypeProvider::provideDataType(type.value_or(DataType::Type::VARSIZED), *nullable)};
}

/// One header line: comma separated fields. The first field that does not parse is the reason the header is rejected.
std::expected<Schema<UnqualifiedUnboundField, Ordered>, std::string> parseHeader(const std::string_view headerLine)
{
    std::vector<UnqualifiedUnboundField> fields;
    for (const auto split : std::ranges::split_view(headerLine, ','))
    {
        const auto field = std::string_view{split.begin(), split.end()};
        if (field.empty())
        {
            continue;
        }
        auto parsed = parseField(field);
        if (not parsed)
        {
            return std::unexpected(std::move(parsed.error()));
        }
        fields.push_back(std::move(*parsed));
    }
    return Schema<UnqualifiedUnboundField, Ordered>{std::move(fields)};
}
}

std::expected<QueryResult, std::string> loadQueryResult(const std::filesystem::path& resultFilePath)
{
    std::ifstream resultFile(resultFilePath);
    if (!resultFile)
    {
        return std::unexpected(fmt::format("result file was not written: {}", resultFilePath.string()));
    }

    std::string line;
    if (!std::getline(resultFile, line))
    {
        return std::unexpected(fmt::format("result file is empty: {}", resultFilePath.string()));
    }
    auto schema = parseHeader(line);
    if (not schema)
    {
        return std::unexpected(fmt::format("result file has a malformed schema header, {}: {}", schema.error(), resultFilePath.string()));
    }
    if (schema->size() == 0)
    {
        return std::unexpected(fmt::format("result file has an empty schema header: {}", resultFilePath.string()));
    }

    QueryResult result{.schema = std::move(*schema), .tuples = {}};
    while (std::getline(resultFile, line))
    {
        result.tuples.push_back(line);
    }
    return result;
}

}
