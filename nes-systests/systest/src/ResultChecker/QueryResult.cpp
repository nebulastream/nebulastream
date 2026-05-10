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
    for (const auto part : NES::splitOnMultipleDelimiters(field, {':'}, {'"'}))
    {
        parts.emplace_back(trimWhiteSpaces(part));
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

    DataType dataType;
    /// `FIXEDSIZED<ELEMENT,N>` — emitted by `SchemaFormatter::formatTypeForHeader`.
    /// Decode the element type and count and reconstruct the parameterized DataType
    /// directly (the registry only handles scalar types).
    if (parts.at(1).starts_with("FIXEDSIZED<") && parts.at(1).ends_with('>'))
    {
        const auto inner
            = parts.at(1).substr(std::string_view("FIXEDSIZED<").size(), parts.at(1).size() - std::string_view("FIXEDSIZED<").size() - 1);
        /// Inner separator is `;` (not `,`) to avoid colliding with the outer
        /// comma-separated field split. Format: `FIXEDSIZED<ELEMENT;COUNT>`.
        const auto sepPos = inner.find(';');
        if (sepPos == std::string_view::npos)
        {
            throw std::unexpected(fmt::format("Malformed FIXEDSIZED header column: {}", parts.at(1)));
        }
        const auto elementTypeStr = trimWhiteSpaces(inner.substr(0, sepPos));
        const auto countStr = trimWhiteSpaces(inner.substr(sepPos + 1));
        const auto elementType = magic_enum::enum_cast<DataType::Type>(elementTypeStr);
        if (not elementType.has_value())
        {
            return std::unexpected(fmt::format("Unknown FIXEDSIZED element type: {}", elementTypeStr));
        }
        uint32_t count = 0;
        try
        {
            count = static_cast<uint32_t>(std::stoul(std::string(countStr)));
        }
        catch (const std::exception&)
        {
            return std::unexpected(fmt::format("Could not parse FIXEDSIZED count: {}", countStr));
        }
        dataType = DataType{DataType::Type::FIXEDSIZED, *nullable, DataType{*elementType, DataType::NULLABLE::NOT_NULLABLE}, count};
    }
    else if (auto type = magic_enum::enum_cast<DataType::Type>(parts.at(1)); type.has_value() && type.value() != DataType::Type::STRUCT)
    {
        /// STRUCT explicitly excluded — its enum name carries no layout, so
        /// `provideDataType(STRUCT)` would fail. Named struct types reach
        /// the fallback below via their registered name.
        dataType = DataTypeProvider::provideDataType(type.value(), *nullable);
    }
    else if (toLowerCase(parts.at(1)) == "varsized")
    {
        dataType = DataTypeProvider::provideDataType(NES::DataType::Type::VARSIZED, *nullable);
    }
    else if (auto plugin = DataTypeProvider::tryProvideDataType(std::string{parts.at(1)}, *nullable); plugin.has_value())
    {
        /// Plugin-registered named type (e.g. ThermalFrame). Matches the
        /// header form emitted by `SchemaFormatter::formatTypeForHeader`.
        dataType = *plugin;
    }
    else
    {
        return std::unexpected(fmt::format("field '{}' has an unknown type '{}'", field, parts.at(1)));
    }
    /// Case sensitive field names will arrive quoted here and therefore remain case sensitive. Case insensitive field names will be canonicalized into upper-case for comparison, so they are not quoted here.
    return UnqualifiedUnboundField{Identifier::parse(std::string(parts.at(0))), dataType};
}

/// One header line: comma separated fields. The first field that does not parse is the reason the header is rejected.
std::expected<Schema<UnqualifiedUnboundField, Ordered>, std::string> parseHeader(const std::string_view headerLine)
{
    std::vector<UnqualifiedUnboundField> fields;
    for (const auto field : NES::splitOnMultipleDelimiters(headerLine, {','}, {'"'}))
    {
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
