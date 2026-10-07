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
/// Parses the TYPE column of a header field, as emitted by `SchemaFormatter::formatTypeForHeader`.
/// Container types nest recursively (e.g. `FIXEDSIZED<FIXEDSIZED<INT32;2>;3>`, `VECTOR<FIXEDSIZED<INT32;2>>`).
/// Only the outermost type carries the field's nullability; element types are never nullable.
std::expected<DataType, std::string> parseType(const std::string_view typeStr, const DataType::NULLABLE nullable)
{
    /// `FIXEDSIZED<ELEMENT;COUNT>`. Decode the element type and count and reconstruct the parameterized DataType
    /// directly (the registry only handles scalar types).
    if (typeStr.starts_with("FIXEDSIZED<") && typeStr.ends_with('>'))
    {
        const auto inner
            = typeStr.substr(std::string_view("FIXEDSIZED<").size(), typeStr.size() - std::string_view("FIXEDSIZED<").size() - 1);
        /// Inner separator is `;` (not `,`) to avoid colliding with the outer comma-separated field split.
        /// The count is always the last component, so the last `;` separates it from a possibly nested element type.
        const auto sepPos = inner.rfind(';');
        if (sepPos == std::string_view::npos)
        {
            return std::unexpected(fmt::format("Malformed FIXEDSIZED header column: {}", typeStr));
        }
        const auto elementType = parseType(trimWhiteSpaces(inner.substr(0, sepPos)), DataType::NULLABLE::NOT_NULLABLE);
        if (not elementType.has_value())
        {
            return std::unexpected(fmt::format("Unknown FIXEDSIZED element type in {}: {}", typeStr, elementType.error()));
        }
        const auto countStr = trimWhiteSpaces(inner.substr(sepPos + 1));
        uint32_t count = 0;
        try
        {
            count = static_cast<uint32_t>(std::stoul(std::string(countStr)));
        }
        catch (const std::exception&)
        {
            return std::unexpected(fmt::format("Could not parse FIXEDSIZED count: {}", countStr));
        }
        return DataType{DataType::Type::FIXEDSIZED, nullable, *elementType, count};
    }
    if (typeStr.starts_with("VECTOR<") && typeStr.ends_with('>'))
    {
        const auto inner = typeStr.substr(std::string_view("VECTOR<").size(), typeStr.size() - std::string_view("VECTOR<").size() - 1);
        const auto elementType = parseType(trimWhiteSpaces(inner), DataType::NULLABLE::NOT_NULLABLE);
        if (not elementType.has_value())
        {
            return std::unexpected(fmt::format("Unknown VECTOR element type in {}: {}", typeStr, elementType.error()));
        }
        return DataType{DataType::Type::VECTOR, nullable, *elementType};
    }
    if (auto type = magic_enum::enum_cast<DataType::Type>(typeStr); type.has_value() && type.value() != DataType::Type::STRUCT)
    {
        /// STRUCT explicitly excluded — its enum name carries no layout, so
        /// `provideDataType(STRUCT)` would fail. Named struct types reach
        /// the fallback below via their registered name.
        return DataTypeProvider::provideDataType(type.value(), nullable);
    }
    if (toLowerCase(typeStr) == "varsized")
    {
        return DataTypeProvider::provideDataType(NES::DataType::Type::VARSIZED, nullable);
    }
    if (auto plugin = DataTypeProvider::tryProvideDataType(std::string{typeStr}, nullable); plugin.has_value())
    {
        /// Plugin-registered named type (e.g. ThermalFrame). Matches the
        /// header form emitted by `SchemaFormatter::formatTypeForHeader`.
        return *plugin;
    }
    return std::unexpected(fmt::format("unknown type '{}'", typeStr));
}

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

    const auto dataType = parseType(parts.at(1), *nullable);
    if (not dataType.has_value())
    {
        return std::unexpected(fmt::format("field '{}' has an unparsable type: {}", field, dataType.error()));
    }
    /// Case sensitive field names will arrive quoted here and therefore remain case sensitive. Case insensitive field names will be canonicalized into upper-case for comparison, so they are not quoted here.
    return UnqualifiedUnboundField{Identifier::parse(std::string(parts.at(0))), *dataType};
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
