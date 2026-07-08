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

#include <SinksParsing/SchemaFormatter.hpp>

#include <ranges>
#include <string>
#include <DataTypes/DataType.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
/// Header type column. For scalar types this is just the enum name (e.g. `UINT16`).
/// FIXEDSIZED carries element type and count which scalar types don't, so we encode
/// them as `FIXEDSIZED<UINT16,16>` to keep the field separator (`:`) count constant
/// across rows. The decoder lives in `nes-systests/.../SystestResultCheck.cpp`.
std::string formatTypeForHeader(const DataType& dataType)
{
    if (dataType.type == DataType::Type::FIXEDSIZED)
    {
        /// `;` (not `,`) inside the angle brackets so the comma-separated outer
        /// field split in `SystestResultCheck::parseFieldNames` doesn't tokenize it.
        return fmt::format("FIXEDSIZED<{};{}>", magic_enum::enum_name(dataType.elementType[0].type), dataType.count);
    }
    if (dataType.type == DataType::Type::VARARRAY)
    {
        /// `;` (not `,`) inside the angle brackets so the comma-separated outer
        /// field split in `SystestResultCheck::parseFieldNames` doesn't tokenize it.
        return fmt::format("VARARRAY<{}>", magic_enum::enum_name(dataType.elementType[0].type));
    }
    if (dataType.type == DataType::Type::STRUCT)
    {
        /// Nominal STRUCTs are identified by their registered name; emitting that
        /// directly lets `SystestResultCheck::parseFieldNames` round-trip via
        /// `DataTypeProvider::tryProvideDataType(name)`.
        return dataType.structName;
    }
    return std::string(magic_enum::enum_name(dataType.type));
}
}

std::string SchemaFormatter::getFormattedSchema()
{
    PRECONDITION(!std::ranges::empty(*schema), "Encountered schema without fields.");
    return fmt::format(
        "{}\n",
        fmt::join(
            *schema
                | std::views::transform(
                    [](const auto& field)
                    {
                        return fmt::format(
                            "{}:{}:{}",
                            field.getFullyQualifiedName(),
                            formatTypeForHeader(field.getDataType()),
                            magic_enum::enum_name(
                                field.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE));
                    }),
            ","));
}
}
