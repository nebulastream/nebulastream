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
                              magic_enum::enum_name(field.getDataType().type),
                              magic_enum::enum_name(
                                  field.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE
                                                               : DataType::NULLABLE::NOT_NULLABLE));
                      }),
            ","));
}
}
