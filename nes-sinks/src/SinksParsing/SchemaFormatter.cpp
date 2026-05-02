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
#include <sstream>
#include <string>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <DataTypes/Nullable.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
std::string SchemaFormatter::getFormattedSchema()
{
    PRECONDITION(schema->hasFields(), "Encountered schema without fields.");
    auto formatField = [](const auto& field, std::stringstream& out)
    {
        out << field.name << ':' << field.logicalType.getName() << ':' << magic_enum::enum_name(field.logicalType.getNullable());
    };
    std::stringstream ss;
    formatField(schema->getFields().front(), ss);
    for (const auto& field : schema->getFields() | std::views::drop(1))
    {
        ss << ',';
        formatField(field, ss);
    }
    return fmt::format("{}\n", ss.str());
}
}
