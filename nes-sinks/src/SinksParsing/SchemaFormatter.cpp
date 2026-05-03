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

#include <sstream>
#include <string>
#include <fmt/format.h>
#include <DataTypes/PhysicalSchema.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
std::string SchemaFormatter::getFormattedSchema()
{
    PRECONDITION(schema->hasFields(), "Encountered schema without fields.");
    std::stringstream ss;
    bool first = true;
    /// Walk components: a `Point` field expands to one CSV column per component
    /// (`_X`, `_Y`, `_Z`). Each column reports the prototype LogicalType name
    /// (`INTEGER`/`FLOAT`/`BOOL`/`TEXT`) so the result-checker can resolve it
    /// through `LogicalTypeRegistry`.
    for (const auto& field : schema->getFields())
    {
        const auto* nullableStr = field.physicalType.nullable ? "IS_NULLABLE" : "NOT_NULLABLE";
        for (const auto& component : field.physicalType.components)
        {
            if (not first)
            {
                ss << ',';
            }
            first = false;
            ss << field.name << component.suffix << ':' << primitiveLogicalTypeName(component.type) << ':' << nullableStr;
        }
    }
    return fmt::format("{}\n", ss.str());
}
}
