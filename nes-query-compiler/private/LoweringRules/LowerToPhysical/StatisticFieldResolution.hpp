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

#include <string>
#include <string_view>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Resolves the physical record field identifier of a logical field in a physical schema.
/// Physical field names always start with the logical field's last name (DecideFieldMappings only ever appends
/// qualifiers, e.g. a trailing "new" on write-read conflicts), so matching on the first identifier is unambiguous
/// for the fixed statistic field names.
inline QualifiedIdentifier
resolvePhysicalFieldName(const Schema<QualifiedUnboundField, Ordered>& physicalSchema, const std::string_view logicalFieldName)
{
    const auto identifier = Identifier::parse(std::string{logicalFieldName});
    for (const auto& field : physicalSchema)
    {
        if (*field.getFullyQualifiedName().begin() == identifier)
        {
            return field.getFullyQualifiedName();
        }
    }
    throw CannotInferSchema("Field {} not found in physical schema {}", logicalFieldName, physicalSchema);
}

}
