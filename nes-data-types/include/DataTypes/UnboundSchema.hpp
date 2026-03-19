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
#include <span>
#include <utility>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <Identifiers/Identifier.hpp>

namespace NES
{


Schema<QualifiedUnboundField, Ordered> convertLegacySchema(const LegacySchema& legacySchema)
{
    std::vector<QualifiedUnboundField> fields;
    for (const auto& legacyField : legacySchema)
    {
        auto newId = convertLegacyIdList(legacyField.name);
        auto newField = QualifiedUnboundField{newId, legacyField.dataType};
        fields.push_back(newField);
    }
    return Schema<QualifiedUnboundField, Ordered>(std::move(fields));
}

LegacySchema convertToLegacySchema(const Schema<QualifiedUnboundField, Ordered>& schema)
{
    LegacySchema legacySchema;
    for (const auto& field : schema)
    {
        legacySchema.addField(convertToLegacyIdList(field.getFullyQualifiedName()), field.getDataType());
    }
    return legacySchema;
}

}

template <>
struct fmt::formatter<NES::Schema<NES::QualifiedUnboundField, NES::Ordered>> : fmt::ostream_formatter
{
};

template <>
struct fmt::formatter<NES::Schema<NES::UnqualifiedUnboundField, NES::Unordered>> : fmt::ostream_formatter
{
};

template <>
struct fmt::formatter<NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>> : fmt::ostream_formatter
{
};

static_assert(fmt::formattable<NES::Schema<NES::UnboundFieldBase<std::dynamic_extent>, NES::Ordered>>);
