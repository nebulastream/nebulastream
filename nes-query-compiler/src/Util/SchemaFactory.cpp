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

#include <Util/SchemaFactory.hpp>

#include <ranges>
#include <unordered_map>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

Schema<QualifiedUnboundField, Ordered> createSchemaFromTraits(
    const std::unordered_map<UnqualifiedUnboundField, QualifiedIdentifier>& fieldMappings,
    const Schema<UnqualifiedUnboundField, Ordered>& fieldOrdering)
{
    PRECONDITION(fieldMappings.size() == fieldOrdering.size(), "FieldMappings and FieldOrdering must have the same size");
    const auto schema = fieldOrdering
        | std::views::transform(
                            [&fieldMappings](const auto& field)
                            {
                                auto foundMapping = fieldMappings.find(field);
                                PRECONDITION(foundMapping != fieldMappings.end(), "Field in fieldOrdering was not present in mapping");
                                return QualifiedUnboundField{foundMapping->second, field.getDataType()};
                            })
        | std::ranges::to<Schema<QualifiedUnboundField, Ordered>>();
    PRECONDITION(
        std::ranges::size(schema) == fieldMappings.size(),
        "FieldMappings and FieldOrdering must have the same size, check field name collisions");
    return schema;
}

Schema<QualifiedUnboundField, Ordered> createPhysicalOutputSchema(const TraitSet& traitSet)
{
    const auto outputFieldMappingOpt = traitSet.get<FieldMappingTrait>();

    const auto fieldOrderingOpt = traitSet.get<FieldOrderingTrait>();
    return createSchemaFromTraits(outputFieldMappingOpt->getUnderlying(), fieldOrderingOpt->getOrderedFields());
}
}
