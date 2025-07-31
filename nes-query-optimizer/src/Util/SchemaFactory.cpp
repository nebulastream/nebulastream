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

#include <ranges>
#include <Util/SchemaFactory.hpp>

#include "Operators/LogicalOperator.hpp"
#include "Traits/FieldMappingTrait.hpp"
#include "Traits/FieldOrderingTrait.hpp"

namespace NES
{

UnboundOrderedSchema createSchemaFromTraits(
    const std::unordered_map<UnboundFieldBase<1>, IdentifierList>& fieldMappings, const SchemaBase<UnboundFieldBase<1>, true>& fieldOrdering)
{
    return fieldOrdering | std::views::transform([&fieldMappings](const auto& field) {
        auto foundMapping = fieldMappings.find(field);
        PRECONDITION(foundMapping != fieldMappings.end(), "Field in fieldOrdering was not present in mapping");
        return UnboundField{foundMapping->second, field.getDataType()};
    }) | std::ranges::to<UnboundOrderedSchema>();
}


UnboundOrderedSchema createPhysicalOutputSchema(const TraitSet& traitSet) {
    const auto outputFieldMappingOpt = traitSet.get<FieldMappingTrait>();
    // PRECONDITION(outputFieldMappingOpt.has_value(), "Expected FieldMappingTrait to be set");

    const auto fieldOrderingOpt = traitSet.get<FieldOrderingTrait>();
    // PRECONDITION(fieldOrderingOpt.has_value(), "Expected a FieldOrderingTrait");
    return createSchemaFromTraits(outputFieldMappingOpt.getUnderlying(), fieldOrderingOpt.getOrderedFields());
}
}