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

#include <Traits/FieldOrderingTrait.hpp>
#include <folly/Hash.h>
#include "Serialization/IdentifierSerializationUtil.hpp"

#include "TraitRegisty.hpp"

namespace NES
{
FieldOrderingTrait::FieldOrderingTrait(SchemaBase<UnboundFieldBase<1>, true> orderedFields) : orderedFields(std::move(orderedFields))
{
}

const SchemaBase<UnboundFieldBase<1>, true>& FieldOrderingTrait::getOrderedFields() const
{
    return orderedFields;
}

const std::type_info& FieldOrderingTrait::getType() const
{
    return typeid(FieldOrderingTrait);
}

std::string_view FieldOrderingTrait::getName() const
{
    return NAME;
}

SerializableTrait FieldOrderingTrait::serialize() const
{
    SerializableTrait trait;
    auto& orderedFieldsVariant = (*trait.mutable_config())["orderedFields"];
    auto* serializedOrderedFields = orderedFieldsVariant.mutable_orderedfields();
    serializedOrderedFields->clear_fields();
    for (const auto& field : orderedFields)
    {
        auto* serializedField = serializedOrderedFields->add_fields();
        IdentifierSerializationUtil::serializeIdentifier(*std::ranges::rbegin(field.getFullyQualifiedName()), serializedField);
    }
    return trait;
}

bool FieldOrderingTrait::operator==(const TraitConcept& other) const
{
    const auto* const casted = dynamic_cast<const FieldOrderingTrait*>(&other);
    if (casted == nullptr)
    {
        return false;
    }
    return orderedFields == casted->orderedFields;
}

std::string FieldOrderingTrait::explain(ExplainVerbosity) const
{
    return fmt::format(
        "FieldOrderingTrait({})",
        fmt::join(orderedFields | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); }), ", "));
}

size_t FieldOrderingTrait::hash() const
{
    return folly::hash::hash_range(orderedFields.begin(), orderedFields.end());
}

TraitRegistryReturnType TraitGeneratedRegistrar::RegisterFieldOrderingTrait(TraitRegistryArguments arguments)
{
    const auto outputSchema = arguments.logicalOperator->getOutputSchema();
    const auto serializedOrderedFieldsVariantIter = arguments.config.find("orderedFields");
    if (serializedOrderedFieldsVariantIter == arguments.config.end())
    {
        throw CannotDeserialize("FieldOrderingTrait, missing orderedFields");
    }
    if (!std::holds_alternative<SerializableOrderedFields>(serializedOrderedFieldsVariantIter->second))
    {
        throw CannotDeserialize("FieldOrderingTrait, orderedFields is not a SerializableOrderedFields");
    }
    const auto& serializedOrderedFields = std::get<SerializableOrderedFields>(serializedOrderedFieldsVariantIter->second);
    PRECONDITION(serializedOrderedFields.fields_size() >= 0, "FieldOrderingTrait, orderedFields has negative size");
    if (static_cast<uint64_t>(serializedOrderedFields.fields_size()) != std::ranges::size(outputSchema))
    {
        throw CannotDeserialize("FieldOrderingTrait, orderedFields does not match outputSchema");
    }
    std::vector<UnboundFieldBase<1>> orderedFields;
    orderedFields.reserve(serializedOrderedFields.fields_size());
    for (const auto& serializedField : serializedOrderedFields.fields())
    {
        const auto name = IdentifierSerializationUtil::deserializeIdentifier(serializedField);
        const auto fieldOpt = outputSchema.getFieldByName(name);
        if (!fieldOpt.has_value())
        {
            throw CannotDeserialize("FieldOrderingTrait, field {} not found in outputSchema", name);
        }
        orderedFields.push_back(fieldOpt.value().unbound());
    }
    return FieldOrderingTrait{SchemaBase<UnboundFieldBase<1>, true>{std::move(orderedFields)}};
}
}