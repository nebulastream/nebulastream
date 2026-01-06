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
#include <Traits/FieldMappingTrait.hpp>
#include "Serialization/IdentifierSerializationUtil.hpp"

#include <folly/hash/Hash.h>
#include "TraitRegisty.hpp"

namespace NES
{
namespace
{
inline constexpr auto CONFIG_KEY = "fieldMapping";

}

FieldMappingTrait::FieldMappingTrait(std::unordered_map<UnboundFieldBase<1>, IdentifierList> mapping) : mapping(std::move(mapping))
{
}

std::optional<IdentifierList> FieldMappingTrait::getMapping(const UnboundFieldBase<1>& identifier) const
{
    const auto iter = mapping.find(identifier);
    if (iter == mapping.end())
    {
        return std::nullopt;
    }
    return iter->second;
}

const std::unordered_map<UnboundFieldBase<1>, IdentifierList>& FieldMappingTrait::getUnderlying() const
{
    return mapping;
}

const std::type_info& FieldMappingTrait::getType() const
{
    return typeid(FieldMappingTrait);
}

std::string_view FieldMappingTrait::getName() const
{
    return NAME;
}

SerializableTrait FieldMappingTrait::serialize() const
{
    SerializableTrait serializedTrait;
    auto& variantValue = (*serializedTrait.mutable_config())[CONFIG_KEY];
    variantValue.mutable_fieldmapping()->clear_fieldmappings();
    for (const auto& [field, physicalIdentifier] : mapping)
    {
        auto* entry = variantValue.mutable_fieldmapping()->add_fieldmappings();
        IdentifierSerializationUtil::serializeIdentifier(field.getFullyQualifiedName(), entry->mutable_logicalfield());
        IdentifierSerializationUtil::serializeIdentifierList(physicalIdentifier, entry->mutable_physicalfield());
    }
    return serializedTrait;
}

bool FieldMappingTrait::operator==(const TraitConcept& other) const
{
    const auto* const casted = dynamic_cast<const FieldMappingTrait*>(&other);
    if (casted == nullptr)
    {
        return false;
    }
    return this->mapping == casted->mapping;
}

std::string FieldMappingTrait::explain(ExplainVerbosity) const
{
    return fmt::format(
        "FieldMappingTrait({})",
        fmt::join(
            mapping | std::views::transform([](const auto& pair) { return fmt::format("{} -> {}", pair.first.getFullyQualifiedName(), pair.second); }), ", "));
}

size_t FieldMappingTrait::hash() const
{
    return folly::hash::commutative_hash_combine_range_generic(13, folly::hash::StdHasher{}, mapping.begin(), mapping.end());
}

TraitRegistryReturnType TraitGeneratedRegistrar::RegisterFieldMappingTrait(TraitRegistryArguments arguments)
{
    const auto iter = arguments.config.find(CONFIG_KEY);
    if (iter == arguments.config.end())
    {
        throw CannotDeserialize("FieldMappingTrait, missing field map");
    }
    if (!std::holds_alternative<SerializableFieldMapping>(iter->second))
    {
        throw CannotDeserialize("FieldMappingTrait, \"fieldMapping\" entry is not a field map");
    }
    const auto serializedFieldMapping = std::get<SerializableFieldMapping>(iter->second);
    const auto schema = arguments.logicalOperator.getOutputSchema();
    std::unordered_map<UnboundFieldBase<1>, IdentifierList> mapping;
    for (const auto& pair : serializedFieldMapping.fieldmappings())
    {
        const auto logicalFieldIdentifier = IdentifierSerializationUtil::deserializeIdentifier(pair.logicalfield());
        const auto fieldOpt = schema.getFieldByName(logicalFieldIdentifier);
        if (!fieldOpt)
        {
            throw CannotDeserialize("FieldMappingTrait, unknown field {}", logicalFieldIdentifier);
        }
        const auto physicalFieldIdentifier = IdentifierSerializationUtil::deserializeIdentifierList(pair.physicalfield());
        if (const auto [_, success] = mapping.try_emplace(fieldOpt.value().unbound(), physicalFieldIdentifier); !success)
        {
            throw CannotDeserialize("FieldMappingTrait, duplicate field mapping");
        }
    }
    return FieldMappingTrait{std::move(mapping)};
}
}