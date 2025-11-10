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

#include <Traits/ImplementationTypeTrait.hpp>

#include <cstddef>
#include <string_view>
#include <typeinfo>
#include <variant>

#include <Configurations/Enums/EnumWrapper.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SerializableTrait.pb.h>
#include <SerializableVariantDescriptor.pb.h>
#include <TraitRegisty.hpp>

namespace NES
{
/// Required for plugin registration, no implementation necessary
TraitRegistryReturnType TraitGeneratedRegistrar::RegisterJoinImplementationTypeTrait(TraitRegistryArguments arguments)
{
    if (const auto typeIter = arguments.config.find("implementationJoinType"); typeIter != arguments.config.end())
    {
        if (std::holds_alternative<EnumWrapper>(typeIter->second))
        {
            if (const auto implementation = std::get<EnumWrapper>(typeIter->second).asEnum<JoinImplementation>();
                implementation.has_value())
            {
                return JoinImplementationTypeTrait{implementation.value()};
            }
        }
    }
    throw CannotDeserialize("Failed to deserialize ImplementationTypeTrait");
}

JoinImplementationTypeTrait::JoinImplementationTypeTrait(const JoinImplementation implementationType)
    : implementationType(implementationType)
{
}

const std::type_info& JoinImplementationTypeTrait::getType() const
{
    return typeid(JoinImplementationTypeTrait);
}

SerializableTrait JoinImplementationTypeTrait::serialize() const
{
    SerializableTrait trait;
    SerializableEnumWrapper wrappedImplType;
    wrappedImplType.set_value(magic_enum::enum_name(implementationType));
    SerializableVariantDescriptor variant{};
    variant.set_allocated_enum_value(&wrappedImplType);
    (*trait.mutable_config())["implementationJoinType"] = variant;
    return trait;
}

bool JoinImplementationTypeTrait::operator==(const TraitConcept& other) const
{
    const auto* const casted = dynamic_cast<const JoinImplementationTypeTrait*>(&other);
    if (casted == nullptr)
    {
        return false;
    }
    return implementationType == casted->implementationType;
}

size_t JoinImplementationTypeTrait::hash() const
{
    return magic_enum::enum_integer(implementationType);
}

std::string JoinImplementationTypeTrait::explain(ExplainVerbosity) const
{
    return fmt::format("JoinImplementationTypeTrait: {}", magic_enum::enum_name(implementationType));
}

std::string_view JoinImplementationTypeTrait::getName() const
{
    return NAME;
}

/// Required for plugin registration, no implementation necessary
TraitRegistryReturnType TraitGeneratedRegistrar::RegisterMemoryLayoutTypeTrait(TraitRegistryArguments arguments)
{
    if (const auto typeIter = arguments.config.find("memoryLayoutType"); typeIter != arguments.config.end())
    {
        if (std::holds_alternative<EnumWrapper>(typeIter->second))
        {
            if (const auto implementation = std::get<EnumWrapper>(typeIter->second).asEnum<MemoryLayoutType>(); implementation.has_value())
            {
                return MemoryLayoutTypeTrait{implementation.value()};
            }
        }
    }
    throw CannotDeserialize("Failed to deserialize ImplementationTypeTrait");
}

MemoryLayoutTypeTrait::MemoryLayoutTypeTrait(const MemoryLayoutType memoryLayout) : memoryLayout(memoryLayout)
{
}

const std::type_info& MemoryLayoutTypeTrait::getType() const
{
    return typeid(MemoryLayoutTypeTrait);
}

SerializableTrait MemoryLayoutTypeTrait::serialize() const
{
    SerializableTrait trait;
    SerializableEnumWrapper wrappedImplType;
    wrappedImplType.set_value(magic_enum::enum_name(memoryLayout));
    SerializableVariantDescriptor variant{};
    variant.set_allocated_enum_value(&wrappedImplType);
    (*trait.mutable_config())["memoryLayoutType"] = variant;
    return trait;
}

bool MemoryLayoutTypeTrait::operator==(const TraitConcept& other) const
{
    const auto* const casted = dynamic_cast<const MemoryLayoutTypeTrait*>(&other);
    if (casted == nullptr)
    {
        return false;
    }
    return memoryLayout == casted->memoryLayout;
}

size_t MemoryLayoutTypeTrait::hash() const
{
    return magic_enum::enum_integer(memoryLayout);
}

std::string MemoryLayoutTypeTrait::explain(ExplainVerbosity) const
{
    return fmt::format("MemoryLayoutTypeTrait: {}", magic_enum::enum_name(memoryLayout));
}

std::string_view MemoryLayoutTypeTrait::getName() const
{
    return NAME;
}

}
