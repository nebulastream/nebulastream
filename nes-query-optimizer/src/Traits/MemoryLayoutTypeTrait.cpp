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

#include <Traits/MemoryLayoutTypeTrait.hpp>

#include <cstddef>
#include <string>
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

bool MemoryLayoutTypeTrait::operator==(const MemoryLayoutTypeTrait& other) const
{
    return memoryLayout == other.memoryLayout;
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
}
