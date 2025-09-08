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
TraitRegistryReturnType TraitGeneratedRegistrar::RegisterImplementationTypeTrait(TraitRegistryArguments arguments)
{
    if (const auto typeIter = arguments.config.find("implementationType"); typeIter != arguments.config.end())
    {
        if (std::holds_alternative<EnumWrapper>(typeIter->second))
        {
            if (const auto implementation = std::get<EnumWrapper>(typeIter->second).asEnum<JoinImplementation>();
                implementation.has_value())
            {
                return ImplementationTypeTrait{implementation.value()};
            }
        }
    }
    throw CannotDeserialize("Failed to deserialize ImplementationTypeTrait");
}

ImplementationTypeTrait::ImplementationTypeTrait(const JoinImplementation implementationType) : implementationType(implementationType)
{
}

const std::type_info& ImplementationTypeTrait::getType() const
{
    return typeid(ImplementationTypeTrait);
}

SerializableTrait ImplementationTypeTrait::serialize() const
{
    SerializableTrait trait;
    auto wrappedImplType = SerializableEnumWrapper{};
    wrappedImplType.set_value(magic_enum::enum_name(implementationType));
    SerializableVariantDescriptor variant{};
    variant.set_allocated_enum_value(&wrappedImplType);
    (*trait.mutable_config())["implementationType"] = variant;
    return trait;
}

bool ImplementationTypeTrait::operator==(const TraitConcept& other) const
{
    const auto* const casted = dynamic_cast<const ImplementationTypeTrait*>(&other);
    if (casted == nullptr)
    {
        return false;
    }
    return implementationType == casted->implementationType;
}

size_t ImplementationTypeTrait::hash() const
{
    return magic_enum::enum_integer(implementationType);
}

std::string ImplementationTypeTrait::explain(ExplainVerbosity) const
{
    return fmt::format("ImplementationTypeTrait: {}", magic_enum::enum_name(implementationType));
}

std::string_view ImplementationTypeTrait::getName() const
{
    return NAME;
}
}
