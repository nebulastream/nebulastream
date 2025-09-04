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

#include <cstddef>
#include <cstdint>
#include <typeinfo>
#include <Traits/Trait.hpp>
#include <magic_enum/magic_enum.hpp>
#include <SerializableTrait.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
enum class JoinImplementation : uint8_t
{
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    CHOICELESS
};

/// Struct that stores implementation types as traits. For now, we simply have a choice/implementation type for the joins (Hash-Join vs. NLJ)
struct ImplementationTypeTrait final : public TraitConcept
{
    JoinImplementation implementationType;

    explicit ImplementationTypeTrait(const JoinImplementation implementationType) : implementationType(implementationType) { }

    [[nodiscard]] const std::type_info& getType() const override { return typeid(ImplementationTypeTrait); }

    [[nodiscard]] SerializableTrait serialize() const override
    {
        SerializableTrait trait;
        trait.set_trait_type(getType().name());
        auto wrappedImplType = SerializableEnumWrapper{};
        wrappedImplType.set_value(magic_enum::enum_name(implementationType));
        SerializableVariantDescriptor variant{};
        variant.set_allocated_enum_value(&wrappedImplType);
        (*trait.mutable_config())["implementationType"] = variant;
        return trait;
    }

    bool operator==(const TraitConcept& other) const override
    {
        const auto casted = dynamic_cast<const ImplementationTypeTrait*>(&other);
        if (casted == nullptr)
        {
            return false;
        }
        return implementationType == casted->implementationType;
    };

    [[nodiscard]] size_t hash() const override { return magic_enum::enum_integer(implementationType); }
};

}
