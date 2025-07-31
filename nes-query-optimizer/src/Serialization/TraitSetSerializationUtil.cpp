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

#include <Serialization/TraitSetSerializationUtil.hpp>

#include <ranges>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <SerializableTrait.pb.h>
#include <TraitRegisty.hpp>

namespace NES
{
SerializableTraitSet* TraitSetSerializationUtil::serialize(const NES::TraitSet& traitSet, SerializableTraitSet* traitSetPtr)
{
    for (const auto& trait : traitSet | std::views::values)
    {
        traitSetPtr->mutable_traits()->try_emplace(trait.getName(), trait.serialize());
    }
    return traitSetPtr;
}

TraitSet TraitSetSerializationUtil::deserialize(const SerializableTraitSet* traitSetPtr, LogicalOperator logicalOperator)
{
    std::vector<Trait> deserializedTraits;
    for (const auto& [name, trait] : traitSetPtr->traits())
    {
        DescriptorConfig::Config config;
        for (const auto& [key, value] : trait.config())
        {
            config[key] = protoToDescriptorConfigType(value);
        }
        auto deserializedTrait = TraitRegistry::instance().create(name, TraitRegistryArguments{std::move(logicalOperator), config});
        if (!deserializedTrait.has_value())
        {
            throw CannotDeserialize("Failed to deserialize trait of type {}", name);
        }
        deserializedTraits.push_back(deserializedTrait.value());
    }

    return TraitSet{deserializedTraits};
}
}
