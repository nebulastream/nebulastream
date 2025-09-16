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

#include <Traits/PlacementTrait.hpp>

#include <string>
#include <typeinfo>
#include <utility>

#include <Traits/Trait.hpp>
#include <folly/hash/Hash.h>
#include <SerializableTrait.pb.h>

namespace NES
{

PlacementTrait::PlacementTrait(std::string workerId) : onNode{std::move(workerId)}
{
}

const std::type_info& PlacementTrait::getType() const
{
    return typeid(this);
}

SerializableTrait PlacementTrait::serialize() const
{
    SerializableTrait trait;
    trait.set_trait_type(getType().name());

    SerializableVariantDescriptor config;
    config.set_string_value(onNode);
    trait.mutable_config()->emplace("onNode", std::move(config));

    return trait;
}

bool PlacementTrait::operator==(const TraitConcept& other) const
{
    return typeid(other) == typeid(*this);
}

size_t PlacementTrait::hash() const
{
    return folly::hash::hash_combine(std::type_index(getType()).hash_code(), onNode);
}

}
