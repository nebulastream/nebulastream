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

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <variant>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SerializableTrait.pb.h>
#include <SerializableVariantDescriptor.pb.h>
#include <TraitRegisty.hpp>

namespace NES
{
/// Required for plugin registration, no implementation necessary
TraitRegistryReturnType TraitGeneratedRegistrar::RegisterPlacementTrait(TraitRegistryArguments arguments)
{
    if (const auto typeIter = arguments.config.find("onNode"); typeIter != arguments.config.end())
    {
        if (const auto* onNode = std::get_if<std::string>(&typeIter->second))
        {
            return PlacementTrait{WorkerId(*onNode)};
        }
    }
    throw CannotDeserialize("Failed to deserialize PlacementTrait");
}

PlacementTrait::PlacementTrait(WorkerId workerId) : onNode(std::move(workerId))
{
}

const std::type_info& PlacementTrait::getType() const
{
    return typeid(PlacementTrait);
}

SerializableTrait PlacementTrait::serialize() const
{
    SerializableTrait trait;
    SerializableVariantDescriptor variant{};
    variant.set_string_value(onNode.getRawValue());
    (*trait.mutable_config())["onNode"] = variant;
    return trait;
}

size_t PlacementTrait::hash() const
{
    return std::hash<std::string>{}(onNode.getRawValue());
}

std::string PlacementTrait::explain(ExplainVerbosity) const
{
    return fmt::format("PlacementTrait: {}", onNode.getRawValue());
}

std::string_view PlacementTrait::getName() const
{
    return NAME;
}
}
