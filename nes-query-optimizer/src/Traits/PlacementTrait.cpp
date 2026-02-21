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
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <TraitRegisty.hpp>

namespace NES
{
/// Required for plugin registration, no implementation necessary
TraitRegistryReturnType TraitGeneratedRegistrar::RegisterPlacementTrait(TraitRegistryArguments arguments)
{
    return unreflect<PlacementTrait>(arguments.reflected);
}

PlacementTrait::PlacementTrait(WorkerId workerId) : onNode(std::move(workerId))
{
}

const std::type_info& PlacementTrait::getType() const
{
    return typeid(PlacementTrait);
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

Reflected Reflector<PlacementTrait>::operator()(const PlacementTrait& trait) const
{
    return reflect(detail::ReflectedPlacementTrait{trait.onNode.getRawValue()});
}

PlacementTrait Unreflector<PlacementTrait>::operator()(const Reflected& reflected) const
{
    auto [onNode] = unreflect<detail::ReflectedPlacementTrait>(reflected);
    return PlacementTrait{WorkerId(onNode)};
}
}
