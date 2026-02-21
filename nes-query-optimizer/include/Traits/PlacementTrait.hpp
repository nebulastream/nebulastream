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
#include <string>
#include <string_view>
#include <typeinfo>

#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Assigns a placement on a physical node to an operator
struct PlacementTrait final
{
    static constexpr std::string_view NAME = "Placement";
    explicit PlacementTrait(WorkerId workerId);
    WorkerId onNode;

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const PlacementTrait& other) const = default;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<PlacementTrait>;
};

template <>
struct Reflector<PlacementTrait>
{
    Reflected operator()(const PlacementTrait& trait) const;
};

template <>
struct Unreflector<PlacementTrait>
{
    PlacementTrait operator()(const Reflected& reflected) const;
};

static_assert(TraitConcept<PlacementTrait>);

}

namespace NES::detail
{
struct ReflectedPlacementTrait
{
    std::string onNode;
};
}
