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
#include <string_view>
#include <typeinfo>
#include <Operators/AlignLogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

struct AlignStrategyTrait final
{
    static constexpr std::string_view NAME = "AlignStrategy";
    AlignLogicalOperator::AlignStrategy strategy;

    explicit AlignStrategyTrait(AlignLogicalOperator::AlignStrategy strategy);

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const AlignStrategyTrait& other) const;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<AlignStrategyTrait>;
};

template <>
struct Reflector<AlignStrategyTrait>
{
    Reflected operator()(const AlignStrategyTrait& trait, const ReflectionContext& context) const;
};

template <>
struct Unreflector<AlignStrategyTrait>
{
    AlignStrategyTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<AlignStrategyTrait>);
}

namespace NES::detail
{
struct ReflectedAlignStrategyTrait
{
    AlignLogicalOperator::AlignStrategy strategy;
};
}
