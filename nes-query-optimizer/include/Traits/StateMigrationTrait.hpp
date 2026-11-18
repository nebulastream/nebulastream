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
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// Marks a join as the new instance of a join that moved to another worker (ISQD state migration): before the join processes data, it
/// receives the state of the old instance over the channel. Set by the placement amendment on the join of the new decomposed query plan.
/// The original attaches the path of the state file to the new operator instead.
struct StateMigrationTrait final
{
    static constexpr std::string_view NAME = "StateMigration";

    StateMigrationTrait(std::string channel, std::string receiverDataEndpoint);

    std::string channel;
    std::string receiverDataEndpoint;

    [[nodiscard]] const std::type_info& getType() const;
    bool operator==(const StateMigrationTrait& other) const = default;
    [[nodiscard]] size_t hash() const;
    [[nodiscard]] std::string explain(ExplainVerbosity) const;
    [[nodiscard]] std::string_view getName() const;
};

template <>
struct Reflector<StateMigrationTrait>
{
    Reflected operator()(const StateMigrationTrait& trait, const ReflectionContext& context) const;
};

template <>
struct Unreflector<StateMigrationTrait>
{
    StateMigrationTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<StateMigrationTrait>);

}

namespace NES::detail
{
struct ReflectedStateMigrationTrait
{
    std::string channel;
    std::string receiverDataEndpoint;
};
}
