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
#include <string>
#include <string_view>
#include <typeinfo>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{
enum class JoinImplementation : uint8_t
{
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    CHOICELESS
};

/// Struct that stores the join implementation type as traits. For now, we simply have a choice/implementation type for the joins (Hash-Join vs. NLJ)
struct JoinImplementationTypeTrait final
{
    static constexpr std::string_view NAME = "JoinImplementationType";
    JoinImplementation implementationType;

    explicit JoinImplementationTypeTrait(JoinImplementation implementationType);

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const JoinImplementationTypeTrait& other) const;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<JoinImplementationTypeTrait>;
};

template <>
struct Reflector<JoinImplementationTypeTrait>
{
    Reflected operator()(const JoinImplementationTypeTrait& trait) const;
};

template <>
struct Unreflector<JoinImplementationTypeTrait>
{
    JoinImplementationTypeTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<JoinImplementationTypeTrait>);
}

namespace NES::detail
{
struct ReflectedImplementationTypeTrait
{
    JoinImplementation joinImplementationType;
};
}
