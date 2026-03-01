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

#include <concepts>
#include <string>
#include <Traits/Trait.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <TraitRegistry.hpp>

namespace NES::detail
{
struct ReflectedTrait
{
    std::string type;
    Reflected config;
};
}

namespace NES
{

template <>
struct Reflector<detail::ErasedTrait>
{
    Reflected operator()(const detail::ErasedTrait& erased) const { return erased.reflect(); }
};

template <TraitConcept Checked>
struct Reflector<TypedTrait<Checked>>
{
    Reflected operator()(const TypedTrait<Checked>& trait) const
    {
        return reflect(detail::ReflectedTrait{std::string{trait.getName()}, reflect(*trait)});
    }
};

template <>
struct Reflector<TypedTrait<detail::ErasedTrait>>
{
    Reflected operator()(const TypedTrait<detail::ErasedTrait>& trait) const
    {
        return reflect(detail::ReflectedTrait{.type = std::string{trait.getName()}, .config = trait->reflect()});
    }
};

template <>
struct Unreflector<TypedTrait<>>
{
    TypedTrait<> operator()(const Reflected& rfl, const ReflectionContext& context) const
    {
        auto [name, data] = context.unreflect<detail::ReflectedTrait>(rfl);

        auto trait = TraitRegistry::instance().unreflect(name, data, context);
        if (!trait.has_value())
        {
            throw CannotDeserialize("Failed to unreflect trait of type {}", name);
        }
        return trait.value();
    }
};

static_assert(requires(Trait trait) {
    { reflect(trait) } -> std::same_as<Reflected>;
});


}
