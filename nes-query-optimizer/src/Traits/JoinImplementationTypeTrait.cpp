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

#include <Traits/JoinImplementationTypeTrait.hpp>

#include <cstddef>
#include <string_view>
#include <typeinfo>
#include <variant>

#include <Configurations/Enums/EnumWrapper.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <TraitRegistry.hpp>

namespace NES
{

JoinImplementationTypeTrait::JoinImplementationTypeTrait(const JoinImplementation implementationType)
    : implementationType(implementationType)
{
}

const std::type_info& JoinImplementationTypeTrait::getType() const
{
    return typeid(JoinImplementationTypeTrait);
}

bool JoinImplementationTypeTrait::operator==(const JoinImplementationTypeTrait& other) const
{
    return implementationType == other.implementationType;
}

size_t JoinImplementationTypeTrait::hash() const
{
    return magic_enum::enum_integer(implementationType);
}

std::string JoinImplementationTypeTrait::explain(ExplainVerbosity) const
{
    return fmt::format("JoinImplementationTypeTrait: {}", magic_enum::enum_name(implementationType));
}

std::string_view JoinImplementationTypeTrait::getName() const
{
    return NAME;
}

Reflected Reflector<JoinImplementationTypeTrait>::operator()(const JoinImplementationTypeTrait& trait) const
{
    return reflect(detail::ReflectedImplementationTypeTrait{trait.implementationType});
}

JoinImplementationTypeTrait
Unreflector<JoinImplementationTypeTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [joinImplementationType] = context.unreflect<detail::ReflectedImplementationTypeTrait>(reflected);
    return JoinImplementationTypeTrait{joinImplementationType};
}

}
