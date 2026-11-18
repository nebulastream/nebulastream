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

#include <Traits/StateMigrationTrait.hpp>

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>

namespace NES
{

StateMigrationTrait::StateMigrationTrait(std::string channel, std::string receiverDataEndpoint)
    : channel(std::move(channel)), receiverDataEndpoint(std::move(receiverDataEndpoint))
{
}

const std::type_info& StateMigrationTrait::getType() const
{
    return typeid(StateMigrationTrait);
}

size_t StateMigrationTrait::hash() const
{
    return std::hash<std::string>{}(channel);
}

std::string StateMigrationTrait::explain(ExplainVerbosity) const
{
    return fmt::format("StateMigrationTrait: channel {} at {}", channel, receiverDataEndpoint);
}

std::string_view StateMigrationTrait::getName() const
{
    return NAME;
}

Reflected Reflector<StateMigrationTrait>::operator()(const StateMigrationTrait& trait, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedStateMigrationTrait{.channel = trait.channel, .receiverDataEndpoint = trait.receiverDataEndpoint});
}

StateMigrationTrait Unreflector<StateMigrationTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [channel, receiverDataEndpoint] = context.unreflect<detail::ReflectedStateMigrationTrait>(reflected);
    return StateMigrationTrait{std::move(channel), std::move(receiverDataEndpoint)};
}

}
