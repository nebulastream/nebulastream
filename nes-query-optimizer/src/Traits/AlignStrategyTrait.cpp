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

#include <Traits/AlignStrategyTrait.hpp>

#include <cstddef>
#include <string_view>
#include <typeinfo>

#include <Operators/AlignLogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

AlignStrategyTrait::AlignStrategyTrait(const AlignLogicalOperator::AlignStrategy strategy) : strategy(strategy)
{
}

const std::type_info& AlignStrategyTrait::getType() const
{
    return typeid(AlignStrategyTrait);
}

bool AlignStrategyTrait::operator==(const AlignStrategyTrait& other) const
{
    return strategy == other.strategy;
}

size_t AlignStrategyTrait::hash() const
{
    return magic_enum::enum_integer(strategy);
}

std::string AlignStrategyTrait::explain(ExplainVerbosity) const
{
    return fmt::format("AlignStrategyTrait: {}", magic_enum::enum_name(strategy));
}

std::string_view AlignStrategyTrait::getName() const
{
    return NAME;
}

Reflected Reflector<AlignStrategyTrait>::operator()(const AlignStrategyTrait& trait, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedAlignStrategyTrait{trait.strategy});
}

AlignStrategyTrait Unreflector<AlignStrategyTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [strategy] = context.unreflect<detail::ReflectedAlignStrategyTrait>(reflected);
    return AlignStrategyTrait{strategy};
}

}
