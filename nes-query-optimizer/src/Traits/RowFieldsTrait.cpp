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

#include <Traits/RowFieldsTrait.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>

namespace NES
{

RowFieldsTrait::RowFieldsTrait(std::vector<uint64_t> positions) : positions(std::move(positions))
{
}

const std::type_info& RowFieldsTrait::getType() const
{
    return typeid(RowFieldsTrait);
}

bool RowFieldsTrait::operator==(const RowFieldsTrait& other) const
{
    return positions == other.positions;
}

size_t RowFieldsTrait::hash() const
{
    return folly::hash::hash_range(positions.begin(), positions.end());
}

std::string RowFieldsTrait::explain(ExplainVerbosity) const
{
    return fmt::format("RowFieldsTrait: {}", fmt::join(positions, ", "));
}

std::string_view RowFieldsTrait::getName() const
{
    return NAME;
}

Reflected Reflector<RowFieldsTrait>::operator()(const RowFieldsTrait& trait, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedRowFieldsTrait{trait.positions});
}

RowFieldsTrait Unreflector<RowFieldsTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [positions] = context.unreflect<detail::ReflectedRowFieldsTrait>(reflected);
    return RowFieldsTrait{std::move(positions)};
}
}
