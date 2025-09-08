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

#include <Traits/TraitSet.hpp>

#include <cstddef>
#include <ranges>
#include <string>
#include <typeindex>
#include <utility>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace NES
{

bool operator==(const TraitSet& lhs, const TraitSet& rhs)
{
    return lhs.traitMap == rhs.traitMap;
}

std::size_t TraitSet::size() const
{
    return traitMap.size();
}

std::string TraitSet::explain(ExplainVerbosity verbosity) const
{
    return fmt::format(
        "{}",
        fmt::join(
            traitMap
                | std::views::transform([verbosity](const std::pair<const std::type_index, Trait>& pair)
                                        { return fmt::format("{}: {}", pair.first.name(), pair.second.explain(verbosity)); }),
            ", "));
}

}
