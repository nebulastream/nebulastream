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


#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <Traitsets/Traitsets.hpp>

namespace NES::Traitsets
{
template <>
inline NES::LogicalOperator get<NES::LogicalOperator, NES::LogicalOperator>(NES::LogicalOperator traitset)
{
    return traitset;
}

template <>
inline std::optional<OriginIdAssignerTrait> get<std::optional<OriginIdAssignerTrait>, LogicalOperator>(LogicalOperator traitset)
{
    auto attachedTraitset = traitset.getTraitSet();
    const auto found = std::ranges::find_if(
        attachedTraitset,
        [](auto& iter)
        {
            if (iter.template tryGet<OriginIdAssignerTrait>())
            {
                return true;
            }
            return false;
        });
    if (found != attachedTraitset.end())
    {
        return found->get<OriginIdAssignerTrait>();
    }
    return std::nullopt;
}

template <>
inline std::vector<LogicalOperator> getEdges<Children, LogicalOperator, std::vector>(LogicalOperator traitset)
{
    return traitset.getChildren();
}

static_assert(hasEdges<Children, LogicalOperator, std::vector>);


}
