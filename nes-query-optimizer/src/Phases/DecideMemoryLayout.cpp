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
#include <Phases/DecideMemoryLayout.hpp>

#include <algorithm>
#include <ranges>
#include <unordered_set>
#include <vector>

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <Utils.hpp>

namespace NES
{

LogicalPlan DecideMemoryLayout::apply(const LogicalPlan& queryPlan)
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Only single root operators are supported for now");
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    return LogicalPlan{queryPlan.getQueryId(), {apply(queryPlan.getRootOperators()[0])}};
}

LogicalOperator DecideMemoryLayout::apply(const LogicalOperator& logicalOperator)
{
    const auto children = logicalOperator.getChildren()
        | std::views::transform([this](const LogicalOperator& child) { return apply(child); }) | std::ranges::to<std::vector>();
    auto traitSet = logicalOperator.getTraitSet();
    std::erase_if(traitSet, [](const Trait& trait) { return trait.tryGet<MemoryLayoutTypeTrait>().has_value(); });

    if (this->conf.layoutStrategy.getValue() == MemoryLayoutStrategy::USE_SINGLE_LAYOUT)
    {
        if (logicalOperator.tryGet<SourceDescriptorLogicalOperator>() || logicalOperator.tryGet<SourceNameLogicalOperator>())
        {
            /// sources are always in row layout
            traitSet.insert(MemoryLayoutTypeTrait{Schema::MemoryLayoutType::ROW_LAYOUT, Schema::MemoryLayoutType::ROW_LAYOUT});
        }else
        {
            auto incomingTraitSet = getMemoryLayoutTypeTrait(children[0]);
            auto incomingLayout = incomingTraitSet.targetLayoutType;
            auto targetLayout = this->conf.memoryLayout.getValue();
            if (logicalOperator.tryGet<SinkLogicalOperator>())
            {
                /// Sinks target layout can currently only be row layout
                traitSet.insert(MemoryLayoutTypeTrait{incomingLayout, Schema::MemoryLayoutType::ROW_LAYOUT});
            }else
            {
                traitSet.insert(MemoryLayoutTypeTrait{incomingLayout, targetLayout});
            }
        }
    }
    else if (this->conf.layoutStrategy.getValue() == MemoryLayoutStrategy::LEGACY)
    {
        return logicalOperator;
    }
    ///TODO: add advanced strategies here

    return logicalOperator.withChildren(children).withTraitSet(traitSet);
}
}
