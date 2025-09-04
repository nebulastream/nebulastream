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

#include <LegacyOptimizer/OriginIdInferencePhase.hpp>

#include <algorithm>
#include <iterator>
#include <ranges>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/Trait.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
LogicalOperator propagateOriginIds(const LogicalOperator& visitingOperator, OriginId& lastOriginId)
{
    std::vector<LogicalOperator> newChildren;
    std::vector<std::vector<OriginId>> childOriginIds;
    for (const auto& child : visitingOperator.getChildren())
    {
        auto newChild = propagateOriginIds(child, lastOriginId);
        newChildren.push_back(newChild);
        childOriginIds.push_back(newChild.getOutputOriginIds());
    }

    auto copy = visitingOperator;

    if (copy.tryGetAs<OriginIdAssigner>().has_value())
    {
        lastOriginId = OriginId{lastOriginId.getRawValue() + 1};
        copy = copy.withOutputOriginIds({lastOriginId});
    }
    else
    {
        copy = copy.withOutputOriginIds(
            childOriginIds | std::views::join | std::ranges::to<std::unordered_set>() | std::ranges::to<std::vector>());
    }

    if (copy.tryGetAs<SourceDescriptorLogicalOperator>().has_value())
    {
        copy = copy.withInputOriginIds({copy.getOutputOriginIds()});
    }
    else
    {
        copy = copy.withInputOriginIds(childOriginIds);
    }

    return copy.withChildren(newChildren);
}
}

void OriginIdInferencePhase::apply(LogicalPlan& queryPlan) const /// NOLINT(readability-convert-member-functions-to-static)
{
    /// origin ids, always start from 1 to n, whereby n is the number of operators that assign new orin ids
    auto originIdCounter = OriginId{INITIAL_ORIGIN_ID.getRawValue()};
    /// propagate origin ids through the complete query plan
    std::vector<LogicalOperator> newSinks;
    newSinks.reserve(queryPlan.getRootOperators().size());
    for (auto& sinkOperator : queryPlan.getRootOperators())
    {
        newSinks.push_back(propagateOriginIds(sinkOperator, originIdCounter));
    }
    queryPlan = queryPlan.withRootOperators(newSinks);
}
}
