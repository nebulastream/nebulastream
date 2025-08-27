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

#include <GlobalOptimizer/Phases/OriginIdInferencePhase.hpp>

#include <algorithm>
#include <iterator>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <Traits/Trait.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
LogicalOperator propagateOriginIds(const LogicalOperator& visitingOperator)
{
    std::vector<LogicalOperator> newChildren;
    std::vector<std::vector<OriginId>> childOriginIds;
    for (const auto& child : visitingOperator.getChildren())
    {
        auto newChild = propagateOriginIds(child);
        newChildren.push_back(newChild);
        childOriginIds.push_back(newChild.getOutputOriginIds());
    }

    auto copy = visitingOperator;

    if (not copy.tryGet<SourceDescriptorLogicalOperator>())
    {
        copy = copy.withInputOriginIds(childOriginIds);
    }

    if (not hasTrait<OriginIdAssignerTrait>(visitingOperator.getTraitSet()))
    {
        std::unordered_set<OriginId> outputOriginIds;
        for (auto& originIds : childOriginIds)
        {
            std::ranges::copy(originIds, std::inserter(outputOriginIds, outputOriginIds.begin()));
        }
        copy = copy.withOutputOriginIds({outputOriginIds.begin(), outputOriginIds.end()});
    }

    return copy.withChildren(newChildren);
}
}

LogicalPlan OriginIdInferencePhase::apply(const LogicalPlan& inputPlan)
{
    LogicalPlan plan = inputPlan;
    /// origin ids, always start from 1 to n, whereby n is the number of operators that assign new origin ids
    auto originIdCounter = INITIAL_ORIGIN_ID.getRawValue();
    for (auto& originIdAssignerOp : getOperatorsByTraits<OriginIdAssignerTrait>(inputPlan))
    {
        LogicalOperator inferredOp;
        if (originIdAssignerOp.tryGet<SourceDescriptorLogicalOperator>())
        {
            inferredOp
                = originIdAssignerOp.withInputOriginIds({{OriginId(++originIdCounter)}}).withOutputOriginIds({OriginId(originIdCounter)});
        }
        else
        {
            inferredOp = originIdAssignerOp.withOutputOriginIds({OriginId(++originIdCounter)});
        }
        plan = *replaceOperator(plan, originIdAssignerOp.getId(), std::move(inferredOp));
    }

    /// propagate origin ids through the complete query plan
    std::vector<LogicalOperator> newSinks;
    newSinks.reserve(plan.getRootOperators().size());
    for (auto& sinkOperator : plan.getRootOperators())
    {
        newSinks.push_back(propagateOriginIds(sinkOperator));
    }
    return plan.withRootOperators(newSinks);
}
}
