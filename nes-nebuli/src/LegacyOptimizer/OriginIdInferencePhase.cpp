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

#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <LegacyOptimizer/OriginIdInferencePhase.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <Traits/Trait.hpp>
#include <ErrorHandling.hpp>

namespace NES::LegacyOptimizer
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
        copy = copy.withOutputOriginIds(childOriginIds[0]);
    }

    return copy.withChildren(newChildren);
}
}

void OriginIdInferencePhase::apply(LogicalPlan& queryPlan)
{
    /// origin ids, always start from 1 to n, whereby n is the number of operators that assign new orin ids
    auto originIdCounter = INITIAL_ORIGIN_ID.getRawValue();
    for (auto& assigner : getOperatorsByTraits<OriginIdAssignerTrait>(queryPlan))
    {
        if (assigner.tryGet<SourceDescriptorLogicalOperator>())
        {
            assigner = assigner.withInputOriginIds({{OriginId(++originIdCounter)}});
            auto inferredAssigner = assigner.withOutputOriginIds({OriginId(originIdCounter)});
            auto replaceResult = replaceOperator(queryPlan, assigner, inferredAssigner);
            INVARIANT(replaceResult.has_value(), "replaceOperator failed");
            queryPlan = std::move(replaceResult.value());
        }
        else
        {
            auto inferredAssigner = assigner.withOutputOriginIds({OriginId(++originIdCounter)});
            auto replaceResult = replaceOperator(queryPlan, assigner, inferredAssigner);
            INVARIANT(replaceResult.has_value(), "replaceOperator failed");
            queryPlan = std::move(replaceResult.value());
        }
    }

    /// propagate origin ids through the complete query plan
    std::vector<LogicalOperator> newSinks;
    newSinks.reserve(queryPlan.rootOperators.size());
    for (auto& sinkOperator : queryPlan.rootOperators)
    {
        newSinks.push_back(propagateOriginIds(sinkOperator));
    }
    queryPlan.rootOperators = newSinks;
}
}
