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

#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <LegacyOptimizer/Phases/OriginIdInferencePhase.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <ErrorHandling.hpp>
#include <Traits/Trait.hpp>

namespace NES::LegacyOptimizer
{

LogicalOperator propagateOriginIds(const LogicalOperator& op, const std::vector<OriginId>& parentOriginIds)
{
    if (hasTrait<Optimizer::OriginIdAssignerTrait>(op.getTraitSet()))
    {
        /// no op
        return op;
    }

    LogicalOperator updatedOp = op.withOriginIds({ parentOriginIds });

    auto children = op.getChildren();
    std::vector<LogicalOperator> updatedChildren;
    if (op.tryGet<UnionLogicalOperator>())
    {
        for (const auto& child : children)
        {
            LogicalOperator childUpdated = propagateOriginIds(child, updatedOp.getOutputOriginIds());
            updatedChildren.push_back(childUpdated);
        }
        updatedOp = updatedOp.withChildren(updatedChildren);
    }
    else if (children.size() == 1)
    {
        const auto& outputOrigins = updatedOp.getOutputOriginIds();
        LogicalOperator childUpdated = propagateOriginIds(children[0], outputOrigins);
        updatedChildren.push_back(childUpdated);
        updatedOp = updatedOp.withChildren(updatedChildren);
    }
    else if (children.size() == 2)
    {
        const auto& outputOrigins = updatedOp.getOutputOriginIds();
        LogicalOperator leftUpdated  = propagateOriginIds(children[0], { outputOrigins[0] });
        LogicalOperator rightUpdated = propagateOriginIds(children[1], { outputOrigins[1] });
        updatedChildren.push_back(leftUpdated);
        updatedChildren.push_back(rightUpdated);
        updatedOp = updatedOp.withChildren(updatedChildren);
    }
    else
    {
        INVARIANT(false, "No support for LogicalOperators with more than 2 children or 0 children that is not a OriginIdAssigner");
    }
    return updatedOp;
}

void OriginIdInferencePhase::apply(LogicalPlan& queryPlan)
{
    /// origin ids, always start from 1 to n, whereby n is the number of operators that assign new orin ids
    auto originIdCounter = INITIAL_ORIGIN_ID.getRawValue();
    for (const auto& assigner : queryPlan.getOperatorsByTraits<Optimizer::OriginIdAssignerTrait>())
    {
        auto copy = assigner.withOriginIds({{OriginId(originIdCounter++)}});
        queryPlan.replaceOperator(assigner, copy);
    }

    /// propagate origin ids through the complete query plan
    std::vector<LogicalOperator> newSinks;
    for (auto& sinkOperator : queryPlan.rootOperators)
    {
        LogicalOperator updatedSink = propagateOriginIds(sinkOperator, sinkOperator.getOutputOriginIds());
        newSinks.push_back(updatedSink);
    }
    queryPlan.rootOperators = newSinks;
}
}
