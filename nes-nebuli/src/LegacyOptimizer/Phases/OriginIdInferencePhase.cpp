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

namespace NES::LegacyOptimizer
{

void inferInputOrigins(LogicalOperator& logicalOperator)
{
    if (hasTrait<Optimizer::OriginIdAssignerTrait>(logicalOperator.getTraitSet()))
    {
        /// noop
    }
    else if (logicalOperator.tryGet<UnionLogicalOperator>())
    {
        auto unionOp = logicalOperator.get<UnionLogicalOperator>();
        std::vector<OriginId> combinedInputOriginIds;
        for (auto child : unionOp.getChildren())
        {
            inferInputOrigins(child);
            auto childInputOriginIds = child.getOutputOriginIds();
            combinedInputOriginIds.insert(combinedInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
        }
        logicalOperator.setInputOriginIds({combinedInputOriginIds});
    } else if (logicalOperator.getChildren().size() == 2)
    {
        std::vector<OriginId> leftInputOriginIds;
        auto rightChild = logicalOperator.getChildren()[0];
        inferInputOrigins(rightChild);
        auto childInputOriginIds = rightChild.getOutputOriginIds();
        leftInputOriginIds.insert(leftInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());

        std::vector<OriginId> rightInputOriginIds;
        auto leftChild = logicalOperator.getChildren()[0];
        inferInputOrigins(leftChild);
        childInputOriginIds = leftChild.getOutputOriginIds();
        rightInputOriginIds.insert(rightInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());

        logicalOperator.setInputOriginIds({leftInputOriginIds, rightInputOriginIds});
    } else if (logicalOperator.getChildren().size() == 1)
    {
        std::vector<OriginId> inputOriginIds;
        auto child = logicalOperator.getChildren()[0];
        inferInputOrigins(child);
        auto childInputOriginIds = child.getOutputOriginIds();
        inputOriginIds.insert(inputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());

        logicalOperator.setInputOriginIds({inputOriginIds});

    } else if (logicalOperator.getChildren().size() > 2)
    {
        INVARIANT(false, "No support for LogicalOperators with more than 2 children");
    }
}

void OriginIdInferencePhase::apply(LogicalPlan& queryPlan)
{
    /// origin ids, always start from 1 to n, whereby n is the number of operators that assign new orin ids
    uint64_t originIdCounter = INITIAL_ORIGIN_ID.getRawValue();
    for (auto operatorWithOriginId : queryPlan.getOperatorsByTraits<Optimizer::OriginIdAssignerTrait>())
    {

        auto copy = operatorWithOriginId;
        copy.setInputOriginIds({{OriginId(originIdCounter++)}});
        copy.setOutputOriginIds({OriginId(originIdCounter)});

        queryPlan.replaceOperator(operatorWithOriginId, copy);
    }

    /// propagate origin ids through the complete query plan
    for (auto& rootOperator : queryPlan.getRootOperators())
    {
        inferInputOrigins(rootOperator);
    }
}
}
