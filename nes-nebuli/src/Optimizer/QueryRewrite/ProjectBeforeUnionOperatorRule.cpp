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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer
{

std::shared_ptr<ProjectBeforeUnionOperatorRule> ProjectBeforeUnionOperatorRule::create()
{
    return std::make_shared<ProjectBeforeUnionOperatorRule>(ProjectBeforeUnionOperatorRule());
}

std::shared_ptr<QueryPlan> ProjectBeforeUnionOperatorRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    NES_DEBUG("Before applying ProjectBeforeUnionOperatorRule to the query plan: {}", queryPlan->toString());
    auto unionOperators = queryPlan->getOperatorByType<LogicalUnionOperator>();
    for (auto& unionOperator : unionOperators)
    {
        auto rightInputSchema = unionOperator->getRightInputSchema();
        auto leftInputSchema = unionOperator->getLeftInputSchema();
        /// Only apply the rule when right side and left side schema are different
        if (!(*rightInputSchema == *leftInputSchema))
        {
            /// Construct project operator for mapping rightInputSource To leftInputSource
            auto projectOperator = constructProjectOperator(rightInputSchema, leftInputSchema);
            auto childrenToUnionOperator = unionOperator->getChildren();
            for (auto& child : childrenToUnionOperator)
            {
                auto childOutputSchema = NES::Util::as<LogicalOperator>(child)->getOutputSchema();
                /// Find the child that matches the right schema and inset the project operator there
                if (*rightInputSchema == *childOutputSchema)
                {
                    child->insertBetweenThisAndParentNodes(projectOperator);
                    break;
                }
            }
        }
    }
    NES_DEBUG("After applying ProjectBeforeUnionOperatorRule to the query plan: {}", queryPlan->toString());
    return queryPlan;
}

std::shared_ptr<LogicalOperator> ProjectBeforeUnionOperatorRule::constructProjectOperator(
    const std::shared_ptr<Schema>& sourceSchema, const std::shared_ptr<Schema>& destinationSchema)
{
    NES_TRACE(
        "Computing Projection operator for Source Schema{} and Destination schema {}",
        sourceSchema->toString(),
        destinationSchema->toString());
    /// Fetch source and destination schema fields
    const auto& sourceFields = sourceSchema;
    const auto& destinationFields = destinationSchema;
    std::vector<std::shared_ptr<NodeFunction>> projectFunctions;
    /// Compute projection functions
    for (uint64_t i = 0; i < sourceSchema->getFieldCount(); i++)
    {
        auto field = sourceFields->getFieldByIndex(i);
        auto updatedFieldName = destinationFields->getFieldByIndex(i)->getName();
        /// Compute field access and field rename function
        auto originalField = NodeFunctionFieldAccess::create(field->getDataType(), field->getName());
        auto newField = NodeFunctionFieldAccess::create(field->getDataType(), updatedFieldName);
        auto fieldRename = NodeFunctionFieldAssignment::create(NES::Util::as<NodeFunctionFieldAccess>(newField), NES::Util::as<NodeFunctionFieldAccess>(originalField));
        projectFunctions.push_back(fieldRename);
    }
    /// Create Projection operator
    return std::make_shared<LogicalProjectionOperator>(projectFunctions, getNextOperatorId());
}

}
