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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/QueryRewrite/RenameSourceToProjectOperatorRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer
{

QueryPlanPtr RenameSourceToProjectOperatorRule::apply(QueryPlanPtr queryPlan)
{
    NES_DEBUG("RenameSourceToProjectOperatorRule: Convert all Rename Source operator to the project operator");
    auto renameSourceOperators = queryPlan->getOperatorByType<RenameSourceOperator>();
    /// Iterate over all rename source operators and convert them to project operator
    for (auto& renameSourceOperator : renameSourceOperators)
    {
        /// Convert the rename source operator to project operator
        auto projectOperator = convert(renameSourceOperator);
        /// Replace rename source operator with the project operator
        renameSourceOperator->replace(projectOperator);
        /// Assign the project operator the id of as operator
        projectOperator->setId(renameSourceOperator->getId());
    }
    /// Return updated query plan
    return queryPlan;
}

std::shared_ptr<Operator> RenameSourceToProjectOperatorRule::convert(const std::shared_ptr<Operator>& operatorNode)
{
    /// Fetch the new source name and input schema for the as operator
    auto renameSourceOperator = NES::Util::as<RenameSourceOperator>(operatorNode);
    auto newSourceName = renameSourceOperator->getNewSourceName();
    auto inputSchema = renameSourceOperator->getInputSchema();

    std::vector<NodeFunctionPtr> projectionAttributes;
    /// Iterate over the input schema and add a new field rename function
    for (const auto& field : *inputSchema)
    {
        /// compute the new name for the field by added new source name as field qualifier
        std::string fieldName = field->getName();
        /// Compute new name without field qualifier
        std::string updatedFieldName = newSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR + fieldName;
        /// Compute field access and field rename function
        auto originalField = NodeFunctionFieldAccess::create(field->getDataType(), fieldName);
        auto fieldRenameFunction = NodeFunctionFieldRename::create(NES::Util::as<NodeFunctionFieldAccess>(originalField), updatedFieldName);
        projectionAttributes.push_back(fieldRenameFunction);
    }
    /// Construct a new project operator
    auto projectOperator = std::make_shared<LogicalProjectionOperator>(projectionAttributes, getNextOperatorId());
    return projectOperator;
}

RenameSourceToProjectOperatorRulePtr RenameSourceToProjectOperatorRule::create()
{
    return std::make_shared<RenameSourceToProjectOperatorRule>(RenameSourceToProjectOperatorRule());
}

}
