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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Optimizer/QueryRewrite/RenameSourceToProjectOperatorRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

QueryPlanPtr RenameSourceToProjectOperatorRule::apply(QueryPlanPtr queryPlan) {
    NES_DEBUG("RenameSourceToProjectOperatorRule: Convert all Rename Source operator to the project operator");
    auto renameSourceOperators = queryPlan->getOperatorByType<RenameSourceOperator>();
    //Iterate over all rename source operators and convert them to project operator
    for (auto& renameSourceOperator : renameSourceOperators) {
        //Convert the rename source operator to project operator
        auto projectOperator = convert(renameSourceOperator);
        //Replace rename source operator with the project operator
        renameSourceOperator->replace(projectOperator);
        //Assign the project operator the id of as operator
        projectOperator->setId(renameSourceOperator->getId());
    }
    //Return updated query plan
    return queryPlan;
}

OperatorPtr RenameSourceToProjectOperatorRule::convert(const OperatorPtr& operatorNode) {
    //Fetch the new source name and input schema for the as operator
    auto renameSourceOperator = operatorNode->as<RenameSourceOperator>();
    auto newSourceName = renameSourceOperator->getNewSourceName();
    auto inputSchema = renameSourceOperator->getInputSchema();

    std::vector<ExpressionNodePtr> projectionAttributes;
    //Iterate over the input schema and add a new field rename expression
    for (const auto& field : inputSchema->fields) {
        //compute the new name for the field by added new source name as field qualifier
        std::string fieldName = field->getName();
        //Compute new name without field qualifier
        std::string updatedFieldName = newSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR + fieldName;
        //Compute field access and field rename expression
        auto originalField = FieldAccessExpressionNode::create(field->getDataType(), fieldName);
        auto fieldRenameExpression =
            FieldRenameExpressionNode::create(originalField->as<FieldAccessExpressionNode>(), updatedFieldName);
        projectionAttributes.push_back(fieldRenameExpression);
    }
    //Construct a new project operator
    auto projectOperator = LogicalOperatorFactory::createProjectionOperator(projectionAttributes);
    return projectOperator;
}

RenameSourceToProjectOperatorRulePtr RenameSourceToProjectOperatorRule::create() {
    return std::make_shared<RenameSourceToProjectOperatorRule>(RenameSourceToProjectOperatorRule());
}

}// namespace NES::Optimizer
