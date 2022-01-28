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
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/RenameStreamOperatorNode.hpp>
#include <Optimizer/QueryRewrite/RenameStreamToProjectOperatorRule.hpp>

namespace NES::Optimizer {

QueryPlanPtr RenameStreamToProjectOperatorRule::apply(QueryPlanPtr queryPlan) {
    NES_DEBUG("RenameStreamToProjectOperatorRule: Convert all Rename Stream operator to the project operator");
    auto renameStreamOperators = queryPlan->getOperatorByType<RenameStreamOperatorNode>();
    //Iterate over all rename stream operators and convert them to project operator
    for (auto& renameStreamOperator : renameStreamOperators) {
        //Convert the rename stream operator to project operator
        auto projectOperator = convert(renameStreamOperator);
        //Replace rename stream operator with the project operator
        renameStreamOperator->replace(projectOperator);
        //Assign the project operator the id of as operator
        projectOperator->setId(renameStreamOperator->getId());
    }
    //Return updated query plan
    return queryPlan;
}

OperatorNodePtr RenameStreamToProjectOperatorRule::convert(const OperatorNodePtr& operatorNode) {
    //Fetch the new stream name and input schema for the as operator
    auto renameStreamOperator = operatorNode->as<RenameStreamOperatorNode>();
    auto newStreamName = renameStreamOperator->getNewStreamName();
    auto inputSchema = renameStreamOperator->getInputSchema();

    std::vector<ExpressionNodePtr> projectionAttributes;
    //Iterate over the input schema and add a new field rename expression
    for (const auto& field : inputSchema->fields) {
        //compute the new name for the field by added new stream name as field qualifier
        std::string fieldName = field->getName();
        //Compute new name without field qualifier
        std::string updatedFieldName = newStreamName + Schema::ATTRIBUTE_NAME_SEPARATOR + fieldName;
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

RenameStreamToProjectOperatorRulePtr RenameStreamToProjectOperatorRule::create() {
    return std::make_shared<RenameStreamToProjectOperatorRule>(RenameStreamToProjectOperatorRule());
}

}// namespace NES::Optimizer
