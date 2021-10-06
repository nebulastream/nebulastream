/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRule.hpp>

namespace NES::Optimizer {

ProjectBeforeUnionOperatorRulePtr ProjectBeforeUnionOperatorRule::create() {
    return std::make_shared<ProjectBeforeUnionOperatorRule>(ProjectBeforeUnionOperatorRule());
}

QueryPlanPtr ProjectBeforeUnionOperatorRule::apply(QueryPlanPtr queryPlan) {

    NES_INFO("Apply ProjectBeforeUnionOperatorRule");
    auto unionOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    for (auto& unionOperator : unionOperators) {
        auto rightInputSchema = unionOperator->getRightInputSchema();
        auto leftInputSchema = unionOperator->getLeftInputSchema();
        //Only apply the rule when right side and left side schema are different
        if (!rightInputSchema->equals(leftInputSchema, false)) {
            //Construct project operator for mapping rightInputStream To leftInputStream
            auto projectOperator = constructProjectOperator(rightInputSchema, leftInputSchema);
            auto childrenToUnionOperator = unionOperator->getChildren();
            for (auto& child : childrenToUnionOperator) {
                auto childOutputSchema = child->as<LogicalOperatorNode>()->getOutputSchema();
                //Find the child that matches the right schema and inset the project operator there
                if (rightInputSchema->equals(childOutputSchema, false)) {
                    child->insertBetweenThisAndParentNodes(projectOperator);
                    break;
                }
            }
        }
    }
    return queryPlan;
}

LogicalOperatorNodePtr ProjectBeforeUnionOperatorRule::constructProjectOperator(const SchemaPtr& sourceSchema,
                                                                                const SchemaPtr& destinationSchema) {
    NES_TRACE("Computing Projection operator for Source Schema " << sourceSchema->toString() << " and Destination schema "
                                                                 << destinationSchema->toString());
    //Fetch source and destination schema fields
    auto sourceFields = sourceSchema->fields;
    auto destinationFields = destinationSchema->fields;
    std::vector<ExpressionNodePtr> projectExpressions;
    //Compute projection expressions
    for (uint64_t i = 0; i < sourceSchema->getSize(); i++) {
        auto field = sourceFields[i];
        auto updatedFieldName = destinationFields[i]->getName();
        //Compute field access and field rename expression
        auto originalField = FieldAccessExpressionNode::create(field->getDataType(), field->getName());
        auto fieldRenameExpression =
            FieldRenameExpressionNode::create(originalField->as<FieldAccessExpressionNode>(), updatedFieldName);
        projectExpressions.push_back(fieldRenameExpression);
    }
    //Create Projection operator
    return LogicalOperatorFactory::createProjectionOperator(projectExpressions);
}

}// namespace NES::Optimizer
