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

#include <Parsers/NebulaSQL/NebulaSQLQueryPlanCreator.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>


namespace NES::Parsers {

QueryPlanPtr buildQuery() {
    QueryPlanPtr queryPlan;
    return queryPlan;
}

void NebulaSQLQueryPlanCreator::enterSelectClause(NebulaSQLParser::SelectClauseContext* context) {
    for (auto namedExprContext : context->namedExpressionSeq()->namedExpression()) {
        auto expressionText = namedExprContext->expression()->getText();
        auto attribute = NES::Attribute(expressionText).getExpressionNode();
        helper.addProjectionField(attribute);
        // catach den alias
        if (namedExprContext->name) {
            std::string alias = namedExprContext->name->getText();
            //Stream Name ändern?
        }
    }
}

void NebulaSQLQueryPlanCreator::enterRelation(NebulaSQLParser::RelationContext* context) {
    if (context->relationPrimary()) {
        auto relationPrimaryCtx = context->relationPrimary();
        if (auto multipartIdentifierCtx = dynamic_cast<NebulaSQLParser::MultipartIdentifierContext*>(relationPrimaryCtx->children[0])) {
            helper.addSource(multipartIdentifierCtx->getText());
        }
    }
}
void NebulaSQLQueryPlanCreator::enterSinkClause(NebulaSQLParser::SinkClauseContext *context) {
    if (context->sinkType()) {
        auto sinkType = context->sinkType()->getText();
        SinkDescriptorPtr sinkDescriptor;
        if (sinkType == "Print") {
            sinkDescriptor = NES::PrintSinkDescriptor::create();
        }
        if (sinkType == "File") {
            sinkDescriptor = NES::FileSinkDescriptor::create(context->getText());
        }
        if (sinkType == "MQTT") {
            sinkDescriptor = NES::NullOutputSinkDescriptor::create();
        }
        if (sinkType == "Network") {
            sinkDescriptor = NES::NullOutputSinkDescriptor::create();
        }
        if (sinkType == "NullOutput") {
            sinkDescriptor = NES::NullOutputSinkDescriptor::create();
        }
        helper.setSink(sinkDescriptor);
    }
}

void NebulaSQLQueryPlanCreator::exitLogicalBinary(NebulaSQLParser::LogicalBinaryContext* context) {
    auto leftExpression = NES::Attribute(context->left->getText()).getExpressionNode();
    auto rightExpression = NES::Attribute(context->right->getText()).getExpressionNode();
    NES::ExpressionNodePtr expression;
    if (context->op->getText() == "AND") {
        expression = NES::AndExpressionNode::create(leftExpression, rightExpression);
    }
    else if (context->op->getText() == "OR") {
    expression = NES::OrExpressionNode::create(leftExpression, rightExpression);
}
    helper.addWhereClause(expression);
}

}// namespace NES::Parsers