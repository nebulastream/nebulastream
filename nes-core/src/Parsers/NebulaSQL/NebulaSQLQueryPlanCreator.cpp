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

#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Parsers/NebulaSQL/NebulaSQLQueryPlanCreator.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <regex>

namespace NES::Parsers {

        std::string queryPlanToString(const QueryPlanPtr queryPlan) {
            std::regex r2("[0-9]");
            std::regex r1("  ");
            std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r1, "");
            queryPlanStr = std::regex_replace(queryPlanStr, r2, "");
            return queryPlanStr;
        }

        QueryPlanPtr NebulaSQLQueryPlanCreator::getQueryPlan() const {
            std::cout << queryPlanToString(completeQueryPlan) <<"\n";

            return completeQueryPlan;

            /**

            //ExpressionNodePtr predicate = helper.getWhereClauses().at(0);
            //queryPlan = QueryPlanBuilder::addFilter(predicate, queryPlan);
            if (!helper.getProjectionFields().empty())
                queryPlan = QueryPlanBuilder::addProjection(helper.getProjectionFields(), queryPlan);
            //queryPlan = QueryPlanBuilder::addLimit(helper.getLimit(), queryPlan);
            //queryPlan = QueryPlanBuilder::addRename(helper.getNewName(),queryPlan);
            //queryPlan = QueryPlanBuilder::addMap(helper.getMapExpression(),queryPlan);
            // TODO: Implement logic to handle the union of 2 queryPlan
            //queryPlan = QueryPlanBuilder::addUnion(queryPlan,queryPlan);
            //queryPlan = QueryPlanBuilder::addJoin();
            //queryPlan = QueryPlanBuilder::assignWatermark(queryPlan,helper.getWatermarkStrategieDescriptor());
            //queryPlan = QueryPlanBuilder::checkAndAddWatermarkAssignment(queryPlan,helper.getWindowType());
            **/
        }

        void NebulaSQLQueryPlanCreator::poppush(NebulaSQLHelper helper){
            helpers.pop();
            helpers.push(helper);
        }

        void NebulaSQLQueryPlanCreator::enterSelectClause(NebulaSQLParser::SelectClauseContext* context) {
            helpers.top().isSelect=true;
            NebulaSQLBaseListener::enterSelectClause(context);

        }

        void NebulaSQLQueryPlanCreator::enterFromClause(NebulaSQLParser::FromClauseContext* context) {
            helpers.top().isFrom= true;
            NebulaSQLBaseListener::enterFromClause(context);

        }

        void NebulaSQLQueryPlanCreator::enterSinkClause(NebulaSQLParser::SinkClauseContext *context) {
            if (context->sinkType()) {
                auto sinkType = context->sinkType()->getText();
                SinkDescriptorPtr sinkDescriptor;
                if (sinkType == "PRINT") {
                    sinkDescriptor = NES::PrintSinkDescriptor::create();
                }
                if (sinkType == "FILE") {
                    sinkDescriptor = NES::FileSinkDescriptor::create(context->getText());
                }
                if (sinkType == "MQTT") {
                    auto mqttContext = context->sinkType()->sinkTypeMQTT();
                    std::string adress = mqttContext->mqttHostLabel->getText();
                    std::string topic = mqttContext->topic->getText();
                    std::string user = mqttContext->user->getText();
                    int maxBufferedMSGs = std::stoi(mqttContext->maxBufferedMSGs->getText());
                    std::string timeUnits = mqttContext->mqttTimeUnitLabel->getText();
                    MQTTSinkDescriptor::TimeUnits timeUnit = MQTTSinkDescriptor::TimeUnits::seconds;
                    if (timeUnits == "nanoseconds"){
                        timeUnit = MQTTSinkDescriptor::TimeUnits::nanoseconds;
                    }
                    if (timeUnits == "milliseconds"){
                        timeUnit = MQTTSinkDescriptor::TimeUnits::milliseconds;
                    }

                    int messageDelay = std::stoi(mqttContext->messageDelay->getText());
                    MQTTSinkDescriptor::ServiceQualities qos = MQTTSinkDescriptor::ServiceQualities::exactlyOnce;
                    if(mqttContext->qualityOfService->getText() == "atMostOnce"){
                        qos = MQTTSinkDescriptor::ServiceQualities::atMostOnce;
                    }
                    if(mqttContext->qualityOfService->getText() == "atLeastOnce"){
                        qos = MQTTSinkDescriptor::ServiceQualities::atLeastOnce;
                    }
                    bool asynchronousClient = mqttContext->asynchronousClient->getText() == "true";
                    sinkDescriptor = NES::MQTTSinkDescriptor::create(std::move(adress),std::move(topic),std::move(user),maxBufferedMSGs,timeUnit,messageDelay,qos,asynchronousClient);
                }
                if (sinkType == "ZMQ") {
                    auto zmqContext = context->sinkType()->sinkTypeZMQ();
                    std::string host = zmqContext->host()->getText();
                    int port = std::stoi(zmqContext->port()->getText());
                    sinkDescriptor = NES::ZmqSinkDescriptor::create(host,port);
                }
                if (sinkType == "Kafka") {
                    auto kafkaContext = context->sinkType()->sinkTypeKafka();
                    std::string sinkFormat = kafkaContext->kafkaKeyword()->getText();
                    std::string broker = kafkaContext->broker->getText();
                    std::string topic = kafkaContext->topic->getText();
                    uint64_t timeout = std::stoull(kafkaContext->timeout->getText());
                    sinkDescriptor = NES::KafkaSinkDescriptor::create(sinkFormat,topic,broker,timeout);
                }
                if (sinkType == "OPC") {
                    auto opcContext = context->sinkType()->sinkTypeOPC();
                    std::string url = opcContext->url->getText();
                    std::string nodeId = opcContext->nodeId->getText();
                    std::string user = opcContext->user->getText();
                    std::string password = opcContext->password->getText();
                    //sinkDescriptor = NES::OPCSinkDescriptor::create(url,nodeId,user,password);
                    //gibt es nicht?
                }
                if (sinkType == "NullOutput") {
                    sinkDescriptor = NES::NullOutputSinkDescriptor::create();
                }
                NebulaSQLHelper helper = helpers.top();
                helper.addSink(sinkDescriptor);
                poppush(helper);
            }
        }

        void NebulaSQLQueryPlanCreator::exitNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext* context) {
            ExpressionNodePtr expressionNode;
            NebulaSQLHelper helper = helpers.top();
            for (auto namedExpression : context->namedExpression()) {
                auto attributeField = NES::Attribute(context->getText());
                helper.addProjectionField(attributeField);
            }
            poppush(helper);

        }
        void NebulaSQLQueryPlanCreator::enterNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext* context) {
            NebulaSQLBaseListener::enterNamedExpressionSeq(context);

        }

        void NebulaSQLQueryPlanCreator::exitLogicalBinary(NebulaSQLParser::LogicalBinaryContext* context) {

            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;

            auto rightExpression = helper.ExpressionBuilder.back();
            helper.ExpressionBuilder.pop_back();
            auto leftExpression = helper.ExpressionBuilder.back();
            helper.ExpressionBuilder.pop_back();

            std::string opText = context->op->getText();

            if (opText == "AND") {
                expression = NES::AndExpressionNode::create(leftExpression, rightExpression);
            }
            else if (opText == "OR") {
                expression = NES::OrExpressionNode::create(leftExpression, rightExpression);
            }

            helpers.top().addExpression(expression);
        }
        void NebulaSQLQueryPlanCreator::exitSelectClause(NebulaSQLParser::SelectClauseContext* context) {
            auto temp = context->getText();

            helpers.top().isSelect= false;
            NebulaSQLBaseListener::exitSelectClause(context);

        }
        void NebulaSQLQueryPlanCreator::exitFromClause(NebulaSQLParser::FromClauseContext* context) {
            helpers.top().isFrom= false;
            //helpers.top().addSource(context->relation(sourceCounter)->getText());
            NebulaSQLBaseListener::exitFromClause(context);
        }
        void NebulaSQLQueryPlanCreator::enterWhereClause(NebulaSQLParser::WhereClauseContext* context) {
            helpers.top().isWhere= true;
            NebulaSQLHelper helper = helpers.top();
            helper.ExpressionBuilder.clear();
            poppush(helper);
            NebulaSQLBaseListener::enterWhereClause(context);

        }
        void NebulaSQLQueryPlanCreator::exitWhereClause(NebulaSQLParser::WhereClauseContext* context) {
            helpers.top().isWhere= false;
            NebulaSQLHelper helper = helpers.top();
            if(helper.ExpressionBuilder.size()!=1){
                NES_ERROR("Zuviele Expressions oder zuwenige,warum??")
            }
            helper.addExpression(helper.ExpressionBuilder.back());
            poppush(helper);
            NebulaSQLBaseListener::exitWhereClause(context);

        }
        void NebulaSQLQueryPlanCreator::enterComparisonOperator(NebulaSQLParser::ComparisonOperatorContext* context) {
            NebulaSQLHelper helper = helpers.top();

            helper.opBoolean = context->getText();
            auto temp = context->getText();
            poppush(helper);
            NebulaSQLBaseListener::enterComparisonOperator(context);
        }
        void NebulaSQLQueryPlanCreator::enterConstantDefault(NebulaSQLParser::ConstantDefaultContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::enterConstantDefault(context);
        }
        void NebulaSQLQueryPlanCreator::enterArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext* context) {
            auto temp = context->getText();

            NES::ExpressionNodePtr arithmeticBinaryExpr = NES::Attribute(context->getText()).getExpressionNode();
            //helper.addArithmeticBinaryExpression(arithmeticBinaryExpr);
        }
        void NebulaSQLQueryPlanCreator::exitArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext* context) {
            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;

            auto rightExpression = helper.ExpressionBuilder.back();
            helper.ExpressionBuilder.pop_back();
            auto leftExpression = helper.ExpressionBuilder.back();
            helper.ExpressionBuilder.pop_back();

            std::string opText = context->op->getText();
            if (opText == "*"){
                expression = leftExpression * rightExpression;
            }
            else if (opText == "/"){
                expression = leftExpression / rightExpression;
            }
            else if (opText == "+"){
                expression = leftExpression + rightExpression;
            }
            else if (opText == "-"){
                expression = leftExpression - rightExpression;
            }
            helper.ExpressionBuilder.push_back(expression);
            poppush(helper);
        }
        void NebulaSQLQueryPlanCreator::exitArithmeticUnary(NebulaSQLParser::ArithmeticUnaryContext* context){
            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;

            auto innerExpression = helper.ExpressionBuilder.back();
            helper.ExpressionBuilder.pop_back();

            std::string opText = context->op->getText();

            if (opText == "+"){
                expression = innerExpression;
            }
            else if (opText == "-"){
                expression = -1*innerExpression;
            }
            else if (opText == "~"){
                NES_WARNING("Operation tilde not supported yeet")
            }
            helper.ExpressionBuilder.push_back(expression);
            poppush(helper);


        }
        void NebulaSQLQueryPlanCreator::exitErrorCapturingIdentifier(NebulaSQLParser::ErrorCapturingIdentifierContext* context) {
            NebulaSQLBaseListener::exitErrorCapturingIdentifier(context);
        }
        void NebulaSQLQueryPlanCreator::enterUnquotedIdentifier(NebulaSQLParser::UnquotedIdentifierContext* context) {
            NebulaSQLBaseListener::enterUnquotedIdentifier(context);
        }


        void NebulaSQLQueryPlanCreator::enterIdentifier(NebulaSQLParser::IdentifierContext* context) {
            // TODO bei enter identifier aus Where oder Select oder Having Attribute erstellen, und auf die liste pushen

            NebulaSQLHelper helper = helpers.top();
            auto parentContext = static_cast<NebulaSQLParser::IdentifierContext*>(context->parent);
            size_t parentRuleIndex = -1;
            if (parentContext != nullptr) {
                parentRuleIndex = parentContext->getRuleIndex();
            }
            if(helper.isWhere && NebulaSQLParser::RulePrimaryExpression == parentRuleIndex){
                helper.ExpressionBuilder.push_back(NES::Attribute(context->getText()));
            }else if(helper.isFrom && !helper.isJoinRelation && NebulaSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex){
                helper.addSource(context->getText());
            }else if(helper.isSelect){
                if(NebulaSQLParser::RulePrimaryExpression == parentRuleIndex && !helper.isArithmeticBinary &&!helper.isFunctionCall){
                    //Add Select Attribute to list
                    helper.projections.push_back(NES::Attribute(context->getText()));
                }
                else if(NebulaSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex && !helper.isFunctionCall){
                    if(helper.isArithmeticBinary){
                        // TODO irgendwas mit ner map clause?
                        // queryItem.setMapClause(CommonUtils.formatAttribute(ctx.getText()) + "=" + queryItem.getMapClause());

                    } else{
                        //add rename to previous select attribute
                        auto attr = helper.projections.back();
                        auto cattr = std::dynamic_pointer_cast<ExpressionItem>(attr)->as(context->getText());
                        helper.projections.back() = cattr;
                    }
                }

            }

            poppush(helper);
        }

        void NebulaSQLQueryPlanCreator::enterPrimaryQuery(NebulaSQLParser::PrimaryQueryContext* context) {
            NebulaSQLHelper helper;
            helpers.push(helper);
            NebulaSQLBaseListener::enterPrimaryQuery(context);
        }
        void NebulaSQLQueryPlanCreator::exitPrimaryQuery(NebulaSQLParser::PrimaryQueryContext* context) {
            NebulaSQLHelper helper = helpers.top();
            QueryPlanPtr queryPlan;

            queryPlan = QueryPlanBuilder::createQueryPlan(helper.getSource());

            for (auto& whereExpr : helper.getExpressions()) {
                queryPlan = QueryPlanBuilder::addFilter(whereExpr, queryPlan);
            }

            if(!helper.projections.empty()){
                queryPlan = QueryPlanBuilder::addProjection(helper.projections, queryPlan);
            }

            queryPlan = QueryPlanBuilder::addSink(queryPlan,helper.getSinks().front());

            for(const QueryPlanPtr& qp : helper.queryPlans){
                QueryId subQueryId = qp->getQueryId();
                queryPlan->setQuerySubPlanId(subQueryId);
            }
            helpers.pop();
            if(helpers.empty()){
                completeQueryPlan = queryPlan;
            }else{
                NebulaSQLHelper subQueryHelper = helpers.top();
                subQueryHelper.queryPlans.push_back(queryPlan);
            }
            NebulaSQLBaseListener::exitPrimaryQuery(context);
        }
        void NebulaSQLQueryPlanCreator::enterTimeUnit(NebulaSQLParser::TimeUnitContext* context) {
            NebulaSQLBaseListener::enterTimeUnit(context);
        }
        void NebulaSQLQueryPlanCreator::exitSizeParameter(NebulaSQLParser::SizeParameterContext* context) {
            NebulaSQLBaseListener::exitSizeParameter(context);
        }
        void NebulaSQLQueryPlanCreator::exitTimestampParameter(NebulaSQLParser::TimestampParameterContext* context) {
            NebulaSQLBaseListener::exitTimestampParameter(context);
        }
        void NebulaSQLQueryPlanCreator::exitAdvancebyParameter(NebulaSQLParser::AdvancebyParameterContext* context) {
            NebulaSQLBaseListener::exitAdvancebyParameter(context);
        }
        void NebulaSQLQueryPlanCreator::exitWindowedAggregationClause(NebulaSQLParser::WindowedAggregationClauseContext* context) {
            NebulaSQLBaseListener::exitWindowedAggregationClause(context);
        }
        void NebulaSQLQueryPlanCreator::exitTumblingWindow(NebulaSQLParser::TumblingWindowContext* context) {
            NebulaSQLBaseListener::exitTumblingWindow(context);
        }
        void NebulaSQLQueryPlanCreator::exitSlidingWindow(NebulaSQLParser::SlidingWindowContext* context) {
            NebulaSQLBaseListener::exitSlidingWindow(context);
        }
        void NebulaSQLQueryPlanCreator::exitCountBasedWindow(NebulaSQLParser::CountBasedWindowContext* context) {
            NebulaSQLBaseListener::exitCountBasedWindow(context);
        }
        void NebulaSQLQueryPlanCreator::exitCountBasedTumbling(NebulaSQLParser::CountBasedTumblingContext* context) {
            NebulaSQLBaseListener::exitCountBasedTumbling(context);
        }
        void NebulaSQLQueryPlanCreator::enterNamedExpression(NebulaSQLParser::NamedExpressionContext* context) {
            NebulaSQLBaseListener::enterNamedExpression(context);
        }
        void NebulaSQLQueryPlanCreator::exitNamedExpression(NebulaSQLParser::NamedExpressionContext* context) {
            NebulaSQLBaseListener::exitNamedExpression(context);
        }
        void NebulaSQLQueryPlanCreator::enterFunctionCall(NebulaSQLParser::FunctionCallContext* context) {
            NebulaSQLBaseListener::enterFunctionCall(context);
        }
        void NebulaSQLQueryPlanCreator::exitFunctionCall(NebulaSQLParser::FunctionCallContext* context) {
            NebulaSQLBaseListener::exitFunctionCall(context);
        }
        void NebulaSQLQueryPlanCreator::enterHavingClause(NebulaSQLParser::HavingClauseContext* context) {
            NebulaSQLBaseListener::enterHavingClause(context);
        }
        void NebulaSQLQueryPlanCreator::exitHavingClause(NebulaSQLParser::HavingClauseContext* context) {
            NebulaSQLBaseListener::exitHavingClause(context);
        }
        void NebulaSQLQueryPlanCreator::enterComparison(NebulaSQLParser::ComparisonContext* context) {
            auto temp = context->getText();
            NebulaSQLBaseListener::enterComparison(context);
        }
        void NebulaSQLQueryPlanCreator::exitComparison(NebulaSQLParser::ComparisonContext* context) {

            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;

            auto rightExpression = helper.ExpressionBuilder.back();
            helper.ExpressionBuilder.pop_back();
            auto leftExpression = helper.ExpressionBuilder.back();
            helper.ExpressionBuilder.pop_back();
            std::string op = context->children.at(1)->getText();
            if (op == ">") {
                expression = NES::GreaterExpressionNode::create(leftExpression, rightExpression);
            }
            else if (op == "<") {
                expression = NES::LessExpressionNode::create(leftExpression, rightExpression);
            }
            else if (op == ">=") {
                expression = NES::GreaterEqualsExpressionNode::create(leftExpression, rightExpression);
            }
            else if (op == "<=") {
                expression = NES::LessEqualsExpressionNode::create(leftExpression, rightExpression);
            }
            else if (op == "==") {
                expression = NES::EqualsExpressionNode::create(leftExpression, rightExpression);
            }
            helper.ExpressionBuilder.push_back(expression);
            poppush(helper);
            NebulaSQLBaseListener::enterComparison(context);
        }
        void NebulaSQLQueryPlanCreator::exitAggregationClause(NebulaSQLParser::AggregationClauseContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::exitAggregationClause(context);
        }
        void NebulaSQLQueryPlanCreator::enterJoinRelation(NebulaSQLParser::JoinRelationContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::enterJoinRelation(context);
        }
        void NebulaSQLQueryPlanCreator::enterColumnReference(NebulaSQLParser::ColumnReferenceContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::enterColumnReference(context);
        }
        void NebulaSQLQueryPlanCreator::enterDereference(NebulaSQLParser::DereferenceContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::enterDereference(context);
        }
        void NebulaSQLQueryPlanCreator::exitDereference(NebulaSQLParser::DereferenceContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::exitDereference(context);
        }
        void NebulaSQLQueryPlanCreator::exitJoinCriteria(NebulaSQLParser::JoinCriteriaContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::exitJoinCriteria(context);
        }
        void NebulaSQLQueryPlanCreator::enterTableName(NebulaSQLParser::TableNameContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::enterTableName(context);
        }
        void NebulaSQLQueryPlanCreator::enterWatermarkClause(NebulaSQLParser::WatermarkClauseContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::enterWatermarkClause(context);
        }
        void NebulaSQLQueryPlanCreator::enterWatermarkParameters(NebulaSQLParser::WatermarkParametersContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::enterWatermarkParameters(context);
        }
        void NebulaSQLQueryPlanCreator::exitWatermarkClause(NebulaSQLParser::WatermarkClauseContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::exitWatermarkClause(context);
        }
        void NebulaSQLQueryPlanCreator::exitSingleStatement(NebulaSQLParser::SingleStatementContext* context) {
            auto temp = context->getText();

            NebulaSQLBaseListener::exitSingleStatement(context);
        }
        void NebulaSQLQueryPlanCreator::enterPredicated(NebulaSQLParser::PredicatedContext* context){
            NebulaSQLBaseListener::enterPredicated(context);
        }

        void NebulaSQLQueryPlanCreator::exitPredicated(NebulaSQLParser::PredicatedContext* context) {
            /*
            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;
            if (context->children.size()>=3) {
                auto rightExpression = helper.ExpressionBuilder.back();
                helper.ExpressionBuilder.pop_back();
                auto leftExpression = helper.ExpressionBuilder.back();
                helper.ExpressionBuilder.pop_back();

                if (helper.opBoolean == "<") {
                    expression = NES::LessExpressionNode::create(leftExpression, rightExpression);
                }
                else if (helper.opBoolean == "<=") {
                    expression = NES::LessEqualsExpressionNode::create(leftExpression, rightExpression);
                }
                else if (helper.opBoolean == ">") {
                    expression = NES::GreaterExpressionNode::create(leftExpression, rightExpression);
                }
                else if (helper.opBoolean == ">=") {
                    expression = NES::GreaterEqualsExpressionNode::create(leftExpression, rightExpression);
                }
                else if (helper.opBoolean == "==") {
                    expression = NES::EqualsExpressionNode::create(leftExpression, rightExpression);
                }
                else if (helper.opBoolean == "&&") {
                    expression = NES::AndExpressionNode::create(leftExpression, rightExpression);
                }
                else if (helper.opBoolean == "||") {
                    expression = NES::OrExpressionNode::create(leftExpression, rightExpression);
                }
                else if (helper.opBoolean == "AND") {
                    expression = NES::AndExpressionNode::create(leftExpression, rightExpression);
                }
                else if (helper.opBoolean == "OR") {
                    expression = NES::OrExpressionNode::create(leftExpression, rightExpression);
                }

                helper.tempExpressionVectorBoolean.push_back(expression);
                poppush(helper);
            }
             */
            NebulaSQLBaseListener::exitPredicated(context);

        }
void NebulaSQLQueryPlanCreator::exitLogicalNot(NebulaSQLParser::LogicalNotContext* context) {
            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;

            auto innerExpression = helper.ExpressionBuilder.back();
            helper.ExpressionBuilder.pop_back();

            expression = !innerExpression;
            helper.ExpressionBuilder.push_back(expression);
            poppush(helper);
            NebulaSQLBaseListener::exitLogicalNot(context);
}

void NebulaSQLQueryPlanCreator::exitConstantDefault(NebulaSQLParser::ConstantDefaultContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.ExpressionBuilder.push_back(NES::Attribute(context->getText()));
            poppush(helper);
}




    }// namespace NES::Parsers

