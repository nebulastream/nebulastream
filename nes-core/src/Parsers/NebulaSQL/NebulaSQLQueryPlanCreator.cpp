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
#include <Parsers/NebulaSQL/NebulaSQLQueryPlanCreator.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <regex>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Actions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Actions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/TriggerPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>

namespace NES::Parsers {
        std::string queryPlanToString(const QueryPlanPtr queryPlan) {
            std::regex r2("[0-9]");
            std::regex r1("  ");
            std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r1, "");
            queryPlanStr = std::regex_replace(queryPlanStr, r2, "");
            return queryPlanStr;
        }

        QueryPlanPtr NebulaSQLQueryPlanCreator::getQueryPlan() const {
            if(sink != nullptr){
                return QueryPlanBuilder::addSink(queryPlans.top(),sink);

            }else{
                return queryPlans.top();
            }
        }
        Windowing::TimeMeasure NebulaSQLQueryPlanCreator::buildTimeMeasure(int size, const std::string& timebase){


            if (timebase == "MS") {
                return  Milliseconds(size) ;
            } else if (timebase == "SEC") {
                return Seconds(size);
            } else if (timebase == "MIN"){
                return Minutes(size);
            } else if (timebase == "HOUR"){
                return Hours(size);
            } else {
                return Milliseconds(0);
            }
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
                sink = sinkDescriptor;
            }
        }


        void NebulaSQLQueryPlanCreator::enterNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.expressionBuilder.clear();
            poppush(helper);
            NebulaSQLBaseListener::enterNamedExpressionSeq(context);
        }

        void NebulaSQLQueryPlanCreator::exitLogicalBinary(NebulaSQLParser::LogicalBinaryContext* context) {

            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;

            auto rightExpression = helper.expressionBuilder.back();
            helper.expressionBuilder.pop_back();
            auto leftExpression = helper.expressionBuilder.back();
            helper.expressionBuilder.pop_back();

            std::string opText = context->op->getText();

            if (opText == "AND") {
                expression = NES::AndExpressionNode::create(leftExpression, rightExpression);
            }
            else if (opText == "OR") {
                expression = NES::OrExpressionNode::create(leftExpression, rightExpression);
            }

            helper.expressionBuilder.push_back(expression);
            poppush(helper);
        }
        void NebulaSQLQueryPlanCreator::exitSelectClause(NebulaSQLParser::SelectClauseContext* context) {
            ExpressionNodePtr expressionNode;
            NebulaSQLHelper helper = helpers.top();
            for (const ExpressionNodePtr& selectExpression : helper.expressionBuilder) {
                helper.addProjectionField(selectExpression);
            }
            helper.expressionBuilder.clear();
            poppush(helper);
            helpers.top().isSelect= false;
            NebulaSQLBaseListener::exitSelectClause(context);

        }
        void NebulaSQLQueryPlanCreator::exitFromClause(NebulaSQLParser::FromClauseContext* context) {
            helpers.top().isFrom= false;
            NebulaSQLBaseListener::exitFromClause(context);
        }
        void NebulaSQLQueryPlanCreator::enterWhereClause(NebulaSQLParser::WhereClauseContext* context) {
            helpers.top().isWhereOrHaving = true;
            NebulaSQLHelper helper = helpers.top();
            poppush(helper);
            NebulaSQLBaseListener::enterWhereClause(context);

        }
        void NebulaSQLQueryPlanCreator::exitWhereClause(NebulaSQLParser::WhereClauseContext* context) {
            helpers.top().isWhereOrHaving = false;
            NebulaSQLHelper helper = helpers.top();
            if(helper.expressionBuilder.size()!=1){
                NES_ERROR("Zuviele Expressions oder zuwenige,warum??");
            }
            helper.addWhereClause(helper.expressionBuilder.back());
            helper.expressionBuilder.clear();
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
        void NebulaSQLQueryPlanCreator::exitArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext* context) {
            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;

            auto rightExpression = helper.expressionBuilder.back();
            helper.expressionBuilder.pop_back();
            auto leftExpression = helper.expressionBuilder.back();
            helper.expressionBuilder.pop_back();

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
            else if (opText == "%"){
                expression = leftExpression % rightExpression;
            } else {
                NES_ERROR("Unknown Arithmetic Binary Operator");
            }
            helper.expressionBuilder.push_back(expression);
            poppush(helper);
        }
        void NebulaSQLQueryPlanCreator::exitArithmeticUnary(NebulaSQLParser::ArithmeticUnaryContext* context){
            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;

            auto innerExpression = helper.expressionBuilder.back();
            helper.expressionBuilder.pop_back();

            std::string opText = context->op->getText();

            if (opText == "+"){
                expression = innerExpression;
            }
            else if (opText == "-"){
                expression = -1*innerExpression;
            }
            else if (opText == "~"){
                NES_WARNING("Operation tilde not supported yeet");
            } else {
                NES_ERROR("Unknown Arithmetic Unary Operator");
            }
            helper.expressionBuilder.push_back(expression);
            poppush(helper);


        }

        void NebulaSQLQueryPlanCreator::enterUnquotedIdentifier(NebulaSQLParser::UnquotedIdentifierContext* context) {
            NebulaSQLHelper helper = helpers.top();

            // Get Index of  Parent Rule to check type of parent rule in conditions
            auto parentContext = static_cast<NebulaSQLParser::IdentifierContext*>(context->parent);
            size_t parentRuleIndex = -1;
            if (parentContext != nullptr) {
                parentRuleIndex = parentContext->getRuleIndex();
            }

            if(helper.isFrom && !helper.isJoinRelation){
                helper.newSourceName = context->getText();
            } else if (helper.isJoinRelation && parentRuleIndex == NebulaSQLParser::RuleTableAlias){
                helper.joinSourceRenames.emplace_back(context->getText());
            }
            poppush(helper);
            NebulaSQLBaseListener::enterUnquotedIdentifier(context);
        }


        void NebulaSQLQueryPlanCreator::enterIdentifier(NebulaSQLParser::IdentifierContext* context) {
            NebulaSQLHelper helper = helpers.top();

            // Get Index of  Parent Rule to check type of parent rule in conditions
            auto parentContext = static_cast<NebulaSQLParser::IdentifierContext*>(context->parent);
            size_t parentRuleIndex = -1;
            if (parentContext != nullptr) {
                parentRuleIndex = parentContext->getRuleIndex();
            }

            if(( helper.isWhereOrHaving || helper.isSelect || helper.isWindow ) && NebulaSQLParser::RulePrimaryExpression == parentRuleIndex){
                // add identifiers in select, window, where and having clauses to the expression builder list
                helper.expressionBuilder.push_back(NES::Attribute(context->getText()));
            }else if(helper.isFrom && !helper.isJoinRelation && NebulaSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex){
                // get main source name
                helper.addSource(context->getText());
            }
            else if(NebulaSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex && !helper.isFunctionCall && !helper.isJoinRelation){
                // handle renames of identifiers
                if(helper.isArithmeticBinary){
                    NES_THROW_RUNTIME_ERROR("Why are we here? Just to suffer?");
                }
                else if (( helper.isWhereOrHaving || helper.isSelect )){
                    ExpressionNodePtr attr = helper.expressionBuilder.back();
                    helper.expressionBuilder.pop_back();
                    if(helper.identCountHelper == 1){
                        // rename of a single attribute
                        ExpressionItem cattr = static_cast<ExpressionItem>(attr);
                        cattr = cattr.as(context->getText());
                        helper.expressionBuilder.push_back(cattr);
                    } else {
                        // rename of an expression
                        helper.mapBuilder.push_back(Attribute(context->getText()) = attr);
                    }


                }
            }
            else if (helper.isFunctionCall && NebulaSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex) {
                auto aggFunc = helper.windowAggs.back();
                helper.windowAggs.pop_back();
                aggFunc = aggFunc->as(Attribute(context->getText()));
                helper.windowAggs.push_back(aggFunc);
            }
            else if(helper.isJoinRelation && NebulaSQLParser::RulePrimaryExpression == parentRuleIndex){
                helper.joinKeyRelationHelper.push_back(Attribute(context->getText()));
            }
            else if(helper.isJoinRelation && NebulaSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex){
                helper.joinSources.push_back(context->getText());
            }
            poppush(helper);
        }


        void NebulaSQLQueryPlanCreator::enterPrimaryQuery(NebulaSQLParser::PrimaryQueryContext* context) {
            if(!helpers.empty() && !helpers.top().isFrom && !helpers.top().isSetOperation){
                NES_ERROR("Subqueries are only supported in FROM clauses");
            }

            NebulaSQLHelper helper;

            // Get Index of  Parent Rule to check type of parent rule in conditions
            auto parentContext = static_cast<NebulaSQLParser::IdentifierContext*>(context->parent);
            size_t parentRuleIndex = -1;
            if (parentContext != nullptr) {
                parentRuleIndex = parentContext->getRuleIndex();
            }

            // PrimaryQuery is a queryterm too, but if it's a child of a queryterm we are in a union!
            if (parentRuleIndex == NebulaSQLParser::RuleQueryTerm){
                helper.isSetOperation = true;
            }

            helpers.push(helper);
            NebulaSQLBaseListener::enterPrimaryQuery(context);
        }
        void NebulaSQLQueryPlanCreator::exitPrimaryQuery(NebulaSQLParser::PrimaryQueryContext* context) {
            NebulaSQLHelper helper = helpers.top();
            QueryPlanPtr queryPlan;

            if (!helper.queryPlans.empty()){
                queryPlan = helper.queryPlans[0];
            } else {
                queryPlan = QueryPlanBuilder::createQueryPlan(helper.getSource());
            }
            if(!helper.newSourceName.empty() && helper.newSourceName != helper.getSource()){
                queryPlan = QueryPlanBuilder::addRename(helper.newSourceName, queryPlan);
            }
            if(helper.windowType != nullptr && helper.joinSources.empty()){
                if (helper.isTimeBasedWindow){
                    queryPlan = QueryPlanBuilder::checkAndAddWatermarkAssignment(queryPlan, helper.windowType);
                }
                auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
                auto distributionType = Windowing::DistributionCharacteristic::createCompleteWindowType();
                auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

                auto windowDefinition = Windowing::LogicalWindowDefinition::create(helper.windowAggs,
                                                                                   helper.windowType,
                                                                                   distributionType,
                                                                                   triggerPolicy,
                                                                                   triggerAction,
                                                                                   0);
                auto op = LogicalOperatorFactory::createWindowOperator(windowDefinition);
                queryPlan->appendOperatorAsNewRoot(op);


            } else{
                // Join Handling
                //  Vector von StreamName, Linkes Join Attribute, Recchtes Join Attribute
                for (unsigned long i = 0; i < helper.joinSources.size(); i++) {
                    auto queryPlanRight = QueryPlanBuilder::createQueryPlan(helper.joinSources[i]);
                    if(!helper.joinSourceRenames[i].empty() && helper.joinSourceRenames[i] != helper.joinSources[i]){
                        queryPlanRight = QueryPlanBuilder::addRename(helper.joinSourceRenames[i], queryPlanRight);
                    }
                    auto leftKey = helper.joinKeys[i].first;
                    auto rightKey = helper.joinKeys[i].second;
                    queryPlan = QueryPlanBuilder::addJoin(queryPlan,queryPlanRight, leftKey, rightKey, helper.windowType, Join::LogicalJoinDefinition::JoinType::INNER_JOIN);
                }
            }
            if(!helper.getProjectionFields().empty() && helper.windowType == nullptr){
                queryPlan = QueryPlanBuilder::addProjection(helper.getProjectionFields(), queryPlan);
            }
            for (auto& whereExpr : helper.getWhereClauses()) {
                queryPlan = QueryPlanBuilder::addFilter(whereExpr, queryPlan);
            }



            for (const auto& mapExpr : helper.mapBuilder) {
                queryPlan = QueryPlanBuilder::addMap(mapExpr, queryPlan);
            }

            if(helper.windowType != nullptr) {
                for (auto& havingExpr : helper.getHavingClauses()) {
                    queryPlan = QueryPlanBuilder::addFilter(havingExpr, queryPlan);
                }
            }
            //alternative SubQueryHandling
            /*
            for(const QueryPlanPtr& qp : helper.queryPlans){
                QueryId subQueryId = qp->getQueryId();
                // TODO there is only one querySubPlanId that gets overwritten for each subquery
                queryPlan->setQuerySubPlanId(subQueryId);
            }
             */
            helpers.pop();
            if(helpers.empty() || helper.isSetOperation){
                queryPlans.push(queryPlan);
            }else{
                NebulaSQLHelper subQueryHelper = helpers.top();
                subQueryHelper.queryPlans.push_back(queryPlan);
                poppush(subQueryHelper);
            }
            NebulaSQLBaseListener::exitPrimaryQuery(context);
        }
        void NebulaSQLQueryPlanCreator::enterWindowClause(NebulaSQLParser::WindowClauseContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.isWindow = true;
            poppush(helper);
            NebulaSQLBaseListener::enterWindowClause(context);

        }
        void NebulaSQLQueryPlanCreator::exitWindowClause(NebulaSQLParser::WindowClauseContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.isWindow = false;
            poppush(helper);

            NebulaSQLBaseListener::exitWindowClause(context);

        }
        void NebulaSQLQueryPlanCreator::enterTimeUnit(NebulaSQLParser::TimeUnitContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.timeUnit = context->getText();
            poppush(helper);

        }
        void NebulaSQLQueryPlanCreator::exitSizeParameter(NebulaSQLParser::SizeParameterContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.size = std::stoi(context->children[1]->getText());
            poppush(helper);

            NebulaSQLBaseListener::exitSizeParameter(context);
        }
        void NebulaSQLQueryPlanCreator::exitTimestampParameter(NebulaSQLParser::TimestampParameterContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.timestamp=context->getText();
            poppush(helper);

        }
        // WINDOWS
        void NebulaSQLQueryPlanCreator::exitTumblingWindow(NebulaSQLParser::TumblingWindowContext* context) {
            NebulaSQLHelper helper = helpers.top();
            TimeMeasure timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
            helper.windowType = Windowing::TumblingWindow::of(EventTime(Attribute(helper.timestamp)), timeMeasure);
            poppush(helper);
            NebulaSQLBaseListener::exitTumblingWindow(context);
        }
        void NebulaSQLQueryPlanCreator::exitSlidingWindow(NebulaSQLParser::SlidingWindowContext* context) {
            NebulaSQLHelper helper = helpers.top();
            TimeMeasure timeMeasure = buildTimeMeasure(helper.size, helper.timeUnit);
            TimeMeasure slidingLength = buildTimeMeasure(1, helper.timeUnit);
            helper.windowType = Windowing::SlidingWindow::of(EventTime(Attribute(helper.timestamp)), timeMeasure, slidingLength);
            poppush(helper);
            NebulaSQLBaseListener::exitSlidingWindow(context);
        }
        void NebulaSQLQueryPlanCreator::exitThresholdBasedWindow(NebulaSQLParser::ThresholdBasedWindowContext* context) {
            NebulaSQLHelper helper = helpers.top();
            if(helper.minimumCount != -1) {
                helper.windowType = Windowing::ThresholdWindow::of(helper.expressionBuilder.back(), helper.minimumCount);
            }
            else {
                helper.windowType = Windowing::ThresholdWindow::of(helper.expressionBuilder.back());
            }
            helper.isTimeBasedWindow = false;
            poppush(helper);
            NebulaSQLBaseListener::exitThresholdBasedWindow(context);

        }
        void NebulaSQLQueryPlanCreator::enterNamedExpression(NebulaSQLParser::NamedExpressionContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.identCountHelper = 0;
            poppush(helper);
            NebulaSQLBaseListener::enterNamedExpression(context);
        }
        void NebulaSQLQueryPlanCreator::exitNamedExpression(NebulaSQLParser::NamedExpressionContext* context) {
            NebulaSQLHelper helper = helpers.top();
            // handle implicit maps when no "AS" is supplied, but a rename is needed
            if (!helper.isFunctionCall && !helper.expressionBuilder.empty() && helper.isSelect && helper.identCountHelper > 1 && context->children.size() == 1){
                std::string implicitFieldName = "mapField" + std::to_string(helper.implicitMapCountHelper);
                ExpressionNodePtr attr = helper.expressionBuilder.back();
                helper.expressionBuilder.pop_back();
                helper.mapBuilder.push_back(Attribute(implicitFieldName) = attr);

                helper.implicitMapCountHelper++;
            }
            helper.isFunctionCall = false;
            poppush(helper);

            NebulaSQLBaseListener::exitNamedExpression(context);
        }
        void NebulaSQLQueryPlanCreator::enterFunctionCall(NebulaSQLParser::FunctionCallContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.isFunctionCall = true;
            poppush(helper);
            NebulaSQLBaseListener::enterFunctionCall(context);
        }
        void NebulaSQLQueryPlanCreator::enterHavingClause(NebulaSQLParser::HavingClauseContext* context) {
            helpers.top().isWhereOrHaving = true;
            NebulaSQLHelper helper = helpers.top();
            poppush(helper);
            NebulaSQLBaseListener::enterHavingClause(context);
        }
        void NebulaSQLQueryPlanCreator::exitHavingClause(NebulaSQLParser::HavingClauseContext* context) {
            helpers.top().isWhereOrHaving = false;
            NebulaSQLHelper helper = helpers.top();
            if(helper.expressionBuilder.size()!=1){
                NES_ERROR("Zuviele Expressions oder zuwenige,warum??");
            }
            helper.addHavingClause(helper.expressionBuilder.back());
            helper.expressionBuilder.clear();
            poppush(helper);
            NebulaSQLBaseListener::exitHavingClause(context);
        }

        void NebulaSQLQueryPlanCreator::exitComparison(NebulaSQLParser::ComparisonContext* context) {

            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;
            if (!helper.isJoinRelation) {
                auto rightExpression = helper.expressionBuilder.back();
                helper.expressionBuilder.pop_back();
                auto leftExpression = helper.expressionBuilder.back();
                helper.expressionBuilder.pop_back();
                std::string op = context->children.at(1)->getText();
                if (op == ">") {
                    expression = NES::GreaterExpressionNode::create(leftExpression, rightExpression);
                } else if (op == "<") {
                    expression = NES::LessExpressionNode::create(leftExpression, rightExpression);
                } else if (op == ">=") {
                    expression = NES::GreaterEqualsExpressionNode::create(leftExpression, rightExpression);
                } else if (op == "<=") {
                    expression = NES::LessEqualsExpressionNode::create(leftExpression, rightExpression);
                } else if (op == "==" || op == "=") {
                    expression = NES::EqualsExpressionNode::create(leftExpression, rightExpression);
                } else {
                    NES_ERROR("Unknown Comparison Operator");
                }
                helper.expressionBuilder.push_back(expression);
                poppush(helper);
            }
            NebulaSQLBaseListener::enterComparison(context);
        }
        void NebulaSQLQueryPlanCreator::enterJoinRelation(NebulaSQLParser::JoinRelationContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.joinKeyRelationHelper.clear();
            helper.isJoinRelation = true;
            poppush(helper);
            NebulaSQLBaseListener::enterJoinRelation(context);
        }
        void NebulaSQLQueryPlanCreator::exitLogicalNot(NebulaSQLParser::LogicalNotContext* context) {
            NebulaSQLHelper helper = helpers.top();
            NES::ExpressionNodePtr expression;

            auto innerExpression = helper.expressionBuilder.back();
            helper.expressionBuilder.pop_back();

            expression = !innerExpression;
            helper.expressionBuilder.push_back(expression);
            poppush(helper);
            NebulaSQLBaseListener::exitLogicalNot(context);
        }

        void NebulaSQLQueryPlanCreator::exitConstantDefault(NebulaSQLParser::ConstantDefaultContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.expressionBuilder.push_back(NES::Attribute(context->getText()));
            poppush(helper);
        }

        void NebulaSQLQueryPlanCreator::exitRealIdent(NebulaSQLParser::RealIdentContext* context) {
            NebulaSQLBaseListener::exitRealIdent(context);
        }
        void NebulaSQLQueryPlanCreator::exitFunctionCall(NebulaSQLParser::FunctionCallContext* context) {
            NebulaSQLHelper helper = helpers.top();
            auto funcName = context->children[0]->getText();
            for (char & c : funcName) {
                c = std::tolower(static_cast<unsigned char>(c));
            }
            if(funcName == "count"){
                helper.windowAggs.push_back( API::Count()->aggregation);
            }
            else if(funcName == "avg"){
                helper.windowAggs.push_back(API::Avg(helper.expressionBuilder.back())->aggregation);
                helper.expressionBuilder.pop_back();
            }
            else if(funcName == "max"){
                helper.windowAggs.push_back(API::Max(helper.expressionBuilder.back())->aggregation);
                helper.expressionBuilder.pop_back();
            }
            else if(funcName == "min"){
                helper.windowAggs.push_back(API::Min(helper.expressionBuilder.back())->aggregation);
                helper.expressionBuilder.pop_back();
            }
            else if(funcName == "sum"){
                helper.windowAggs.push_back(API::Sum(helper.expressionBuilder.back())->aggregation);
                helper.expressionBuilder.pop_back();
            }
            else if(funcName == "median"){
                helper.windowAggs.push_back(API::Median(helper.expressionBuilder.back())->aggregation);
                helper.expressionBuilder.pop_back();
            }
            else{
                NES_THROW_RUNTIME_ERROR("Invalid Aggregation Function: "+funcName);
            }
            poppush(helper);
        }
        void NebulaSQLQueryPlanCreator::exitThresholdMinSizeParameter(NebulaSQLParser::ThresholdMinSizeParameterContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.minimumCount = std::stoi(context->getText());
            poppush(helper);
        }

        void NebulaSQLQueryPlanCreator::enterValueExpressionDefault(NebulaSQLParser::ValueExpressionDefaultContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.identCountHelper++;
            poppush(helper);
            NebulaSQLBaseListener::enterValueExpressionDefault(context);

        }
        void NebulaSQLQueryPlanCreator::exitJoinRelation(NebulaSQLParser::JoinRelationContext* context) {
            NebulaSQLHelper helper = helpers.top();
            helper.isJoinRelation = false;
            if(helper.joinKeyRelationHelper.size()>=2){
                helper.joinKeys.emplace_back(helper.joinKeyRelationHelper[0],helper.joinKeyRelationHelper[1]);
            }
            else{
                NES_ERROR("Join Keys have to be specified explicitly")
            }
            if(helper.joinSources.size() == helper.joinSourceRenames.size() + 1){
                helper.joinSourceRenames.emplace_back("");
            }
            poppush(helper);
            NebulaSQLBaseListener::exitJoinRelation(context);


        }

        void NebulaSQLQueryPlanCreator::exitSetOperation(NebulaSQLParser::SetOperationContext* context) {
            if(queryPlans.size() < 2)
                NES_ERROR("Union does not have sufficient amount of QueryPlans for unifiying");

            auto rightQuery = queryPlans.top();
            queryPlans.pop();
            auto leftQuery = queryPlans.top();
            queryPlans.pop();
            auto queryPlan = QueryPlanBuilder::addUnion(leftQuery,rightQuery);
            if (!helpers.empty()){
                // we are in a subquery
                auto helper = helpers.top();
                helper.queryPlans.push_back(queryPlan);
                poppush(helper);
            } else {
                queryPlans.push(queryPlan );
            }
            NebulaSQLBaseListener::exitSetOperation(context);

        }

    }// namespace NES::Parsers

