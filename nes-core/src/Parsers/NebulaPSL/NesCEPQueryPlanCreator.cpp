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
#include <API/QueryAPI.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Parsers/NebulaPSL/NebulaPSLQueryPlanCreator.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>

namespace NES::Parsers {

void NesCEPQueryPlanCreator::enterListEvents(NesCEPParser::ListEventsContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : enterListEvents: init tree walk and initialize read out of AST ");
    this->nodeId++;
    NesCEPBaseListener::enterListEvents(context);
}

void NesCEPQueryPlanCreator::enterEventElem(NesCEPParser::EventElemContext* cxt) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitEventElem: found a stream source  {}", cxt->getStart()->getText());
    //create sources pair, e.g., <identifier,SourceName>
    pattern.addSource(std::make_pair(sourceCounter, cxt->getStart()->getText()));
    this->lastSeenSourcePtr = sourceCounter;
    sourceCounter++;
    NES_DEBUG("NesCEPQueryPlanCreator : exitEventElem: inserted {} to sources", cxt->getStart()->getText());
    this->nodeId++;
    NesCEPBaseListener::exitEventElem(cxt);
}

void NesCEPQueryPlanCreator::exitOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitOperatorRule: create a node for the operator  {}", context->getText());
    //create Operator node and set attributes with context information
    NebulaPSLOperatorNode node = NebulaPSLOperatorNode(nodeId);
    node.setParentNodeId(-1);
    node.setOperatorName(context->getText());
    node.setLeftChildId(lastSeenSourcePtr);
    node.setRightChildId(lastSeenSourcePtr + 1);
    pattern.addOperatorNode(node);
    // increase nodeId
    nodeId++;
    NesCEPBaseListener::exitOperatorRule(context);
}

void NesCEPQueryPlanCreator::exitInputStream(NesCEPParser::InputStreamContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitInputStream: replace alias with streamName  {}", context->getText());
    std::string sourceName = context->getStart()->getText();
    std::string aliasName = context->getStop()->getText();
    //replace alias in the list of sources with actual sources name
    std::map<int32_t, std::string> sources = pattern.getSources();
    std::map<int32_t, std::string>::iterator sourceMapItem;
    for (sourceMapItem = sources.begin(); sourceMapItem != sources.end(); sourceMapItem++) {
        std::string currentEventName = sourceMapItem->second;
        if (currentEventName == aliasName) {
            pattern.updateSource(sourceMapItem->first, sourceName);
            break;
        }
    }
    NesCEPBaseListener::exitInputStream(context);
}

void NesCEPQueryPlanCreator::enterWhereExp(NesCEPParser::WhereExpContext* context) {
    inWhere = true;
    NesCEPBaseListener::enterWhereExp(context);
}

void NesCEPQueryPlanCreator::exitWhereExp(NesCEPParser::WhereExpContext* context) {
    inWhere = false;
    NesCEPBaseListener::exitWhereExp(context);
}

// WITHIN clause
void NesCEPQueryPlanCreator::exitInterval(NesCEPParser::IntervalContext* cxt) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitInterval:  {}", cxt->getText());
    // get window definitions
    std::string timeUnit = cxt->intervalType()->getText();
    int32_t time = std::stoi(cxt->getStart()->getText());
    pattern.setWindow(std::make_pair(timeUnit, time));
    NesCEPBaseListener::exitInterval(cxt);
}

void NesCEPQueryPlanCreator::enterOutAttribute(NesCEPParser::OutAttributeContext* context) {
    //get projection fields
    auto attributeField = NES::Attribute(context->NAME()->getText()).getExpressionNode();
    pattern.addProjectionField(attributeField);
}

void NesCEPQueryPlanCreator::enterSink(NesCEPParser::SinkContext* context) {
    // collect specified sinks
    std::string sinkType = context->sinkType()->getText();
    SinkDescriptorPtr sinkDescriptor;

    if (sinkType == "Print") {
        sinkDescriptor = NES::PrintSinkDescriptor::create();
    }
    if (sinkType == "File") {
        sinkDescriptor = NES::FileSinkDescriptor::create(context->NAME()->getText());
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
    pattern.addSink(sinkDescriptor);
    NesCEPBaseListener::enterSink(context);
}

void NesCEPQueryPlanCreator::exitSinkList(NesCEPParser::SinkListContext* context) { NesCEPBaseListener::exitSinkList(context); }

void NesCEPQueryPlanCreator::enterQuantifiers(NesCEPParser::QuantifiersContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: {}", context->getText())
    //method that specifies the times operator which has several cases
    //create Operator node and add specification
    NebulaPSLOperatorNode timeOperatorNode = NebulaPSLOperatorNode(nodeId);
    timeOperatorNode.setParentNodeId(-1);
    timeOperatorNode.setOperatorName("TIMES");
    timeOperatorNode.setLeftChildId(lastSeenSourcePtr);
    if (context->LBRACKET()) {    // context contains []
        if (context->D_POINTS()) {//e.g., A[2:10] means that we expect at least 2 and maximal 10 occurrences of A
            NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: Times with Min: {} and Max {}",
                      context->iterMin()->INT()->getText(),
                      context->iterMin()->INT()->getText());
            timeOperatorNode.setMinMax(
                std::make_pair(stoi(context->iterMin()->INT()->getText()), stoi(context->iterMax()->INT()->getText())));
        } else {// e.g., A[2] means that we except exact 2 occurrences of A
            timeOperatorNode.setMinMax(std::make_pair(stoi(context->INT()->getText()), stoi(context->INT()->getText())));
        }
    } else if (context->PLUS()) {//e.g., A+, means a occurs at least once
        timeOperatorNode.setMinMax(std::make_pair(0, 0));
    } else if (context->STAR()) {//[]*   //TODO unbounded iteration variant not yet implemented #866
        NES_THROW_RUNTIME_ERROR(
            "NesCEPQueryPlanCreator : enterQuantifiers: NES currently does not support the iteration variant *");
    }
    pattern.addOperatorNode(timeOperatorNode);
    //update pointer
    nodeId++;
    NesCEPBaseListener::enterQuantifiers(context);
}

void NesCEPQueryPlanCreator::exitBinaryComparisonPredicate(NesCEPParser::BinaryComparasionPredicateContext* context) {
    //retrieve the ExpressionNode for the filter and save it in the pattern expressionList
    std::string comparisonOperator = context->comparisonOperator()->getText();
    auto leftExpressionNode = NES::Attribute(this->currentLeftExp).getExpressionNode();
    auto rightExpressionNode = NES::Attribute(this->currentRightExp).getExpressionNode();
    NES::ExpressionNodePtr expression;
    NES_DEBUG("NesCEPQueryPlanCreator: exitBinaryComparisonPredicate: add filters {} {} {}",
              this->currentLeftExp,
              comparisonOperator,
              this->currentRightExp)

    if (comparisonOperator == "<") {
        expression = NES::LessExpressionNode::create(leftExpressionNode, rightExpressionNode);
    }
    if (comparisonOperator == "<=") {
        expression = NES::LessEqualsExpressionNode::create(leftExpressionNode, rightExpressionNode);
    }
    if (comparisonOperator == ">") {
        expression = NES::GreaterExpressionNode::create(leftExpressionNode, rightExpressionNode);
    }
    if (comparisonOperator == ">=") {
        expression = NES::GreaterEqualsExpressionNode::create(leftExpressionNode, rightExpressionNode);
    }
    if (comparisonOperator == "==") {
        expression = NES::EqualsExpressionNode::create(leftExpressionNode, rightExpressionNode);
    }
    if (comparisonOperator == "&&") {
        expression = NES::AndExpressionNode::create(leftExpressionNode, rightExpressionNode);
    }
    if (comparisonOperator == "||") {
        expression = NES::OrExpressionNode::create(leftExpressionNode, rightExpressionNode);
    }
    this->pattern.addExpression(expression);
}

void NesCEPQueryPlanCreator::enterAttribute(NesCEPParser::AttributeContext* cxt) {
    NES_DEBUG("NesCEPQueryPlanCreator: enterAttribute: {}", cxt->getText())
    if (inWhere) {
        if (leftFilter) {
            currentLeftExp = cxt->getText();
            leftFilter = false;
        } else {
            currentRightExp = cxt->getText();
            leftFilter = true;
        }
    }
}

QueryPlanPtr NesCEPQueryPlanCreator::createQueryFromPatternList() const {
    if (this->pattern.getOperatorList().empty() && this->pattern.getSources().size() == 0) {
        NES_THROW_RUNTIME_ERROR("NesCEPQueryPlanCreator: createQueryFromPatternList: Received an empty pattern");
    }
    NES_DEBUG("NesCEPQueryPlanCreator: createQueryFromPatternList: create query from AST elements")
    QueryPlanPtr queryPlan;
    // if for simple patterns without binary CEP operators
    if (this->pattern.getOperatorList().empty() && this->pattern.getSources().size() == 1) {
        auto sourceName = pattern.getSources().at(0);
        queryPlan = QueryPlanBuilder::createQueryPlan(sourceName);
        // else for pattern with binary operators
    } else {
        // iterate over OperatorList, create and add LogicalOperatorNodes
        for (auto operatorNode = pattern.getOperatorList().begin(); operatorNode != pattern.getOperatorList().end();
             ++operatorNode) {
            auto operatorName = operatorNode->second.getOperatorName();
            // add binary operators
            if (operatorName == "OR" || operatorName == "SEQ" || operatorName == "AND") {
                queryPlan = addBinaryOperatorToQueryPlan(operatorName, operatorNode, queryPlan);
            } else if (operatorName == "TIMES") {//add times operator
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add unary operator {}", operatorName)
                auto sourceName = pattern.getSources().at(operatorNode->second.getLeftChildId());
                queryPlan = QueryPlanBuilder::createQueryPlan(sourceName);
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add times operator{}",
                          pattern.getSources().at(operatorNode->second.getLeftChildId()))

                queryPlan = QueryPlanBuilder::addMap(Attribute("Count") = 1, queryPlan);

                //create window
                auto timeMeasurements = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
                if (timeMeasurements.first.getTime() != TimeMeasure(0).getTime()) {
                    auto windowType = Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")),
                                                                   timeMeasurements.first,
                                                                   timeMeasurements.second);
                    // check and add watermark
                    queryPlan = QueryPlanBuilder::checkAndAddWatermarkAssignment(queryPlan, windowType);
                    // create default pol
                    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
                    auto distributionType = Windowing::DistributionCharacteristic::createCompleteWindowType();
                    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

                    std::vector<WindowAggregationPtr> windowAggs;
                    std::shared_ptr<WindowAggregationDescriptor> sumAgg = API::Sum(Attribute("Count"));

                    auto timeField =
                        WindowType::asTimeBasedWindowType(windowType)->getTimeCharacteristic()->getField()->getName();
                    std::shared_ptr<WindowAggregationDescriptor> maxAggForTime = API::Max(Attribute(timeField));
                    windowAggs.push_back(sumAgg);
                    windowAggs.push_back(maxAggForTime);

                    auto windowDefinition = Windowing::LogicalWindowDefinition::create(windowAggs,
                                                                                       windowType,
                                                                                       distributionType,
                                                                                       triggerPolicy,
                                                                                       triggerAction,
                                                                                       0);

                    OperatorNodePtr op = LogicalOperatorFactory::createWindowOperator(windowDefinition);
                    queryPlan->appendOperatorAsNewRoot(op);

                    int32_t min = operatorNode->second.getMinMax().first;
                    int32_t max = operatorNode->second.getMinMax().second;

                    if (min != 0 && max != 0) {//TODO unbounded iteration variant not yet implemented #866 (min = 1/0 max = 0)
                        ExpressionNodePtr predicate;

                        if (min == max) {
                            predicate = Attribute("Count") = min;
                        } else if (min == 0) {
                            predicate = Attribute("Count") <= max;
                        } else if (max == 0) {
                            predicate = Attribute("Count") >= min;
                        } else {
                            predicate = Attribute("Count") >= min && Attribute("Count") <= max;
                        }
                        queryPlan = QueryPlanBuilder::addFilter(predicate, queryPlan);
                    }
                } else {
                    NES_THROW_RUNTIME_ERROR(
                        "NesCEPQueryPlanCreator: createQueryFromPatternList: The Iteration operator requires a time window.");
                }
            } else {
                NES_THROW_RUNTIME_ERROR("NesCEPQueryPlanCreator: createQueryFromPatternList: Unkown CEP operator"
                                        << operatorName);
            }
        }
    }
    if (!pattern.getExpressions().empty()) {
        queryPlan = addFilters(queryPlan);
    }
    if (!pattern.getProjectionFields().empty()) {
        queryPlan = addProjections(queryPlan);
    }

    const std::vector<NES::OperatorNodePtr>& rootOperators = queryPlan->getRootOperators();
    // add the sinks to the query plan
    for (SinkDescriptorPtr sinkDescriptor : pattern.getSinks()) {
        auto sinkOperator = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
        for (auto& rootOperator : rootOperators) {
            sinkOperator->addChild(rootOperator);
            queryPlan->removeAsRootOperator(rootOperator);
            queryPlan->addRootOperator(sinkOperator);
        }
    }
    return queryPlan;
}

QueryPlanPtr NesCEPQueryPlanCreator::addFilters(QueryPlanPtr queryPlan) const {
    for (auto it = pattern.getExpressions().begin(); it != pattern.getExpressions().end(); ++it) {
        queryPlan = QueryPlanBuilder::addFilter(*it, queryPlan);
    }
    return queryPlan;
}

std::pair<TimeMeasure, TimeMeasure> NesCEPQueryPlanCreator::transformWindowToTimeMeasurements(std::string timeMeasure,
                                                                                              int32_t timeValue) const {

    if (timeMeasure == "MILLISECOND") {
        TimeMeasure size = Minutes(timeValue);
        TimeMeasure slide = Minutes(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "SECOND") {
        TimeMeasure size = Seconds(timeValue);
        TimeMeasure slide = Seconds(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "MINUTE") {
        TimeMeasure size = Minutes(timeValue);
        TimeMeasure slide = Minutes(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "HOUR") {
        TimeMeasure size = Hours(timeValue);
        TimeMeasure slide = Hours(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else {
        NES_THROW_RUNTIME_ERROR("NesCEPQueryPlanCreator: transformWindowToTimeMeasurements: Unkown time measure " + timeMeasure);
    }
    return std::pair<TimeMeasure, TimeMeasure>(TimeMeasure(0), TimeMeasure(0));
}

QueryPlanPtr NesCEPQueryPlanCreator::addProjections(QueryPlanPtr queryPlan) const {
    return QueryPlanBuilder::addProjection(pattern.getProjectionFields(), queryPlan);
}

QueryPlanPtr NesCEPQueryPlanCreator::getQueryPlan() const {
    try {
        return createQueryFromPatternList();
    } catch (std::exception& e) {
        NES_THROW_RUNTIME_ERROR("NesCEPQueryPlanCreator::getQueryPlan(): Was not able to parse query: " << e.what());
    }
}

std::string NesCEPQueryPlanCreator::keyAssignment(std::string keyName) const {
    //first, get unique ids for the key attributes
    auto cepRightId = getNextOperatorId();
    //second, create a unique name for both key attributes
    std::string cepRightKey = keyName + std::to_string(cepRightId);
    return cepRightKey;
}

QueryPlanPtr NesCEPQueryPlanCreator::addBinaryOperatorToQueryPlan(std::string operaterName,
                                                                  std::map<int, NebulaPSLOperatorNode>::const_iterator it,
                                                                  QueryPlanPtr queryPlan) const {
    QueryPlanPtr rightQueryPlan;
    QueryPlanPtr leftQueryPlan;
    NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: add binary operator {}", operaterName)
    // find left (query) and right branch (subquery) of binary operator
    //left query plan
    NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryLeft from {}",
              pattern.getSources().at(it->second.getLeftChildId()))
    auto leftSourceName = pattern.getSources().at(it->second.getLeftChildId());
    auto rightSourceName = pattern.getSources().at(it->second.getRightChildId());
    // if queryPlan is empty
    if (!queryPlan
        || (queryPlan->getSourceConsumed().find(leftSourceName) == std::string::npos
            && queryPlan->getSourceConsumed().find(rightSourceName) == std::string::npos)) {
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: both sources are not in the current queryPlan")
        //make left source current queryPlan
        leftQueryPlan = QueryPlanBuilder::createQueryPlan(leftSourceName);
        //right source as right queryPlan
        rightQueryPlan = QueryPlanBuilder::createQueryPlan(rightSourceName);
    } else {
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: check for the right branch")
        leftQueryPlan = queryPlan;
        rightQueryPlan = checkIfSourceIsAlreadyConsumedSource(leftSourceName, rightSourceName, queryPlan);
    }

    if (operaterName == "OR") {
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: addUnionOperator")
        leftQueryPlan = QueryPlanBuilder::addUnion(leftQueryPlan, rightQueryPlan);
    } else {
        // Seq and And require a window, so first we create and check the window operator
        auto timeMeasurements = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
        if (timeMeasurements.first.getTime() != TimeMeasure(0).getTime()) {
            auto windowType =
                Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), timeMeasurements.first, timeMeasurements.second);

            //Next, we add artificial key attributes to the sources in order to reuse the join-logic later
            std::string cepLeftKey = keyAssignment("cep_leftLeft");
            std::string cepRightKey = keyAssignment("cep_rightkey");

            //next: add Map operator that maps the attributes with value 1 to the left and right source
            leftQueryPlan = QueryPlanBuilder::addMap(Attribute(cepLeftKey) = 1, leftQueryPlan);
            rightQueryPlan = QueryPlanBuilder::addMap(Attribute(cepRightKey) = 1, rightQueryPlan);

            //then, define the artificial attributes as key attributes
            NES_DEBUG("NesCEPQueryPlanCreater: add name cepLeftKey {} and name cepRightKey {}", cepLeftKey, cepRightKey);
            ExpressionItem onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
            ExpressionItem onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
            auto leftKeyFieldAccess = onLeftKey.getExpressionNode()->as<FieldAccessExpressionNode>();
            auto rightKeyFieldAccess = onRightKey.getExpressionNode()->as<FieldAccessExpressionNode>();

            leftQueryPlan = QueryPlanBuilder::addJoin(leftQueryPlan,
                                                      rightQueryPlan,
                                                      onLeftKey,
                                                      onRightKey,
                                                      windowType,
                                                      Join::LogicalJoinDefinition::JoinType::CARTESIAN_PRODUCT);

            if (operaterName == "SEQ") {
                // for SEQ we need to add additional filter for order by time
                auto timestamp = WindowType::asTimeBasedWindowType(windowType)->getTimeCharacteristic()->getField()->getName();
                // to guarantee a correct order of events by time (sequence) we need to identify the correct source and its timestamp
                // in case of composed streams on the right branch
                auto sourceNameRight = rightQueryPlan->getSourceConsumed();
                if (sourceNameRight.find("_") != std::string::npos) {
                    // we find the most left source and use its timestamp for the filter constraint
                    uint64_t posStart = sourceNameRight.find("_");
                    uint64_t posEnd = sourceNameRight.find("_", posStart + 1);
                    sourceNameRight = sourceNameRight.substr(posStart + 1, posEnd - 2) + "$" + timestamp;
                }// in case the right branch only contains 1 source we can just use it
                else {
                    sourceNameRight = sourceNameRight + "$" + timestamp;
                }
                auto sourceNameLeft = leftQueryPlan->getSourceConsumed();
                // in case of composed sources on the left branch
                if (sourceNameLeft.find("_") != std::string::npos) {
                    // we find the most right source and use its timestamp for the filter constraint
                    uint64_t posStart = sourceNameLeft.find_last_of("_");
                    sourceNameLeft = sourceNameLeft.substr(posStart + 1) + "$" + timestamp;
                }// in case the left branch only contains 1 source we can just use it
                else {
                    sourceNameLeft = sourceNameLeft + "$" + timestamp;
                }
                NES_DEBUG("NesCEPQueryPlanCreater: ExpressionItem for Left Source {} and ExpressionItem for Right Source {}",
                          sourceNameLeft,
                          sourceNameRight);
                //create filter expression and add it to queryPlan
                leftQueryPlan =
                    QueryPlanBuilder::addFilter(Attribute(sourceNameLeft) < Attribute(sourceNameRight), leftQueryPlan);
            }
        } else {
            NES_THROW_RUNTIME_ERROR("NesCEPQueryPlanCreater: createQueryFromPatternList: Cannot create " + operaterName
                                    + "without a window.");
        }
    }
    return leftQueryPlan;
}

QueryPlanPtr NesCEPQueryPlanCreator::checkIfSourceIsAlreadyConsumedSource(std::basic_string<char> leftSourceName,
                                                                          std::basic_string<char> rightSourceName,
                                                                          QueryPlanPtr queryPlan) const {
    QueryPlanPtr rightQueryPlan = nullptr;
    if (queryPlan->getSourceConsumed().find(leftSourceName) != std::string::npos
        && queryPlan->getSourceConsumed().find(rightSourceName) != std::string::npos) {
        NES_THROW_RUNTIME_ERROR("NesCEPQueryPlanCreater: checkIfSourceIsAlreadyConsumedSource: Both sources are already consumed "
                                "and combined with a binary operator");
    } else if (queryPlan->getSourceConsumed().find(leftSourceName) == std::string::npos) {
        // right queryplan
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryRight from {}", leftSourceName)
        return rightQueryPlan = QueryPlanBuilder::createQueryPlan(leftSourceName);
    } else if (queryPlan->getSourceConsumed().find(rightSourceName) == std::string::npos) {
        // right queryplan
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryRight from {}", rightSourceName)
        return rightQueryPlan = QueryPlanBuilder::createQueryPlan(rightSourceName);
    }
    return rightQueryPlan;
}

}// namespace NES::Parsers