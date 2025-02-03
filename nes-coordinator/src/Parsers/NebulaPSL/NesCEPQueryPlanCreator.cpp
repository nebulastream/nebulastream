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
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Parsers/NebulaPSL/NebulaPSLQueryPlanCreator.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <regex>

namespace NES::Parsers {

void NesCEPQueryPlanCreator::enterListEvents(NesCEPParser::ListEventsContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : enterListEvents: init tree walk and initialize read out of AST ");
    this->nodeId++;
    NesCEPBaseListener::enterListEvents(context);
}

void NesCEPQueryPlanCreator::enterEventElem(NesCEPParser::EventElemContext* cxt) {
    if (cxt->LPARENTHESIS()) {
        // Handle parentheses
        NesCEPQueryPlanCreator::handleParenthesizedExpression(cxt->listEvents());
    } else {
        NES_DEBUG("NesCEPQueryPlanCreator : enterEventElem: found a stream source  {}", cxt->event()->getText());
        //create sources pair, e.g., <identifier,SourceName>
        pattern.addSource(std::make_pair(sourceCounter, cxt->getStart()->getText()));
        this->lastSeenSourcePtr = sourceCounter;
        sourceCounter++;
    }
    this->nodeId++;
    NesCEPBaseListener::exitEventElem(cxt);
}

void NesCEPQueryPlanCreator::handleParenthesizedExpression(NesCEPParser::ListEventsContext* context) {
    // Save current state
    uint32_t savedSourceCounter = sourceCounter;
    uint32_t savedNodeId = nodeId;
    uint32_t savedLastSeenSourcePtr = lastSeenSourcePtr;

    // Create new sub-pattern for the nested expression
    NebulaPSLPattern subPattern;
    NebulaPSLPattern* savedPattern = &pattern;
    auto SavedPattern = pattern;
    subPattern.setWindow(savedPattern->getWindow());
    pattern = subPattern;
    auto window = pattern.getWindow();
    NES_DEBUG("NesCEPQueryPlanCreator : handleParenthesizedExpression: Window - Time unit: '{}', Time value: {}",
              window.first,
              window.second);

    // Process the nested expression
    for (auto eventElem : context->eventElem()) {
        enterEventElem(eventElem);
        if (context->operatorRule().size() > 0) {
            exitOperatorRule(context->operatorRule(0));
        }
    }
    // Create subquery from nested expression
    QueryPlanPtr subQueryPlan = createQueryFromPatternList();

    // Restore original state of pattern
    pattern = SavedPattern;
    sourceCounter = savedSourceCounter;
    nodeId = savedNodeId;
    lastSeenSourcePtr = savedLastSeenSourcePtr;

    // Add the subquery as a "source" to the main pattern
    std::string subQueryName = "SubQuery_" + std::to_string(nodeId);
    pattern.addSource(std::make_pair(sourceCounter, subQueryName));
    this->lastSeenSourcePtr = sourceCounter;
    sourceCounter++;
    // Store the subquery plan for later
    subQueries[subQueryName] = subQueryPlan;
    NES_DEBUG("NesCEPQueryPlanCreator : handleParenthesizedExpression: finished processing nested expression");
}

void NesCEPQueryPlanCreator::exitOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitOperatorRule: create a node for the operator  {}", context->getText());
    //create Operator node and set attributes with context information
    NebulaPSLOperator node = NebulaPSLOperator(nodeId);
    node.setParentNodeId(-1);
    node.setOperatorName(context->getText());
    node.setLeftChildId(lastSeenSourcePtr);
    node.setRightChildId(lastSeenSourcePtr + 1);
    pattern.addOperator(node);
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
    pattern.updateAliasList(aliasName, sourceName);
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
    NES_DEBUG("NesCEPQueryPlanCreator : exitInterval with : time {} timeUnit: {}",
              cxt->getStart()->getText(),
              cxt->intervalType()->getText());
    // get window definitions
    std::string timeUnit = cxt->intervalType()->getText();
    NES_DEBUG("NesCEPQueryPlanCreator : timeUnit:  {}", cxt->intervalType()->getText());
    int32_t time = std::stoi(cxt->getStart()->getText());
    NES_DEBUG("NesCEPQueryPlanCreator : exitInterval: Time unit: '{}', Time value: {}", timeUnit, time);
    pattern.setWindow(std::make_pair(timeUnit, time));
    NesCEPBaseListener::exitInterval(cxt);
}

void NesCEPQueryPlanCreator::enterOutAttribute(NesCEPParser::OutAttributeContext* context) {
    //get projection fields
    auto attributeField = NES::Attribute(context->NAME()->getText()).getExpressionNode();
    pattern.addProjectionField(attributeField);
}

void NesCEPQueryPlanCreator::enterSinkWithParameters(NesCEPParser::SinkWithParametersContext* context) {
    std::string sinkType = context->sinkType()->getText();
    NES_DEBUG("NesCEPQueryPlanCreator : specify Sink with Type: {}", sinkType)
    SinkDescriptorPtr sinkDescriptor;

    if (sinkType == "FILE") {
        // get fileName
        if (context->parameters()->parameter().at(0)->getText() == "fileName") {
            sinkDescriptor = NES::FileSinkDescriptor::create(context->parameters()->value(0)->getText());
            NES_DEBUG("NesCEPQueryPlanCreator : create sinkDescriptor for File Sink with name : {}",
                      context->parameters()->value(0)->getText());
        } else {
            NES_THROW_RUNTIME_ERROR("Unknown parameter(s) for FileSink {}", context->parameters()->getText());
        }
    } else if (sinkType == "MQTT") {
        // get topic and address
        NES_DEBUG("NesCEPQueryPlanCreator : number of parameters : {}", context->parameters()->parameter().size());
        if (context->parameters()->parameter().size() == 2) {
            if (context->parameters()->parameter().at(0)->getStart()->getText() == "address") {
                sinkDescriptor = NES::MQTTSinkDescriptor::create(context->parameters()->value(0)->getText(),
                                                                 context->parameters()->value(1)->getText());
            } else {
                sinkDescriptor = NES::MQTTSinkDescriptor::create(context->parameters()->value(1)->getText(),
                                                                 context->parameters()->value(0)->getText());
            }
            NES_DEBUG("NesCEPQueryPlanCreator : create sinkDescriptor for MQTT Sink with address : {} and topic {}",
                      context->parameters()->value(0)->getText(),
                      context->parameters()->value(1)->getText());
        } else {
            NES_THROW_RUNTIME_ERROR("Unknown parameter(s) for MQTTSink {}", context->parameters()->getText());
        }

    } else {
        NES_THROW_RUNTIME_ERROR("Unknown sink was specified");
    }

    pattern.addSink(sinkDescriptor);
    NesCEPBaseListener::exitSinkWithParameters(context);
}

void NesCEPQueryPlanCreator::enterSinkWithoutParameters(NesCEPParser::SinkWithoutParametersContext* context) {
    // collect specified sinks
    std::string sinkType = context->sinkType()->getText();
    NES_DEBUG("NesCEPQueryPlanCreator : found Sink Type: {}", sinkType)
    SinkDescriptorPtr sinkDescriptor;

    if (sinkType == "PRINT") {
        sinkDescriptor = NES::PrintSinkDescriptor::create();
    } else if (sinkType == "NETWORK") {
        sinkDescriptor = NES::NullOutputSinkDescriptor::create();
    } else if (sinkType == "NULLOUTPUT") {
        sinkDescriptor = NES::NullOutputSinkDescriptor::create();
    } else {
        NES_THROW_RUNTIME_ERROR("Unkown sink was specified");
    }
    pattern.addSink(sinkDescriptor);
    NesCEPBaseListener::exitSinkWithoutParameters(context);
}

void NesCEPQueryPlanCreator::enterQuantifiers(NesCEPParser::QuantifiersContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: {}", context->getText())
    //method that specifies the times operator which has several cases
    //create Operator node and add specification
    NebulaPSLOperator timeOperator = NebulaPSLOperator(nodeId);
    timeOperator.setParentNodeId(-1);
    timeOperator.setOperatorName("TIMES");
    timeOperator.setLeftChildId(lastSeenSourcePtr);
    if (context->LBRACKET()) {    // context contains []
        if (context->D_POINTS()) {//e.g., A[2:10] means that we expect at least 2 and maximal 10 occurrences of A
            NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: Times with Min: {} and Max {}",
                      context->iterMin()->INT()->getText(),
                      context->iterMin()->INT()->getText());
            timeOperator.setMinMax(
                std::make_pair(stoi(context->iterMin()->INT()->getText()), stoi(context->iterMax()->INT()->getText())));
        } else {// e.g., A[2] means that we except exact 2 occurrences of A
            timeOperator.setMinMax(std::make_pair(stoi(context->INT()->getText()), stoi(context->INT()->getText())));
        }
    } else if (context->PLUS()) {//e.g., A+, means a occurs at least once
        timeOperator.setMinMax(std::make_pair(0, 0));
    } else if (context->STAR()) {//[]*   //TODO unbounded iteration variant not yet implemented #866
        NES_THROW_RUNTIME_ERROR(
            "NesCEPQueryPlanCreator : enterQuantifiers: NES currently does not support the iteration variant *");
    }
    pattern.addOperator(timeOperator);
    //update pointer
    nodeId++;
    NesCEPBaseListener::enterQuantifiers(context);
}

void NesCEPQueryPlanCreator::exitBinaryComparisonPredicate(NesCEPParser::BinaryComparisonPredicateContext* context) {
    NES_DEBUG("enter exit Binary Comparison : {} ", context->comparisonOperator()->getText());
    //retrieve the ExpressionNode for the filter and save it in the pattern expressionList
    std::string comparisonOperator = context->comparisonOperator()->getText();
    // get left and right expression node
    auto leftExpressionNode = getExpressionItem(context->left->getText());
    auto rightExpressionNode = getExpressionItem(context->right->getText());

    NES::ExpressionNodePtr expression;
    NES_DEBUG("NesCEPQueryPlanCreator: exitBinaryComparisonPredicate: add filters {} {} {}",
              leftExpressionNode.get()->toString(),
              comparisonOperator,
              rightExpressionNode.get()->toString())

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

QueryPlanPtr NesCEPQueryPlanCreator::createQueryFromPatternList() const {
    if (this->pattern.getOperatorList().empty() && this->pattern.getSources().size() == 0) {
        NES_THROW_RUNTIME_ERROR("NesCEPQueryPlanCreator: createQueryFromPatternList: Received an empty pattern");
    }
    NES_DEBUG("NesCEPQueryPlanCreator: createQueryFromPatternList: create query from AST elements");

    auto window = pattern.getWindow();
    NES_DEBUG("NesCEPQueryPlanCreator : addBinaryOperatorToQueryPlan: Window - Time unit: '{}', Time value: {}",
              window.first,
              window.second);

    QueryPlanPtr queryPlan;
    // if for simple patterns without binary CEP operators
    if (this->pattern.getOperatorList().empty() && this->pattern.getSources().size() == 1) {
        auto sourceName = pattern.getSources().at(0);
        queryPlan = QueryPlanBuilder::createQueryPlan(sourceName);
        // else for pattern with binary operators
    } else {
        // iterate over OperatorList, create and add LogicalOperators
        for (auto operatorNode = pattern.getOperatorList().begin(); operatorNode != pattern.getOperatorList().end();
             ++operatorNode) {
            auto operatorName = operatorNode->second.getOperatorName();

            if (pattern.getSinks().size() > 0) {
                if (this->pattern.getWindow().first.empty() && this->pattern.getWindow().second == 0 && operatorName != "OR") {
                    // window must be specified for an operator, throw exception
                    NES_THROW_RUNTIME_ERROR(
                        "NesCEPQueryPlanCreator: WITHIN keyword for time interval missing but must be specified for operators.");
                }
            }
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

                    std::vector<WindowAggregationDescriptorPtr> windowAggs;
                    auto sumAgg = API::Sum(Attribute("Count"))->aggregation;

                    auto timeField =
                        windowType->as<Windowing::TimeBasedWindowType>()->getTimeCharacteristic()->getField()->getName();
                    auto maxAggForTime = API::Max(Attribute(timeField))->aggregation;
                    windowAggs.push_back(sumAgg);
                    windowAggs.push_back(maxAggForTime);

                    auto windowDefinition = Windowing::LogicalWindowDescriptor::create(windowAggs, windowType, 0);

                    auto op = LogicalOperatorFactory::createWindowOperator(windowDefinition);
                    queryPlan->appendOperatorAsNewRoot(op);

                    int32_t min = operatorNode->second.getMinMax().first;
                    int32_t max = operatorNode->second.getMinMax().second;

                    if (min != 0 && max != 0) {//TODO unbounded iteration variant not yet implemented #866 (min = 1/0 max = 0)
                        ExpressionNodePtr predicate;

                        if (min == max) {
                            predicate = Attribute("Count") == min;
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

    const std::vector<NES::OperatorPtr>& rootOperators = queryPlan->getRootOperators();
    // add the sinks to the query plan
    for (const auto& sinkDescriptor : pattern.getSinks()) {
        auto sinkOperator = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
        for (const auto& rootOperator : rootOperators) {
            sinkOperator->addChild(rootOperator);
            queryPlan->removeAsRootOperator(rootOperator);
            queryPlan->addRootOperator(sinkOperator);
        }
    }
    return queryPlan;
}

QueryPlanPtr NesCEPQueryPlanCreator::addFilters(QueryPlanPtr queryPlan) const {
    for (auto it = pattern.getExpressions().begin(); it != pattern.getExpressions().end(); ++it) {
        NES_DEBUG("Add expression : {} ", it->get()->toString());
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
        // when we have a subquery we don't expect it to already include a timeMeasure since that comes at the end of the query
        NES_DEBUG("NesCEPQueryPlanCreator: transformWindowToTimeMeasurements: Unknown time measure: {}", timeMeasure);
        return std::pair<TimeMeasure, TimeMeasure>(Minutes(0), Minutes(0));
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
    std::string cepRightKey = fmt::format("{}{}", keyName, cepRightId);
    return cepRightKey;
}

QueryPlanPtr NesCEPQueryPlanCreator::addBinaryOperatorToQueryPlan(std::string operaterName,
                                                                  std::map<int, NebulaPSLOperator>::const_iterator it,
                                                                  QueryPlanPtr queryPlan) const {
    QueryPlanPtr rightQueryPlan;
    QueryPlanPtr leftQueryPlan;
    NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: add binary operator {}", operaterName)

    // find left (query) and right branch (subquery) of binary operator
    //left query plan
    NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryLeft from {}",
              pattern.getSources().at(it->second.getLeftChildId()))

    auto sourceEnd = pattern.getSources().end();
    sourceEnd = --sourceEnd;
    // checking if we found the last source in the Subquery
    if ((pattern.getSources().at(it->second.getLeftChildId()) == sourceEnd->second)
        || pattern.getSources().at(it->second.getLeftChildId()) == pattern.getSources().at(it->second.getRightChildId())) {
        return queryPlan;
    }
    auto leftSourceName = pattern.getSources().at(it->second.getLeftChildId());
    auto rightSourceName = pattern.getSources().at(it->second.getRightChildId());
    // if queryPlan is empty
    if (!queryPlan
        || (queryPlan->getSourceConsumed().find(leftSourceName) == std::string::npos
            && queryPlan->getSourceConsumed().find(rightSourceName) == std::string::npos)) {
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: both sources are not in the current queryPlan")
        //make left source the current queryPlan
        leftQueryPlan = QueryPlanBuilder::createQueryPlan(leftSourceName);

        std::string generalSubqueryName = "SubQuery_";
        if (pattern.getSources().at(it->second.getRightChildId()).compare(0, generalSubqueryName.size(), generalSubqueryName)
            == 0) {
            auto subqueryPlan = subQueries.find(pattern.getSources().at(it->second.getRightChildId()));
            rightQueryPlan = subqueryPlan->second;// the queryPlan from the subquery
            ++it;
            auto subqueryLeftQueryName = pattern.getSources().at(it->second.getLeftChildId());
            // subquery handling for join
            auto firstOperatorInQuery = pattern.getOperatorList().begin();
            auto subqueryOpName = firstOperatorInQuery->second.getOperatorName();

            if (subqueryOpName == operaterName) {
                ++firstOperatorInQuery;
                subqueryOpName = firstOperatorInQuery->second.getOperatorName();
                auto subqueryPlanLeft = QueryPlanBuilder::createQueryPlan(subqueryLeftQueryName);
                rightQueryPlan = addBinaryOperatorToQueryPlan(subqueryOpName, it, subqueryPlanLeft);
            }
            auto tmpleftqp = leftQueryPlan;
            leftQueryPlan = rightQueryPlan;
            rightQueryPlan = tmpleftqp;

        } else {
            rightQueryPlan = QueryPlanBuilder::createQueryPlan(rightSourceName);
        }
    } else {
        // making sure we don't use the already consumed sources from our subquery again
        auto leftConsumed = queryPlan->getSourceConsumed().find(leftSourceName) != std::string::npos;
        auto rightConsumed = queryPlan->getSourceConsumed().find(rightSourceName) != std::string::npos;
        if (leftConsumed && rightConsumed) {
            return queryPlan;
        }
        leftQueryPlan = queryPlan;
        rightQueryPlan = checkIfSourceIsAlreadyConsumedSource(leftSourceName, rightSourceName, queryPlan);
    }

    if (operaterName == "OR") {
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: addUnionOperator")
        leftQueryPlan = QueryPlanBuilder::addUnion(leftQueryPlan, rightQueryPlan);
    } else {
        // Seq and And require a window, so first we create and check the window operator
        auto timeMeasurements = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
        if (timeMeasurements.first.getTime() != TimeMeasure(0).getTime() || pattern.getSinks().size() < 1) {
            auto windowType =
                Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), timeMeasurements.first, timeMeasurements.second);

            //Next, we add artificial key attributes to the sources in order to reuse the join-logic later
            std::string cepLeftKey = keyAssignment("cep_leftKey");
            std::string cepRightKey = keyAssignment("cep_rightKey");

            //next: add Map operator that maps the attributes with value 1 to the left and right source
            leftQueryPlan = QueryPlanBuilder::addMap(Attribute(cepLeftKey) = 1, leftQueryPlan);
            rightQueryPlan = QueryPlanBuilder::addMap(Attribute(cepRightKey) = 1, rightQueryPlan);

            //then, define the artificial attributes as key attributes
            NES_DEBUG("NesCEPQueryPlanCreater: add name cepLeftKey {} and name cepRightKey {}", cepLeftKey, cepRightKey);
            ExpressionItem onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
            ExpressionItem onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
            auto joinExpression = onLeftKey == onRightKey;
            auto leftKeyFieldAccess = onLeftKey.getExpressionNode()->as<FieldAccessExpressionNode>();
            auto rightKeyFieldAccess = onRightKey.getExpressionNode()->as<FieldAccessExpressionNode>();

            leftQueryPlan = QueryPlanBuilder::addJoin(leftQueryPlan,
                                                      rightQueryPlan,
                                                      joinExpression,
                                                      windowType,
                                                      Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT);

            if (operaterName == "SEQ") {
                // for SEQ we need to add additional filter for order by time
                auto timestamp = windowType->as<Windowing::TimeBasedWindowType>()->getTimeCharacteristic()->getField()->getName();
                // to guarantee a correct order of events by time (sequence) we need to identify the correct source and its timestamp
                // in case of composed streams on the right branch
                auto sourceNameRight = rightQueryPlan->getSourceConsumed();
                std::regex r1("-_+");
                sourceNameRight = std::regex_replace(sourceNameRight, r1, "");
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
                sourceNameLeft = std::regex_replace(sourceNameLeft, r1, "");
                // in case of composed sources on the left branch
                if (sourceNameLeft.find("_") != std::string::npos) {
                    // we find the most right source and use its timestamp for the filter constraint
                    uint64_t posStart = sourceNameLeft.find_first_of("_");
                    sourceNameLeft = sourceNameLeft.substr(0, posStart) + "$" + timestamp;
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

ExpressionNodePtr NesCEPQueryPlanCreator::getExpressionItem(std::string contextValueAsString) {
    ExpressionNodePtr expressionNode;
    // check if context value is a number (guaranteed by grammar if first symbol is - or a digit)
    // and if so, create ConstantExpressionNode
    if (contextValueAsString.at(0) == '-' || isdigit(contextValueAsString.at(0))) {
        if (contextValueAsString.find('.') != std::string::npos) {
            // if string contains a dot, it has to be a float number
            auto constant = std::stof(contextValueAsString);
            expressionNode = NES::ExpressionItem(constant).getExpressionNode();
        } else {
            // else it must be an integer (guaranteed by the grammar)
            auto constant = std::stoi(contextValueAsString);
            expressionNode = NES::ExpressionItem(constant).getExpressionNode();
        }
    } else {// else FieldExpressionNode
        std::string streamName = contextValueAsString.substr(0, contextValueAsString.find("."));
        std::string attributeName;
        if (contextValueAsString.find(".") == std::string::npos) {
            attributeName = contextValueAsString;
        } else {
            attributeName = contextValueAsString.substr(contextValueAsString.find(".") + 1,
                                                        contextValueAsString.at(contextValueAsString.size() - 1));
        }
        NES_DEBUG("the attribute name is {}", attributeName);
        std::string attributeWithFullQualifiedName;
        auto it = pattern.getAliasList().find(streamName);
        if (it != pattern.getAliasList().end()) {
            attributeWithFullQualifiedName = it->second + "$" + attributeName;
        } else {
            attributeWithFullQualifiedName = attributeName;
        }

        if (inWhere) {
            expressionNode = NES::Attribute(attributeWithFullQualifiedName).getExpressionNode();
            NES_DEBUG("NesCEPQueryPlanCreator: add left expression with full qualified name for attributes: {}",
                      attributeWithFullQualifiedName);
        }
    }

    return expressionNode;
}

}// namespace NES::Parsers
