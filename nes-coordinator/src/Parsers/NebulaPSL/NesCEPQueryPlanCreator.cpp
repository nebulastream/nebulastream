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
    NES_DEBUG("init tree walk and initialize read out of AST ");
    this->nodeId++;
    NesCEPBaseListener::enterListEvents(context);
}

void NesCEPQueryPlanCreator::enterCepPattern(NesCEPParser::CepPatternContext* cxt) {
    NES_DEBUG("Triggers the parsing of the WITHIN clause before reading the PATTERN clause");
    if (cxt->windowConstraints() != nullptr) {
        NES_DEBUG("Window constraints detected, invoking enterWindowConstraints...");
        NesCEPQueryPlanCreator::enterWindowConstraints(cxt->windowConstraints());
    } else {
        NES_DEBUG("No window constraints found in the CEP pattern.");
    }
}

void NesCEPQueryPlanCreator::enterEventElem(NesCEPParser::EventElemContext* cxt) {
    if (cxt->LPARENTHESIS()) {
        // Handle parentheses
        NesCEPQueryPlanCreator::handleParenthesizedExpression(cxt->listEvents());
    } else {
        NES_DEBUG(" Add new stream source  {}", cxt->event()->getText());
        //create sources pair, e.g., <identifier,SourceName>
        pattern.addSource(std::make_pair(sourceCounter, cxt->getStart()->getText()));
        this->lastSeenSourcePtr = sourceCounter;
        sourceCounter++;
    }
    this->nodeId++;
    NesCEPBaseListener::exitEventElem(cxt);
}

void NesCEPQueryPlanCreator::handleParenthesizedExpression(NesCEPParser::ListEventsContext* context) {
    NES_DEBUG(" Handle the following subquery  {}", context->getText());
    // Save current state
    uint32_t savedSourceCounter = sourceCounter;
    uint32_t savedNodeId = nodeId;
    uint32_t savedLastSeenSourcePtr = lastSeenSourcePtr;

    // Save the current pattern state
    NebulaPSLPattern savedPattern = pattern;
    // Create new sub-pattern for the nested expression
    NebulaPSLPattern subPattern;
    subPattern.setWindow(savedPattern.getWindow());
    // Assign new pattern
    pattern = subPattern;

    // Process the nested expression
    for (auto eventElem : context->eventElem()) {
        enterEventElem(eventElem);
        if (context->operatorRule().size() > 0) {
            exitOperatorRule(context->operatorRule(0));
        }
    }
    // Create subquery from nested expression
    QueryPlanPtr subQueryPlan = createQueryFromPatternList();

    // Restore original pattern state
    pattern = savedPattern;

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
    NES_DEBUG("Finished processing nested expression");
}

void NesCEPQueryPlanCreator::exitOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    NES_DEBUG("Create a node for the operator  {}", context->getText());
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
    NES_DEBUG(" Replace alias with streamName  {}", context->getText());
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

void NesCEPQueryPlanCreator::enterWindowConstraints(NesCEPParser::WindowConstraintsContext* cxt) {
    NES_DEBUG(" enterWindowConstraints with {}, {}", cxt->windowType()->getText(), cxt->timeConstraints()->NAME()->getText());
    Window window;
    window.type = cxt->windowType()->getText();// i.e., SLIDING, INTERVAL, or TUMBLING
    window.timeAttributeName = cxt->timeConstraints()->NAME()->getText();

    for (size_t i = 0; i < cxt->timeConstraints()->constraint().size(); ++i) {
        std::string paramName = cxt->timeConstraints()->constraint().at(i)->getText();
        // get time specifications
        std::string timeUnit = cxt->timeConstraints()->interval(i)->intervalType()->getText();
        int32_t time = std::stoi(cxt->timeConstraints()->interval(i)->INT()->getText());
        NES_DEBUG(" Param: {}, Time unit: '{}', Time value: {}", paramName, timeUnit, time);

        // Add parameters
        WindowParameter param = {paramName, {timeUnit, time}};
        window.parameters.push_back(param);
    }
    this->pattern.setWindow(window);
    NesCEPBaseListener::exitWindowConstraints(cxt);
}

void NesCEPQueryPlanCreator::enterOutAttribute(NesCEPParser::OutAttributeContext* context) {
    //get projection fields
    auto attributeField = NES::Attribute(context->NAME()->getText()).getExpressionNode();
    pattern.addProjectionField(attributeField);
}

void NesCEPQueryPlanCreator::enterSinkWithParameters(NesCEPParser::SinkWithParametersContext* context) {
    std::string sinkType = context->sinkType()->getText();
    NES_DEBUG("Specify Sink with Type: {}", sinkType)
    SinkDescriptorPtr sinkDescriptor;

    if (sinkType == "FILE") {
        // get fileName
        if (context->parameters()->parameter().at(0)->getText() == "fileName") {
            sinkDescriptor = NES::FileSinkDescriptor::create(context->parameters()->value(0)->getText());
            NES_DEBUG(" Create sinkDescriptor for File Sink with name : {}", context->parameters()->value(0)->getText());
        } else {
            NES_THROW_RUNTIME_ERROR("Unknown parameter(s) for FileSink {}", context->parameters()->getText());
        }
    } else if (sinkType == "MQTT") {
        // get topic and address
        NES_DEBUG(" number of parameters : {}", context->parameters()->parameter().size());
        if (context->parameters()->parameter().size() == 2) {
            if (context->parameters()->parameter().at(0)->getStart()->getText() == "address") {
                sinkDescriptor = NES::MQTTSinkDescriptor::create(context->parameters()->value(0)->getText(),
                                                                 context->parameters()->value(1)->getText());
            } else {
                sinkDescriptor = NES::MQTTSinkDescriptor::create(context->parameters()->value(1)->getText(),
                                                                 context->parameters()->value(0)->getText());
            }
            NES_DEBUG(" Create sinkDescriptor for MQTT Sink with address : {} and topic {}",
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
    NES_DEBUG(" Found Sink Type: {}", sinkType)
    SinkDescriptorPtr sinkDescriptor;

    if (sinkType == "PRINT") {
        sinkDescriptor = NES::PrintSinkDescriptor::create();
    } else if (sinkType == "NETWORK") {
        sinkDescriptor = NES::NullOutputSinkDescriptor::create();
    } else if (sinkType == "NULLOUTPUT") {
        sinkDescriptor = NES::NullOutputSinkDescriptor::create();
    } else {
        NES_THROW_RUNTIME_ERROR("Unknown sink was specified");
    }
    pattern.addSink(sinkDescriptor);
    NesCEPBaseListener::exitSinkWithoutParameters(context);
}

void NesCEPQueryPlanCreator::enterQuantifiers(NesCEPParser::QuantifiersContext* context) {
    NES_DEBUG(" enterQuantifiers: {}", context->getText())
    //method that specifies the times operator which has several cases
    //create Operator node and add specification
    NebulaPSLOperator timeOperator = NebulaPSLOperator(nodeId);
    timeOperator.setParentNodeId(-1);
    timeOperator.setOperatorName("TIMES");
    timeOperator.setLeftChildId(lastSeenSourcePtr);
    if (context->LBRACKET()) {    // context contains []
        if (context->D_POINTS()) {//e.g., A[2:10] means that we expect at least 2 and maximal 10 occurrences of A
            NES_DEBUG(" Times with Min: {} and Max {}",
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
        NES_THROW_RUNTIME_ERROR(" NES currently does not support the iteration variant *");
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
    NES_DEBUG(" Create query from AST elements");

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
                if (this->pattern.getWindow().type.empty() && this->pattern.getWindow().parameters.empty()
                    && operatorName != "OR") {
                    // window must be specified for an operator, throw exception
                    NES_THROW_RUNTIME_ERROR("WITHIN keyword for time interval missing but must be specified for operators.");
                }
            }
            // add binary operators
            if (operatorName == "OR" || operatorName == "SEQ" || operatorName == "AND") {
                queryPlan = addBinaryOperatorToQueryPlan(operatorName, operatorNode, queryPlan);
            } else if (operatorName == "TIMES") {//add times operator
                NES_DEBUG("CreateQueryFromPatternList: add unary operator {}", operatorName)
                auto sourceName = pattern.getSources().at(operatorNode->second.getLeftChildId());
                queryPlan = QueryPlanBuilder::createQueryPlan(sourceName);
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add times operator{}",
                          pattern.getSources().at(operatorNode->second.getLeftChildId()))

                queryPlan = QueryPlanBuilder::addMap(Attribute("Count") = 1, queryPlan);

                auto windowType = createTimeBasedWindow();

                if (windowType) {

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
                    NES_THROW_RUNTIME_ERROR("CreateQueryFromPatternList: The Iteration operator requires a time window.");
                }
            } else {
                NES_THROW_RUNTIME_ERROR("CreateQueryFromPatternList: Unknown CEP operator" << operatorName);
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

TimeMeasure NesCEPQueryPlanCreator::transformWindowToTimeMeasurements(std::pair<std::string, int> timeMeasure) const {
    if (timeMeasure.first == "MILLISECOND") {
        TimeMeasure measure = Milliseconds(timeMeasure.second);
        return measure;
    } else if (timeMeasure.first == "SECOND") {
        TimeMeasure measure = Seconds(timeMeasure.second);
        return measure;
    } else if (timeMeasure.first == "MINUTE") {
        TimeMeasure measure = Minutes(timeMeasure.second);
        return measure;
    } else if (timeMeasure.first == "HOUR") {
        TimeMeasure measure = Hours(timeMeasure.second);
        return measure;
    } else {
        // when we have a subquery we don't expect it to already include a timeMeasure since that comes at the end of the query
        NES_DEBUG("NesCEPQueryPlanCreator: transformWindowToTimeMeasurements: Unknown time measure: {}", timeMeasure.first);
        return Minutes(0);
    }
    return Minutes(0);
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
    } else if (pattern.getWindow().type == "INTERVAL") {
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

        Window window = pattern.getWindow();
        auto measure = createIntervalWindow();
        if (operaterName == "AND") {
            leftQueryPlan = QueryPlanBuilder::addIntervalJoin(leftQueryPlan,
                                                              rightQueryPlan,
                                                              joinExpression,
                                                              EventTime(Attribute(window.timeAttributeName)),
                                                              measure.first,
                                                              measure.second,
                                                              true,
                                                              measure.first,
                                                              measure.second,
                                                              true);

        } else if (operaterName == "SEQ") {
            leftQueryPlan = QueryPlanBuilder::addIntervalJoin(leftQueryPlan,
                                                              rightQueryPlan,
                                                              joinExpression,
                                                              EventTime(Attribute(window.timeAttributeName)),
                                                              0,
                                                              measure.second,
                                                              true,
                                                              measure.first,
                                                              measure.second,
                                                              true);
        }
    } else {
        // Seq and And require a window, so first we create and check the window operator
        auto windowType = createTimeBasedWindow();
        NES_DEBUG("NesCEPQueryPlanCreater: windowType {}", windowType->toString())
        if (windowType || pattern.getSinks().size() < 1) {

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
        NES_THROW_RUNTIME_ERROR("Both sources are already consumed and combined with a binary operator");
    } else if (queryPlan->getSourceConsumed().find(leftSourceName) == std::string::npos) {
        // right queryplan
        NES_DEBUG(" Create subqueryRight from {}", leftSourceName)
        return rightQueryPlan = QueryPlanBuilder::createQueryPlan(leftSourceName);
    } else if (queryPlan->getSourceConsumed().find(rightSourceName) == std::string::npos) {
        // right queryplan
        NES_DEBUG(" Create subqueryRight from {}", rightSourceName)
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
            NES_DEBUG("Add left expression with full qualified name for attributes: {}", attributeWithFullQualifiedName);
        }
    }

    return expressionNode;
}
WindowTypePtr NesCEPQueryPlanCreator::createTimeBasedWindow() const {
    Window window = pattern.getWindow();
    NES_DEBUG(" addBinaryOperatorToQueryPlan: Window Type {}", window.type);

    // Find the parameter size using std::find_if
    auto it = std::find_if(window.parameters.begin(), window.parameters.end(), [](const WindowParameter& param) {
        return param.name == "SIZE";
    });
    if (it == window.parameters.end()) {
        NES_THROW_RUNTIME_ERROR("WINDOW SIZE NOT SPECIFIED.");
    }

    if (window.type == "SLIDING") {
        if (window.parameters.size() == 2) {
            int indexSize = std::distance(window.parameters.begin(), it);
            int indexSlide = window.parameters.size() - indexSize - 1;
            TimeMeasure size = TimeMeasure(transformWindowToTimeMeasurements(window.parameters.at(indexSize).value));
            if (size.getTime() == TimeMeasure(0).getTime()) {
                return nullptr;
            }
            return Windowing::SlidingWindow::of(
                EventTime(Attribute(window.timeAttributeName)),
                size,
                TimeMeasure(transformWindowToTimeMeasurements(window.parameters.at(indexSlide).value)));

        } else {
            NES_THROW_RUNTIME_ERROR("Unknown Parameters in Sliding Window, expects only SIZE and SLIDE.");
        }
    } else if (window.type == "TUMBLING") {
        if (window.parameters.size() == 1) {
            int indexSize = std::distance(window.parameters.begin(), it);
            TimeMeasure size = TimeMeasure(transformWindowToTimeMeasurements(window.parameters.at(indexSize).value));
            if (size.getTime() == TimeMeasure(0).getTime()) {
                return nullptr;
            }
            return Windowing::TumblingWindow::of(EventTime(Attribute(window.timeAttributeName)), size);
        } else {
            NES_THROW_RUNTIME_ERROR("Unknown Parameters in Tumbling Window Type, expect only SIZE.");
        }
    } else {
        NES_THROW_RUNTIME_ERROR("Unknown Window Type, expect SLIDING, TUMBLING, INTERVAL.");
    }
    return nullptr;
}

std::pair<uint64_t, TimeUnit> NesCEPQueryPlanCreator::createIntervalWindow() const {
    Window window = pattern.getWindow();
    NES_DEBUG("Window Type {}", window.type);
    // Find the parameter size using std::find_if
    auto it = std::find_if(window.parameters.begin(), window.parameters.end(), [](const WindowParameter& param) {
        return param.name == "INTERVAL";
    });
    if (it == window.parameters.end()) {
        NES_THROW_RUNTIME_ERROR("INTERVAL for INTERVAL JOIN IS NOT SPECIFIED.");
    }

    if (window.parameters.size() == 1) {
        int indexSize = std::distance(window.parameters.begin(), it);
        auto timeMeasure = window.parameters.at(indexSize).value;
        auto timeUnit = Windowing::TimeUnit::Milliseconds();
        if (timeMeasure.second == 0) {
            NES_THROW_RUNTIME_ERROR("Window Length is zero, but must be greater than 0.");
        } else if (timeMeasure.first == "MILLISECOND") {
            timeUnit = Milliseconds();
        } else if (timeMeasure.first == "SECOND") {
            timeUnit = Seconds();
        } else if (timeMeasure.first == "MINUTE") {
            timeUnit = Minutes();
        } else if (timeMeasure.first == "HOUR") {
            timeUnit = Hours();
        }
        return std::pair<uint64_t, TimeUnit>(window.parameters.at(indexSize).value.second, timeUnit);
    } else {
        NES_THROW_RUNTIME_ERROR("Unknown Parameters in INTERVAL WINDOW Type, expect only INTERVAL.");
    }
}

}// namespace NES::Parsers
