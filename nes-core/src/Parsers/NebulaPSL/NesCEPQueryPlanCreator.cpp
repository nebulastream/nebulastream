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
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Parsers/NebulaPSL/NebulaPSLQueryPlanCreator.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>

namespace NES::Parsers {

void NesCEPQueryPlanCreator::enterListEvents(NesCEPParser::ListEventsContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : enterListEvents: init tree walk and initialize read out of AST ");
    this->currentElementPointer = this->nodeId;
    this->currentOperatorPointer = this->nodeId;
    this->nodeId++;
    NesCEPBaseListener::enterListEvents(context);
}

void NesCEPQueryPlanCreator::enterEventElem(NesCEPParser::EventElemContext* cxt) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitEventElem: found a stream source " + cxt->getStart()->getText());
    //create sources pair, e.g., <identifier,SourceName>
    pattern.addSource(std::make_pair(sourceCounter, cxt->getStart()->getText()));
    this->lastSeenSourcePtr = sourceCounter;
    sourceCounter++;
    NES_DEBUG("NesCEPQueryPlanCreator : exitEventElem: inserted " + cxt->getStart()->getText() + " to sources");
    this->currentElementPointer = this->nodeId;
    this->nodeId++;
    NesCEPBaseListener::exitEventElem(cxt);
}

void NesCEPQueryPlanCreator::exitOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitOperatorRule: create a node for the operator " + context->getText());
    //create Operator node and set attributes with context information
    NebulaPSLOperatorNode node = NebulaPSLOperatorNode(nodeId);
    node.setParentNodeId(-1);
    node.setEventName(context->getText());
    node.setLeftChildId(lastSeenSourcePtr);
    node.setRightChildId(lastSeenSourcePtr + 1);
    pattern.addOperatorNode(node);
    // move pointers
    currentOperatorPointer = nodeId;
    this->currentElementPointer = nodeId;
    nodeId++;
    NesCEPBaseListener::exitOperatorRule(context);
}

void NesCEPQueryPlanCreator::exitInputStream(NesCEPParser::InputStreamContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitInputStream: replace alias with streamName " + context->getText());
    std::string sourceName = context->getStart()->getText();
    std::string aliasName = context->getStop()->getText();
    //replace alias in the list of sources with actual sources name
    std::map<int32_t, std::string> sources = pattern.getSources();
    std::map<int32_t, std::string>::iterator iter;
    for (iter = sources.begin(); iter != sources.end(); iter++) {
        std::string currentEventName = iter->second;
        if (currentEventName == aliasName) {
            pattern.updateSource(iter->first, sourceName);
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
    NES_DEBUG("NesCEPQueryPlanCreator : exitInterval: " + cxt->getText());
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
    SinkDescriptorPtr sinkDescriptorPtr;

    if (sinkType == "Print") {
        sinkDescriptorPtr = NES::PrintSinkDescriptor::create();
    }
    if (sinkType == "File") {
        sinkDescriptorPtr = NES::FileSinkDescriptor::create(context->NAME()->getText());
    }
    if (sinkType == "MQTT") {
        sinkDescriptorPtr = NES::NullOutputSinkDescriptor::create();
    }
    if (sinkType == "Network") {
        sinkDescriptorPtr = NES::NullOutputSinkDescriptor::create();
    }
    if (sinkType == "NullOutput") {
        sinkDescriptorPtr = NES::NullOutputSinkDescriptor::create();
    }
    pattern.addSink(sinkDescriptorPtr);
    NesCEPBaseListener::enterSink(context);
}

void NesCEPQueryPlanCreator::exitSinkList(NesCEPParser::SinkListContext* context) {
    // add AST elements to this queryPlan
    createQueryFromPatternList();
    const std::vector<NES::OperatorNodePtr>& rootOperators = this->queryPlanPtr->getRootOperators();
    // add the sinks to the query plan
    for (SinkDescriptorPtr sinkDescriptor : pattern.getSinks()) {
        auto sinkOperator = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
        for (auto& rootOperator : rootOperators) {
            sinkOperator->addChild(rootOperator);
            this->queryPlanPtr->removeAsRootOperator(rootOperator);
            this->queryPlanPtr->addRootOperator(sinkOperator);
        }
    }
    NesCEPBaseListener::exitSinkList(context);
}

void NesCEPQueryPlanCreator::enterQuantifiers(NesCEPParser::QuantifiersContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: " + context->getText())
    //method that specifies the times operator which has several cases
    //create Operator node and add specification
    NebulaPSLOperatorNode timeOperatorNode = NebulaPSLOperatorNode(nodeId);
    timeOperatorNode.setParentNodeId(-1);
    timeOperatorNode.setEventName("TIMES");
    timeOperatorNode.setLeftChildId(lastSeenSourcePtr);
    if (context->LBRACKET()) {    // context contains []
        if (context->D_POINTS()) {//e.g., A[2:10] means that we expect at least 2 and maximal 10 occurrences of A
            NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: Times with Min: " + context->iterMin()->INT()->getText()
                      + "and Max " + context->iterMin()->INT()->getText());
            timeOperatorNode.setMinMax(
                std::make_pair(stoi(context->iterMin()->INT()->getText()), stoi(context->iterMax()->INT()->getText())));
        } else {// e.g., A[2] means that we except exact 2 occurrences of A
            timeOperatorNode.setMinMax(std::make_pair(stoi(context->INT()->getText()), stoi(context->INT()->getText())));
        }
    } else if (context->PLUS()) {//e.g., A+, means a occurs at least once
        timeOperatorNode.setMinMax(std::make_pair(0, 0));
    } else if (context->STAR()) {//[]*   //TODO unbounded iteration variant not yet implemented
        NES_ERROR("NesCEPQueryPlanCreator : enterQuantifiers: NES currently does not support the iteration variant *")
    }
    pattern.addOperatorNode(timeOperatorNode);
    //update pointer
    currentOperatorPointer = nodeId;
    this->currentElementPointer = nodeId;
    nodeId++;
    NesCEPBaseListener::enterQuantifiers(context);
}

void NesCEPQueryPlanCreator::exitBinaryComparasionPredicate(NesCEPParser::BinaryComparasionPredicateContext* context) {
    //retrieve the ExpressionNode for the filter and save it in the pattern expressionList
    std::string comparisonOperator = context->comparisonOperator()->getText();
    auto leftExpressionNode = NES::Attribute(this->currentLeftExp).getExpressionNode();
    auto rightExpressionNode = NES::Attribute(this->currentRightExp).getExpressionNode();
    NES::ExpressionNodePtr expression;
    NES_DEBUG("NesCEPQueryPlanCreator: exitBinaryComparisonPredicate: add filters " + this->currentLeftExp + comparisonOperator
              + this->currentRightExp)

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
    NES_DEBUG("NesCEPQueryPlanCreator: enterAttribute: " + cxt->getText())
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

void NesCEPQueryPlanCreator::createQueryFromPatternList() {
    NES_DEBUG("NesCEPQueryPlanCreator: createQueryFromPatternList: create query from AST elements")
    // if for simple patterns without binary CEP operators
    if (this->pattern.getOperatorList().empty() && this->pattern.getSources().size() == 1) {
        auto sourceName = pattern.getSources().at(0);
        this->queryPlanPtr = QueryPlanBuilder::createQueryPlan(sourceName);
        // else for pattern with binary operators
    } else {
        // iterate over OperatorList, create and add LogicalOperatorNodes
        for (auto it = pattern.getOperatorList().begin(); it != pattern.getOperatorList().end(); ++it) {
            auto operatorName = it->second.getEventName();
            // add binary operators
            if (operatorName == "OR" || operatorName == "SEQ" || operatorName == "AND") {
                addBinaryOperatorToQueryPlan(operatorName, it);
            } else if (operatorName == "TIMES") {//add times operator
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add unary operator " + operatorName)
                auto sourceName = pattern.getSources().at(it->second.getLeftChildId());
                this->queryPlanPtr = QueryPlanBuilder::createQueryPlan(sourceName);
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add times operator"
                          + pattern.getSources().at(it->second.getLeftChildId()))

                this->queryPlanPtr = QueryPlanBuilder::addMap(Attribute("Count") = 1, this->queryPlanPtr);

                //create window
                auto timeMeasurements = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
                if (timeMeasurements.first.getTime() != TimeMeasure(0).getTime()) {
                    auto windowType = Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")),
                                                                   timeMeasurements.first,
                                                                   timeMeasurements.second);
                    // check and add watermark
                    this->queryPlanPtr = QueryPlanBuilder::checkAndAddWatermarkAssignment(this->queryPlanPtr, windowType);
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
                    this->queryPlanPtr->appendOperatorAsNewRoot(op);

                    int32_t min = it->second.getMinMax().first;
                    int32_t max = it->second.getMinMax().second;

                    if (min != 0 && max != 0) {//TODO min = 1 max = 0
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
                        this->queryPlanPtr = QueryPlanBuilder::addFilter(predicate, this->queryPlanPtr);
                    }
                } else {
                    NES_ERROR("The Iteration operator requires a time window.")
                }
            } else {
                NES_ERROR("Unkown CEP operator" << operatorName);
            }
        }
    }
    if (!pattern.getExpressions().empty()) {
        addFilters();
    }
    if (!pattern.getProjectionFields().empty()) {
        addProjections();
    }
}

void NesCEPQueryPlanCreator::addFilters() {
    for (auto it = pattern.getExpressions().begin(); it != pattern.getExpressions().end(); ++it) {
        this->queryPlanPtr = QueryPlanBuilder::addFilter(*it, this->queryPlanPtr);
    }
}

std::pair<TimeMeasure, TimeMeasure> NesCEPQueryPlanCreator::transformWindowToTimeMeasurements(std::string timeMeasure,
                                                                                              int32_t timeUnit) {

    if (timeMeasure == "MILLISECOND") {
        TimeMeasure size = Minutes(timeUnit);
        TimeMeasure slide = Minutes(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "SECOND") {
        TimeMeasure size = Seconds(timeUnit);
        TimeMeasure slide = Seconds(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "MINUTE") {
        TimeMeasure size = Minutes(timeUnit);
        TimeMeasure slide = Minutes(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "HOUR") {
        TimeMeasure size = Hours(timeUnit);
        TimeMeasure slide = Hours(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else {
        NES_ERROR("NesCEPQueryPlanCreator: Unkown time measure " + timeMeasure)
    }
    return std::pair<TimeMeasure, TimeMeasure>(TimeMeasure(0), TimeMeasure(0));
}

void NesCEPQueryPlanCreator::addProjections() {
    this->queryPlanPtr = QueryPlanBuilder::addProject(pattern.getProjectionFields(), this->queryPlanPtr);
}

QueryPlanPtr NesCEPQueryPlanCreator::getQueryPlan() const { return this->queryPlanPtr; }

std::string NesCEPQueryPlanCreator::keyAssignment(std::string keyName) {
    //first, get unique ids for the key attributes
    auto cepRightId = Util::getNextOperatorId();
    //second, create a unique name for both key attributes
    std::string cepRightKey = keyName + std::to_string(cepRightId);
    return cepRightKey;
}

void NesCEPQueryPlanCreator::addBinaryOperatorToQueryPlan(std::string operaterName,
                                                          std::map<int, NebulaPSLOperatorNode>::const_iterator it) {
    NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: add binary operator " + operaterName)
    // find left (query) and right branch (subquery) of binary operator
    //left query plan
    NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryLeft from "
              + pattern.getSources().at(it->second.getLeftChildId()))
    auto leftSourceName = pattern.getSources().at(it->second.getLeftChildId());
    auto rightSourceName = pattern.getSources().at(it->second.getRightChildId());
    //check if and which source is already consumed, decide which queryPlan is righten side, i.e., to compose to this->queryPlan
    auto rightQueryPlan = checkIfSourceIsAlreadyConsumedSource(leftSourceName, rightSourceName);

    if (operaterName == "OR") {
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: addUnionOperator")
        this->queryPlanPtr = QueryPlanBuilder::addUnionOperator(this->queryPlanPtr, rightQueryPlan);
    } else {
        // Seq and And require a window, so first we create and check the window operator
        auto timeMeasurements = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
        if (timeMeasurements.first.getTime() != TimeMeasure(0).getTime()) {
            auto windowType = Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")),
                                                           timeMeasurements.first,
                                                           timeMeasurements.second);

            //Next, we add artificial key attributes to the sources in order to reuse the join-logic later
            std::string cepLeftKey = keyAssignment("cep_leftLeft");
            std::string cepRightKey = keyAssignment("cep_rightkey");

            //next: add Map operator that maps the attributes with value 1 to the left and right source
            this->queryPlanPtr = QueryPlanBuilder::addMap(Attribute(cepLeftKey) = 1, this->queryPlanPtr);
            rightQueryPlan = QueryPlanBuilder::addMap(Attribute(cepRightKey) = 1, rightQueryPlan);

            //then, define the artificial attributes as key attributes
            NES_DEBUG("NesCEPQueryPlanCreater: add name cepLeftKey " << cepLeftKey << "and name cepRightKey" << cepRightKey);
            ExpressionItem onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
            ExpressionItem onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
            auto leftKeyFieldAccess = onLeftKey.getExpressionNode()->as<FieldAccessExpressionNode>();
            auto rightKeyFieldAccess = onRightKey.getExpressionNode()->as<FieldAccessExpressionNode>();

            this->queryPlanPtr = QueryPlanBuilder::addJoinOperator(this->queryPlanPtr,
                                                                rightQueryPlan,
                                                                onLeftKey,
                                                                onRightKey,
                                                                windowType,
                                                                Join::LogicalJoinDefinition::CARTESIAN_PRODUCT);

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
                auto sourceNameLeft = this->queryPlanPtr->getSourceConsumed();
                // in case of composed sources on the left branch
                if (sourceNameLeft.find("_") != std::string::npos) {
                    // we find the most right source and use its timestamp for the filter constraint
                    uint64_t posStart = sourceNameLeft.find_last_of("_");
                    sourceNameLeft = sourceNameLeft.substr(posStart + 1) + "$" + timestamp;
                }// in case the left branch only contains 1 source we can just use it
                else {
                    sourceNameLeft = sourceNameLeft + "$" + timestamp;
                }
                NES_DEBUG("NesCEPQueryPlanCreater: ExpressionItem for Left Source "
                          << sourceNameLeft << "and ExpressionItem for Right Source " << sourceNameRight);
                //create filter expression and add it to queryPlan
                this->queryPlanPtr =
                    QueryPlanBuilder::addFilter(Attribute(sourceNameLeft) < Attribute(sourceNameRight), this->queryPlanPtr);
            }
        } else {
            NES_ERROR("NesCEPQueryPlanCreater: createQueryFromPatternList: Cannot create " + operaterName + "without a window.")
        }
    }
}
QueryPlanPtr NesCEPQueryPlanCreator::checkIfSourceIsAlreadyConsumedSource(std::basic_string<char> leftSourceName,
                                                                          std::basic_string<char> rightSourceName) {
    QueryPlanPtr rightQueryPlan = nullptr;
    if (this->queryPlanPtr->getSourceConsumed().find(leftSourceName) != std::string::npos
        && this->queryPlanPtr->getSourceConsumed().find(rightSourceName) != std::string::npos) {
        NES_ERROR("NesCEPQueryPlanCreater: Both sources are already consumed and combined with a binary operator")
    } else if (this->queryPlanPtr->getSourceConsumed().find(leftSourceName) == std::string::npos
               && this->queryPlanPtr->getSourceConsumed().find(rightSourceName) == std::string::npos) {
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: both sources are not in the current queryPlan")
        //make left source current queryPlan
        this->queryPlanPtr = QueryPlanBuilder::createQueryPlan(leftSourceName);
        //return right source as right queryPlan
        return rightQueryPlan = QueryPlanBuilder::createQueryPlan(rightSourceName);
    } else if (this->queryPlanPtr->getSourceConsumed().find(leftSourceName) == std::string::npos) {
        // right queryplan
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryRight from " + leftSourceName)
        return rightQueryPlan = QueryPlanBuilder::createQueryPlan(leftSourceName);
    } else if (this->queryPlanPtr->getSourceConsumed().find(rightSourceName) == std::string::npos) {
        // right queryplan
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryRight from " + rightSourceName)
        return rightQueryPlan = QueryPlanBuilder::createQueryPlan(rightSourceName);
    }
    return rightQueryPlan;
}

}// namespace NES::Parsers