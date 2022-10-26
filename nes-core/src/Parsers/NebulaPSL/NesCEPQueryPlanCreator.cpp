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

#include "Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp"
#include "Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp"
#include "Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp"
#include <API/QueryAPI.hpp>
#include <API/AttributeField.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Parsers/NebulaPSL/NebulaPSLQueryPlanCreator.h>
#include <Windowing/DistributionCharacteristic.hpp>

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
    std::string streamName = context->getStart()->getText();
    std::string alias = context->getStop()->getText();
    //replace alias in the list of sources with actual sources name
    std::map<int32_t, std::string> sources = pattern.getSources();
    std::map<int32_t, std::string>::iterator iter;
    for (iter = sources.begin(); iter != sources.end(); iter++) {
        std::string currentEventName = iter->second;
        if (currentEventName == alias) {
            pattern.updateSource(iter->first, streamName);
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
    //get projection field for output
    auto attr = NES::Attribute(context->NAME()->getText()).getExpressionNode();
    pattern.addProjectionField(attr);
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
    // create the queryPlan
    query = createQueryFromPatternList();
    const QueryPlanPtr& queryPlan = query.getQueryPlan();
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
    NesCEPBaseListener::exitSinkList(context);
}

void NesCEPQueryPlanCreator::enterQuantifiers(NesCEPParser::QuantifiersContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: " + context->getText())
    //method that specifies the times operator which has several cases
    //create Operator node and add specification
    NebulaPSLOperatorNode node = NebulaPSLOperatorNode(nodeId);
    node.setParentNodeId(-1);
    node.setEventName("TIMES");
    node.setLeftChildId(lastSeenSourcePtr);
    if (context->LBRACKET()) {    // context contains []
        if (context->D_POINTS()) {//e.g., A[2:10] means that we expect at least 2 and maximal 10 occurrences of A
            NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: Times with Min: " + context->iterMin()->INT()->getText()
                      + "and Max " + context->iterMin()->INT()->getText());
            node.setMinMax(
                std::make_pair(stoi(context->iterMin()->INT()->getText()), stoi(context->iterMax()->INT()->getText())));
        } else {// e.g., A[2] means that we except exact 2 occurrences of A
            node.setMinMax(std::make_pair(stoi(context->INT()->getText()), stoi(context->INT()->getText())));
        }
    } else if (context->PLUS()) {//e.g., A+, means a occurs at least once
        node.setMinMax(std::make_pair(0, 0));
    } else if (context->STAR()) {//[]*   //TODO unbounded iteration variant not yet implemented
        NES_ERROR("NesCEPQueryPlanCreator : enterQuantifiers: NES currently does not support the iteration variant *")
    }
    pattern.addOperatorNode(node);
    //update pointer
    currentOperatorPointer = nodeId;
    this->currentElementPointer = nodeId;
    nodeId++;
    NesCEPBaseListener::enterQuantifiers(context);
}

void NesCEPQueryPlanCreator::exitBinaryComparasionPredicate(NesCEPParser::BinaryComparasionPredicateContext* context) {
    //get the ExpressionNode for the Filter
    std::string comparisonOp = context->comparisonOperator()->getText();
    auto left = NES::Attribute(this->currentLeftExp).getExpressionNode();
    auto right = NES::Attribute(this->currentRightExp).getExpressionNode();
    NES::ExpressionNodePtr expression;
    NES_DEBUG("NesCEPQueryPlanCreator: exitBinaryComparisonPredicate: add filters " + this->currentLeftExp + comparisonOp
              + this->currentRightExp)

    if (comparisonOp == "<") {
        expression = NES::LessExpressionNode::create(left, right);
    }
    if (comparisonOp == "<=") {
        expression = NES::LessEqualsExpressionNode::create(left, right);
    }
    if (comparisonOp == ">") {
        expression = NES::GreaterExpressionNode::create(left, right);
    }
    if (comparisonOp == ">=") {
        expression = NES::GreaterEqualsExpressionNode::create(left, right);
    }
    if (comparisonOp == "==") {
        expression = NES::EqualsExpressionNode::create(left, right);
    }
    if (comparisonOp == "&&") {
        expression = NES::AndExpressionNode::create(left, right);
    }
    if (comparisonOp == "||") {
        expression = NES::OrExpressionNode::create(left, right);
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

NES::Query NesCEPQueryPlanCreator::createQueryFromPatternList() {
    NES_DEBUG("NesCEPQueryPlanCreator: createQueryFromPatternList: create query from AST elements")
    // if for simple patterns without binary CEP operators
    if (this->pattern.getOperatorList().empty() && this->pattern.getSources().size() == 1) {
        auto sourceName = pattern.getSources().at(0);
        auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
        auto queryPlan = QueryPlan::create(sourceOperator);
        this->queryPlan->setSourceConsumed(sourceName);
    // else for pattern with binary operators
    } else {
        // iterate over OperatorList, create and add LogicalOperatorNodes
        for (auto it = pattern.getOperatorList().begin(); it != pattern.getOperatorList().end(); ++it) {
            auto opName = it->second.getEventName();
            // check binary operators
            if (opName == "OR" || opName == "SEQ" || opName == "AND") {

                addBinaryOperatorToQueryPlan(opName, it);

            } else if (opName == "TIMES") {
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add unary operator " + opName)

                auto sourceName = pattern.getSources().at(it->second.getLeftChildId());
                auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
                auto subqueryPlan = QueryPlan::create(sourceOperator);
                this->queryPlan->addRootOperator(subqueryPlan->getRootOperators()[0]);
                //this->queryPlan->appendOperatorAsNewRoot(op);
                this->queryPlan->setSourceConsumed(sourceName);
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + pattern.getSources().at(it->second.getLeftChildId()))

                OperatorNodePtr op = LogicalOperatorFactory::createMapOperator(Attribute("Count") = 1);
                this->queryPlan->appendOperatorAsNewRoot(op);

                //create window
                auto window = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
                if (window.first.getTime() != TimeMeasure(0).getTime()) {
                    auto windowType =
                        Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), window.first, window.second);

                    //we use a on time trigger as default that triggers on each change of the watermark
                    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();



                    int32_t min = it->second.getMinMax().first;
                    int32_t max = it->second.getMinMax().second;

                    if (min == max) {
                        if (min == 0) {

                            // check if query contain watermark assigner, and add if missing (as default behaviour)
                            if (this->queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
                                // we only consider Event time in CEP
                                this->queryPlan->appendOperatorAsNewRoot(
                                    LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                                        Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                                        API::Milliseconds(0),
                                        windowType->getTimeCharacteristic()->getTimeUnit())));
                            }

                            auto windowDefinition = Windowing::LogicalWindowDefinition::create(API::Sum(Attribute("Count")),
                                                                                               windowType,
                                                                                               const DistributionCharacteristicPtr& distChar,
                                                                                               triggerPolicy,
                                                                                               const WindowActionDescriptorPtr& triggerAction,
                                                                                               0);

                            OperatorNodePtr op = LogicalOperatorFactory::createWindowOperator(windowDefinition);
                            this->queryPlan->appendOperatorAsNewRoot(op);

                            query = subQueryLeft.times().window(
                                Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), window.first, window.second));

                        } else {
                            query = subQueryLeft.times(min).window(
                                Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), window.first, window.second));
                        }
                    } else {
                        query = subQueryLeft.times(min, max).window(
                            Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), window.first, window.second));
                    }
                }
                else{
                    NES_ERROR("The Iteration operator requires a time window.")
                }
            }
            else{
                NES_ERROR("Unkown CEP operator" << opName);
            }
        }
    }
    if (!pattern.getExpressions().empty()) {
        addFilters();
    }
    if (!pattern.getProjectionFields().empty()) {
        addProjections();
    }
    return this->query;
}

void NesCEPQueryPlanCreator::addFilters() {
    for (auto it = pattern.getExpressions().begin(); it != pattern.getExpressions().end(); ++it) {
        OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(*it);
        this->queryPlan->appendOperatorAsNewRoot(op);
    }
}

std::pair<TimeMeasure, TimeMeasure> NesCEPQueryPlanCreator::transformWindowToTimeMeasurements(std::string timeMeasure,
                                                                                              int32_t time) {

    if (timeMeasure == "MILLISECOND") {
        TimeMeasure size = Minutes(time);
        TimeMeasure slide = Minutes(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "SECOND") {
        TimeMeasure size = Seconds(time);
        TimeMeasure slide = Seconds(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "MINUTE") {
        TimeMeasure size = Minutes(time);
        TimeMeasure slide = Minutes(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "HOUR") {
        TimeMeasure size = Hours(time);
        TimeMeasure slide = Hours(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else {
        NES_ERROR("NesCEPQueryPlanCreator: Unkown time measure " + timeMeasure)
    }

    return std::pair<TimeMeasure, TimeMeasure>(TimeMeasure(0), TimeMeasure(0));
}

void NesCEPQueryPlanCreator::addProjections() {
    OperatorNodePtr op = LogicalOperatorFactory::createProjectionOperator(pattern.getProjectionFields());
    this->queryPlan->appendOperatorAsNewRoot(op);
   }

const Query& NesCEPQueryPlanCreator::getQuery() const { return this->query; }

std::string NesCEPQueryPlanCreator::keyAssignment(std::string keyName) {
    //first, get unique ids for the key attributes
    auto cepRightId = Util::getNextOperatorId();
    //second, create a unique name for both key attributes
    std::string cepRightKey = keyName + std::to_string(cepRightId);
    return cepRightKey;
}

void NesCEPQueryPlanCreator::addBinaryOperatorToQueryPlan(std::string opName, std::map<int, NebulaPSLOperatorNode>::const_iterator it) {
    NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add binary operator " + opName)
    // find left (query) and right branch (subquery) of binary operator
    //left
    NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: create subqueryLeft from "
              + pattern.getSources().at(it->second.getLeftChildId()))
    auto leftSourceName = pattern.getSources().at(it->second.getLeftChildId());
    auto leftSourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(leftSourceName));
    auto leftQueryPlan = QueryPlan::create(leftSourceOperator);
    leftQueryPlan->setSourceConsumed(leftSourceName);
    // right
    NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: create subqueryRight from "
              + pattern.getSources().at(it->second.getRightChildId()))
    auto rightSourceName = pattern.getSources().at(it->second.getRightChildId());
    auto rightSourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(leftSourceName));
    auto rightQueryPlan = QueryPlan::create(rightSourceOperator);
    rightQueryPlan->setSourceConsumed(rightSourceName);

    if (opName == "OR") {
        OperatorNodePtr op = LogicalOperatorFactory::createUnionOperator();
        this->queryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
        this->queryPlan->addRootOperator(leftQueryPlan->getRootOperators()[0]);
        this->queryPlan->appendOperatorAsNewRoot(op);
        //update source name
        auto newSourceName =
            Util::updateSourceName(queryPlan->getSourceConsumed(), rightQueryPlan->getSourceConsumed());
        this->queryPlan->setSourceConsumed(newSourceName);
    } else {
        // Seq and And require a window, so first we create and check the window operator
        auto window = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
        if (window.first.getTime() != TimeMeasure(0).getTime()) {
            auto windowType =
                Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")),
                                             window.first,
                                             window.second);

            //we use a on time trigger as default that triggers on each change of the watermark
            auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();

            //Next, we add artificial key attributes to the sources in order to reuse the join-logic later
            std::string cepLeftKey = keyAssignment("cep_leftLeft");
            std::string cepRightKey = keyAssignment("cep_rightkey");

            //next: add Map operator that maps the attributes with value 1 to the left and right source
            OperatorNodePtr opLeft = LogicalOperatorFactory::createMapOperator(Attribute(cepLeftKey) = 1);
            leftQueryPlan->appendOperatorAsNewRoot(opLeft);
            OperatorNodePtr opRight = LogicalOperatorFactory::createMapOperator(Attribute(cepRightKey) = 1);
            rightQueryPlan->appendOperatorAsNewRoot(opRight);

            //then, define the artificial attributes as key attributes
            NES_DEBUG("Query: add name cepLeftKey " << cepLeftKey << "and name cepRightKey" << cepRightKey);
            ExpressionItem onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
            ExpressionItem onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
            auto leftKeyFieldAccess = onLeftKey.getExpressionNode()->as<FieldAccessExpressionNode>();
            auto rightKeyFieldAccess = onRightKey.getExpressionNode()->as<FieldAccessExpressionNode>();

            //Join Logic (almost equivalent to QueryPlan)
            //we use a lazy NL join because this is currently the only one that is implemented
            auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();

            // we use a complete window type as we currently do not have a distributed join
            auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();

            NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid right query plan");

            // check if query contain watermark assigner, and add if missing (as default behaviour)
            if (leftQueryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
                // we only consider Event time in CEP
                leftQueryPlan->appendOperatorAsNewRoot(
                    LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                        Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                        API::Milliseconds(0),
                        windowType->getTimeCharacteristic()->getTimeUnit())));
            }

            if (rightQueryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
                auto op =
                    LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                        Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                        API::Milliseconds(0),
                        windowType->getTimeCharacteristic()->getTimeUnit()));
                rightQueryPlan->appendOperatorAsNewRoot(op);
            }

            //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
            //TODO(Ventura?>Steffen) can we know this at this query submission time?
            auto joinDefinition = Join::LogicalJoinDefinition::create(leftKeyFieldAccess,
                                                                      rightKeyFieldAccess,
                                                                      windowType,
                                                                      distrType,
                                                                      triggerPolicy,
                                                                      triggerAction,
                                                                      1,
                                                                      1,
                                                                      Join::LogicalJoinDefinition::CARTESIAN_PRODUCT);

            auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
            this->queryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
            this->queryPlan->addRootOperator(leftQueryPlan->getRootOperators()[0]);
            this->queryPlan->appendOperatorAsNewRoot(op);
            //update source name
            auto sourceNameLeft = leftQueryPlan->getSourceConsumed();
            auto sourceNameRight = rightQueryPlan->getSourceConsumed();
            auto newSourceName = Util::updateSourceName(sourceNameLeft, sourceNameRight);
            auto newSourceNames2 = Util::updateSourceName(this->queryPlan->getSourceConsumed(),newSourceName);
            this->queryPlan->setSourceConsumed(newSourceNames2);

            if (opName == "SEQ") {
                // for SEQ we need to add additional filter for order by time
                auto timestamp = windowType->getTimeCharacteristic()->getField()->getName();
                // to guarantee a correct order of events by time (sequence) we need to identify the correct source and its timestamp
                // in case of composed streams on the right branch
                if (sourceNameRight.find("_") != std::string::npos) {
                    // we find the most left source and use its timestamp for the filter constraint
                    uint64_t posStart = sourceNameRight.find("_");
                    uint64_t posEnd = sourceNameRight.find("_", posStart + 1);
                    sourceNameRight = sourceNameRight.substr(posStart + 1, posEnd - 2) + "$" + timestamp;
                }// in case the right branch only contains 1 source we can just use it
                else {
                    sourceNameRight = sourceNameRight + "$" + timestamp;
                }
                // in case of composed sources on the left branch
                if (sourceNameLeft.find("_") != std::string::npos) {
                    // we find the most right source and use its timestamp for the filter constraint
                    uint64_t posStart = sourceNameLeft.find_last_of("_");
                    sourceNameLeft = sourceNameLeft.substr(posStart + 1) + "$" + timestamp;
                }// in case the left branch only contains 1 source we can just use it
                else {
                    sourceNameLeft = sourceNameLeft + "$" + timestamp;
                }
                NES_DEBUG("ExpressionItem for Left Source " << sourceNameLeft << "and ExpressionItem for Right Source " << sourceNameRight);
                //create Filter
                OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(Attribute(sourceNameLeft) < Attribute(sourceNameRight));
                this->queryPlan->appendOperatorAsNewRoot(op);
            }
        } else {
            NES_ERROR("NesCEPQueryPlanCreater: createQueryFromPatternList: Cannot create " + opName
                      + "without a window.")
        }
    }

}

}// namespace NES::Parsers