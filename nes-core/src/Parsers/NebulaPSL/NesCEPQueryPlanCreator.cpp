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

#include <API/QueryAPI.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Parsers/NebulaPSL/NebulaPSLQueryPlanCreator.h>

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
    // simple patterns without binary CEP operators
    if (this->pattern.getOperatorList().empty() && this->pattern.getSources().size() == 1) {
        this->query = NES::Query::from(pattern.getSources().at(0));
    } else {
        for (auto it = pattern.getOperatorList().begin(); it != pattern.getOperatorList().end(); ++it) {
            auto opName = it->second.getEventName();
            if (opName == "OR" || opName == "SEQ" || opName == "AND") {
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add binary operator " + opName)
                // find left (query) and right branch (subquery) of binary operator
                NES::Query subQueryLeft = NES::Query::from(pattern.getSources().at(it->second.getLeftChildId()));
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + pattern.getSources().at(it->second.getLeftChildId()))
                NES::Query subQueryRight = NES::Query::from(pattern.getSources().at(it->second.getRightChildId()));
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + pattern.getSources().at(it->second.getRightChildId()))

                if (opName == "OR") {
                    query = subQueryLeft.orWith(subQueryRight);
                } else {
                    auto window = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
                    if (window.first.getTime() != TimeMeasure(0).getTime()) {
                        if (opName == "AND") {
                            query = subQueryLeft.andWith(subQueryRight)
                                        .window(Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")),
                                                                             window.first,
                                                                             window.second));
                        } else if (opName == "SEQ") {
                            query = subQueryLeft.seqWith(subQueryRight)
                                        .window(Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")),
                                                                             window.first,
                                                                             window.second));
                        }
                    } else {
                        NES_ERROR("NesCEPQueryPlanCreater: createQueryFromPatternList: Cannot create " + opName
                                  + "without a window.")
                    }
                }
            } else if (opName == "TIMES") {
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add unary operator " + opName)
                NES::Query subQueryLeft = NES::Query::from(pattern.getSources().at(it->second.getLeftChildId()));
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + pattern.getSources().at(it->second.getLeftChildId()))
                auto window = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
                int32_t min = it->second.getMinMax().first;
                int32_t max = it->second.getMinMax().second;
                if (min == max) {
                    if (min == 0) {
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
        }
    }
    if (!pattern.getExpressions().empty()) {
        addFilters();
    }
    if (!pattern.getProjectionFields().empty()) {
        addProjections();
    }
    return query;
}

void NesCEPQueryPlanCreator::addFilters() {
    for (auto it = pattern.getExpressions().begin(); it != pattern.getExpressions().end(); ++it) {
        query.filter(*it);
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

void NesCEPQueryPlanCreator::addProjections() { query.project(pattern.getProjectionFields()); }

const Query& NesCEPQueryPlanCreator::getQuery() const { return this->query; }

}// namespace NES::Parsers