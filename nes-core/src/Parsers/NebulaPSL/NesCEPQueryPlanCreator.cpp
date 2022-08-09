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

#include "API/QueryAPI.hpp"
#include "API/Windowing.hpp"
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Parsers/NebulaPSL/NesCEPQueryPlanCreator.h>
#include <Parsers/NebulaPSL/gen/NesCEPBaseListener.h>
#include <Parsers/NebulaPSL/gen/NesCEPParser.h>

namespace NES {

void NesCEPQueryPlanCreator::enterListEvents(NesCEPParser::ListEventsContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : enterListEvents: init tree walk and initialize read out of AST ");
    this->currentElementPointer = nodeId;
    this->currentOperatorPointer = nodeId;
    nodeId++;
    NesCEPBaseListener::enterListEvents(context);
}

void NesCEPQueryPlanCreator::enterEventElem(NesCEPParser::EventElemContext* cxt) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitEventElem: found a stream source " + cxt->getStart()->getText());
    //create sources pair
    pattern.addSource(std::make_pair(sourceCounter, cxt->getStart()->getText()));
    this->lastSeenSourcePtr = sourceCounter;
    sourceCounter++;
    NES_DEBUG("NesCEPQueryPlanCreator : exitEventElem: inserted " + cxt->getStart()->getText() + " to sources");

    this->currentElementPointer = nodeId;
    nodeId++;
    NesCEPBaseListener::exitEventElem(cxt);
}

void NesCEPQueryPlanCreator::exitOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitOperatorRule: create a node for the operator " + context->getText());
    //create Operator node
    NebulaPSLOperatorNode* node = new NebulaPSLOperatorNode(nodeId);
    node->setParentNodeId(-1);
    node->setEventName(context->getText());
    node->setLeftChildId(lastSeenSourcePtr);
    node->setRightChildId(lastSeenSourcePtr + 1);
    pattern.addOperatorNode(node);
    currentOperatorPointer = nodeId;
    this->currentElementPointer = nodeId;
    nodeId++;
    NesCEPBaseListener::exitOperatorRule(context);
}

void NesCEPQueryPlanCreator::exitInputStream(NesCEPParser::InputStreamContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitInputStream: replace alias with streamName " + context->getText());
    std::string streamName = context->getStart()->getText();
    std::string alias = context->getStop()->getText();
    //replace alias in the list of sources
    std::map<int, std::string> sources = pattern.getSources();
    std::map<int, std::string>::iterator iter;
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
    std::string timeUnit = cxt->intervalType()->getText();
    int time = std::stoi(cxt->getStart()->getText());
    pattern.setWindow(std::make_pair(timeUnit, time));
    NesCEPBaseListener::exitInterval(cxt);
}

void NesCEPQueryPlanCreator::enterOutAttribute(NesCEPParser::OutAttributeContext* context) {
    auto attr = NES::Attribute(context->NAME()->getText()).getExpressionNode();
    pattern.addProjectionField(attr);
}

void NesCEPQueryPlanCreator::exitSinkList(NesCEPParser::SinkListContext* context) {
    query = createQueryFromPatternList();
    const std::vector<NES::OperatorNodePtr>& rootOperators = query.getQueryPlan()->getRootOperators();

    for (std::shared_ptr<NES::SinkDescriptor> it : pattern.getSinks()) {
        query.multipleSink(it, rootOperators);
    }
    NesCEPBaseListener::exitSinkList(context);
}

void NesCEPQueryPlanCreator::enterSink(NesCEPParser::SinkContext* context) {

    std::string sinkType = context->sinkType()->getText();
    std::shared_ptr<NES::SinkDescriptor> sinkDescriptorPtr;

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

void NesCEPQueryPlanCreator::enterQuantifiers(NesCEPParser::QuantifiersContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: " + context->getText())
    //create Operator node
    NebulaPSLOperatorNode* node = new NebulaPSLOperatorNode(nodeId);
    node->setParentNodeId(-1);
    node->setEventName("TIMES");
    node->setLeftChildId(lastSeenSourcePtr);
    if (context->LBRACKET()) {
        if (context->D_POINTS()) {//e.g., A[2:10]
            NES_DEBUG("NesCEPQueryPlanCreator : enterQuantifiers: Times with Min: " + context->iterMin()->INT()->getText()
                      + "and Max " + context->iterMin()->INT()->getText());
            node->setMinMax(
                std::make_pair(stoi(context->iterMin()->INT()->getText()), stoi(context->iterMax()->INT()->getText())));
        } else {
            node->setMinMax(std::make_pair(stoi(context->INT()->getText()), stoi(context->INT()->getText())));
        }
    } else if (context->PLUS()) {//A+
        node->setMinMax(std::make_pair(0, 0));
    } else if (context->STAR()) {//[]*   //TODO unbounded iteration variant not yet implemented
        NES_ERROR("NesCEPQueryPlanCreator : enterQuantifiers: NES currently does not support the iteration variant *")
    }
    pattern.addOperatorNode(node);
    currentOperatorPointer = nodeId;
    this->currentElementPointer = nodeId;
    nodeId++;
    NesCEPBaseListener::enterQuantifiers(context);
}

void NesCEPQueryPlanCreator::exitBinaryComparasionPredicate(NesCEPParser::BinaryComparasionPredicateContext* context) {

    std::string comparisonOp = context->comparisonOperator()->getText();
    auto left = NES::Attribute(currentLeftExp).getExpressionNode();
    auto right = NES::Attribute(currentRightExp).getExpressionNode();
    NES::ExpressionNodePtr expression;
    NES_DEBUG("NesCEPQueryPlanCreator: exitBinaryComparisonPredicate: add filters " + currentLeftExp + comparisonOp
              + currentRightExp)

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
    pattern.addExpression(expression);
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
    if (pattern.getOperatorList().empty() && pattern.getSources().size() == 1) {
        query = NES::Query::from(pattern.getSources().at(0));
    } else {
        for (auto it = pattern.getOperatorList().begin(); it != pattern.getOperatorList().end(); ++it) {
            auto opName = it->second->getEventName();
            if (opName == "OR" || opName == "SEQ" || opName == "AND") {
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add binary operator " + opName)
                // find left (query) and right branch (subquery) of binary operator
                NES::Query subQueryLeft = NES::Query::from(pattern.getSources().at(it->second->getLeftChildId()));
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + pattern.getSources().at(it->second->getLeftChildId()))
                NES::Query subQueryRight = NES::Query::from(pattern.getSources().at(it->second->getRightChildId()));
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + pattern.getSources().at(it->second->getRightChildId()))

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
                NES::Query subQueryLeft = NES::Query::from(pattern.getSources().at(it->second->getLeftChildId()));
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + pattern.getSources().at(it->second->getLeftChildId()))
                auto window = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
                int min = it->second->getMinMax().first;
                int max = it->second->getMinMax().second;
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

std::pair<TimeMeasure, TimeMeasure> NesCEPQueryPlanCreator::transformWindowToTimeMeasurements(std::string timeMeasure, int time) {

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

//Getter and Setter
int NesCEPQueryPlanCreator::GetLastSeenSourcePtr() const { return lastSeenSourcePtr; }
void NesCEPQueryPlanCreator::SetLastSeenSourcePtr(int last_seen_source_ptr) { lastSeenSourcePtr = last_seen_source_ptr; }
int NesCEPQueryPlanCreator::GetSourceCounter() const { return sourceCounter; }
void NesCEPQueryPlanCreator::SetSourceCounter(int source_counter) { sourceCounter = source_counter; }
int NesCEPQueryPlanCreator::getNodeId() const { return nodeId; }
void NesCEPQueryPlanCreator::setNodeId(int node_id) { nodeId = node_id; }
int NesCEPQueryPlanCreator::GetCurrentElementPointer() const { return currentElementPointer; }
void NesCEPQueryPlanCreator::setCurrentElementPointer(int current_element_pointer) {
    currentElementPointer = current_element_pointer;
}
int NesCEPQueryPlanCreator::GetCurrentParentPointer() const { return currentOperatorPointer; }
void NesCEPQueryPlanCreator::SetCurrentParentPointer(int current_parent_pointer) {
    currentOperatorPointer = current_parent_pointer;
}
const NES::Query& NesCEPQueryPlanCreator::getQuery() const { return query; }
void NesCEPQueryPlanCreator::SetQuery(const NES::Query& query) { NesCEPQueryPlanCreator::query = query; }
bool NesCEPQueryPlanCreator::IsInWhere() const { return inWhere; }
void NesCEPQueryPlanCreator::SetInWhere(bool in_where) { inWhere = in_where; }
bool NesCEPQueryPlanCreator::IsLeftFilter() const { return leftFilter; }
void NesCEPQueryPlanCreator::SetLeftFilter(bool left_filter) { leftFilter = left_filter; }
const std::string& NesCEPQueryPlanCreator::GetCurrentLeftExp() const { return currentLeftExp; }
void NesCEPQueryPlanCreator::SetCurrentLeftExp(const std::string& current_left_exp) { currentLeftExp = current_left_exp; }
const std::string& NesCEPQueryPlanCreator::GetCurrentRightExp() const { return currentRightExp; }
void NesCEPQueryPlanCreator::SetCurrentRightExp(const std::string& current_right_exp) { currentRightExp = current_right_exp; }

}// end namespace NES