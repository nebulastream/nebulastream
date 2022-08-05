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

void NesCEPQueryPlanCreator::exitEventElem(NesCEPParser::EventElemContext* cxt) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitEventElem: found a stream source " + cxt->getText());
    //create sources pair
    sources.insert(std::make_pair(sourceCounter, cxt->getText()));
    this->lastSeenSourcePtr = sourceCounter;
    sourceCounter++;
    NES_DEBUG("NesCEPQueryPlanCreator : exitEventElem: inserted " + cxt->getText() + " to sources");

    this->currentElementPointer = nodeId;
    nodeId++;
    NesCEPBaseListener::exitEventElem(cxt);
}

void NesCEPQueryPlanCreator::enterOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    NesCEPBaseListener::enterOperatorRule(context);
}

void NesCEPQueryPlanCreator::exitOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    NES_DEBUG("NesCEPQueryPlanCreator : exitOperatorRule: create a node for the operator " + context->getText());
    //create Operator node
    NePSLPatternEventNode* node = new NePSLPatternEventNode(nodeId);
    node->setParentNodeId(-1);
    node->setEventName(context->getText());
    node->setLeftChildId(lastSeenSourcePtr);
    node->setRightChildId(lastSeenSourcePtr + 1);
    this->operatorList[nodeId] = node;
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
    std::map<int, std::string>::iterator iter;
    for (iter = sources.begin(); iter != sources.end(); iter++) {
        std::string currentEventName = iter->second;
        if (currentEventName == alias) {
            iter->second = streamName;
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

    std::string timeUnit = cxt->intervalType()->getText();
    int time = std::stoi(cxt->getStart()->getText());
    setWindow(std::make_pair(timeUnit, time));
    NesCEPBaseListener::exitInterval(cxt);
}

void NesCEPQueryPlanCreator::enterOutAttribute(NesCEPParser::OutAttributeContext* context) {
    auto attr = NES::Attribute(context->NAME()->getText());
    auto pos = projectionFields.begin();
    projectionFields.insert(pos, attr);
}

void NesCEPQueryPlanCreator::exitSinkList(NesCEPParser::SinkListContext* context) {
    query = createQueryFromPatternList();
    const std::vector<NES::OperatorNodePtr>& rootOperators = query.getQueryPlan()->getRootOperators();

    for (std::shared_ptr<NES::SinkDescriptor> tmpItr : sinks) {
        std::shared_ptr<NES::SinkDescriptor> sink = tmpItr;
        query.multipleSink(sink, rootOperators);
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
    sinks.push_back(sinkDescriptorPtr);
    NesCEPBaseListener::enterSink(context);
}

void NesCEPQueryPlanCreator::enterEvent(NesCEPParser::EventContext* cxt) { NesCEPBaseListener::enterEvent(cxt); }

void NesCEPQueryPlanCreator::enterQuantifiers(NesCEPParser::QuantifiersContext* context) {
    std::map<int, NePSLPatternEventNode*>::iterator tmpIt = operatorList.begin();
    std::advance(tmpIt, currentElementPointer);
    NePSLPatternEventNode event = *tmpIt->second;
    event.setIteration(true);
    if (context->STAR()) {
        //[]*
        //TODO not yet implemented
        //event->setIterMin(0);
        //event->setIterMax(LLONG_MAX);
    } else if (context->PLUS() && !context->LBRACKET()) {
        //e.g., A+
        event.setIterMin(1);
        event.setIterMax(LLONG_MAX);
    } else if (context->D_POINTS()) {
        //e.g., A[2:4], between 2 to 4 occurances
        //Consecutive options not yet supported
        event.setIterMin(stoi(context->iterMin()->INT()->getText()));
        event.setIterMax(stoi(context->iterMax()->INT()->getText()));
    } else if (context->PLUS() && context->LBRACKET()) {
        // e.g., A[2]+ is more than 2
        event.setIterMin(stoi(context->INT()->getText()));
        event.setIterMax(LLONG_MAX);
    } else {
        event.setIterMin(0);
        event.setIterMax(stoi(context->INT()->getText()));
    }
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

    auto pos = expressions.begin();
    expressions.insert(pos, expression);
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
    if (operatorList.empty() && sources.size() == 1) {
        query = NES::Query::from(sources.at(0));
        if (!expressions.empty()) {
            addFilters();
        }
        if (!projectionFields.empty()) {
            addProjections();
        }
    } else {
        for (auto it = operatorList.begin(); it != operatorList.end(); ++it) {
            auto opName = it->second->getEventName();
            if (opName == "OR" || opName == "SEQ" || opName == "AND") {
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add binary operator " + opName)
                // find left (query) and right branch (subquery) of binary operator
                NES::Query subQueryLeft = NES::Query::from(sources.at(it->second->getLeftChildId()));
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + sources.at(it->second->getLeftChildId()))
                NES::Query subQueryRight = NES::Query::from(sources.at(it->second->getRightChildId()));
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + sources.at(it->second->getRightChildId()))

                if (opName == "OR") {
                    query = subQueryLeft.orWith(subQueryRight);
                } else {
                    std::pair<TimeMeasure, TimeMeasure> windowSizeSlide = transformWindowToTimeMeasurements();
                    if (opName == "AND") {
                        query = subQueryLeft.andWith(subQueryRight)
                                    .window(Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")),
                                                                         windowSizeSlide.first,
                                                                         windowSizeSlide.second));
                    }
                    if (opName == "SEQ") {
                        query = subQueryLeft.seqWith(subQueryRight)
                                    .window(Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")),
                                                                         windowSizeSlide.first,
                                                                         windowSizeSlide.second));
                    }
                }
                if (!expressions.empty()) {
                    addFilters();
                }
                if (!projectionFields.empty()) {
                    addProjections();
                }
            }
        }
    }
    /*
        }
        if (p->isIteration()) {
            if (p->getIterMax() != LLONG_MAX) {
                query.times(p->getIterMin(), p->getIterMax());
            } else if (p->getIterMin() == 1 && p->getIterMax() == LLONG_MAX) {
                query.times();
            } else if (p->getIterMin() == 0) {
                query.times(p->getIterMax());
            }
            p->setQuery(query);
        }
        std::advance(tmpIt, 1);
    }*/
    return query;
}

void NesCEPQueryPlanCreator::addFilters() {
    for (auto it = expressions.begin(); it != expressions.end(); ++it) {
        query.filter(*it);
    }
}

std::pair<TimeMeasure, TimeMeasure> NesCEPQueryPlanCreator::transformWindowToTimeMeasurements() {
    std::string timeMeasure = window.first;

    if (timeMeasure == "MILLISECOND") {
        TimeMeasure size = Minutes(window.second);
        TimeMeasure slide = Minutes(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "SECOND") {
        TimeMeasure size = Seconds(window.second);
        TimeMeasure slide = Seconds(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "MINUTE") {
        TimeMeasure size = Minutes(window.second);
        TimeMeasure slide = Minutes(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else if (timeMeasure == "HOUR") {
        TimeMeasure size = Hours(window.second);
        TimeMeasure slide = Hours(1);
        return std::pair<TimeMeasure, TimeMeasure>(size, slide);
    } else {
        NES_ERROR("NesCEPQueryPlanCreator: Unkown time measure " + timeMeasure)
    }

    return std::pair<TimeMeasure, TimeMeasure>(TimeMeasure(0), TimeMeasure(0));
}

void NesCEPQueryPlanCreator::addProjections() {
    for (auto it = projectionFields.begin(); it != projectionFields.end(); ++it) {
        query.project(*it);
    }
}

//Getter and Setter
const std::map<int, NePSLPatternEventNode*>& NesCEPQueryPlanCreator::GetOperatorList() const { return operatorList; }
void NesCEPQueryPlanCreator::SetOperatorList(const std::map<int, NePSLPatternEventNode*>& operator_list) {
    operatorList = operator_list;
}
const std::list<std::shared_ptr<NES::SinkDescriptor>>& NesCEPQueryPlanCreator::GetSinks() const { return sinks; }
void NesCEPQueryPlanCreator::SetSinks(const std::list<std::shared_ptr<NES::SinkDescriptor>>& sinks) {
    NesCEPQueryPlanCreator::sinks = sinks;
}
const std::pair<std::string, int>& NesCEPQueryPlanCreator::getWindow() const { return window; }
void NesCEPQueryPlanCreator::setWindow(const std::pair<std::string, int>& window) { NesCEPQueryPlanCreator::window = window; }
int NesCEPQueryPlanCreator::GetLastSeenSourcePtr() const { return lastSeenSourcePtr; }
void NesCEPQueryPlanCreator::SetLastSeenSourcePtr(int last_seen_source_ptr) { lastSeenSourcePtr = last_seen_source_ptr; }
int NesCEPQueryPlanCreator::GetSourceCounter() const { return sourceCounter; }
void NesCEPQueryPlanCreator::SetSourceCounter(int source_counter) { sourceCounter = source_counter; }
int NesCEPQueryPlanCreator::getNodeId() const { return nodeId; }
void NesCEPQueryPlanCreator::setNodeId(int node_id) { nodeId = node_id; }
int NesCEPQueryPlanCreator::getDirection() const { return direction; }
void NesCEPQueryPlanCreator::setDirection(int direction) { NesCEPQueryPlanCreator::direction = direction; }
int NesCEPQueryPlanCreator::GetCurrentElementPointer() const { return currentElementPointer; }
void NesCEPQueryPlanCreator::setCurrentElementPointer(int current_element_pointer) {
    currentElementPointer = current_element_pointer;
}
int NesCEPQueryPlanCreator::GetCurrentParentPointer() const { return currentOperatorPointer; }
void NesCEPQueryPlanCreator::SetCurrentParentPointer(int current_parent_pointer) {
    currentOperatorPointer = current_parent_pointer;
}
const std::map<int, NePSLPatternEventNode*>::iterator& NesCEPQueryPlanCreator::GetIt() const { return it; }
void NesCEPQueryPlanCreator::SetIt(const std::map<int, NePSLPatternEventNode*>::iterator& it) { NesCEPQueryPlanCreator::it = it; }
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
const std::map<int, std::string>& NesCEPQueryPlanCreator::getSources() { return sources; }
void NesCEPQueryPlanCreator::setSources(const std::map<int, std::string>& sources) { NesCEPQueryPlanCreator::sources = sources; }

}// end namespace NES