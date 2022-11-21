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
#include <Parsers/NebulaPSL/NebulaPSLQueryPlanCreator.h>
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
    // add AST elements to this queryPlan
    createQueryFromPatternList();
    const std::vector<NES::OperatorNodePtr>& rootOperators = this->queryPlan->getRootOperators();
    // add the sinks to the query plan
    for (SinkDescriptorPtr sinkDescriptor : pattern.getSinks()) {
        auto sinkOperator = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
        for (auto& rootOperator : rootOperators) {
            sinkOperator->addChild(rootOperator);
            this->queryPlan->removeAsRootOperator(rootOperator);
            this->queryPlan->addRootOperator(sinkOperator);
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

void NesCEPQueryPlanCreator::createQueryFromPatternList() {
    NES_DEBUG("NesCEPQueryPlanCreator: createQueryFromPatternList: create query from AST elements")
    // if for simple patterns without binary CEP operators
    if (this->pattern.getOperatorList().empty() && this->pattern.getSources().size() == 1) {
        auto sourceName = pattern.getSources().at(0);
        this->queryPlan = QueryPlanBuilder::createQueryPlan(sourceName);

    // else for pattern with binary operators
    } else {
        // iterate over OperatorList, create and add LogicalOperatorNodes
        for (auto it = pattern.getOperatorList().begin(); it != pattern.getOperatorList().end(); ++it) {
            auto opName = it->second.getEventName();
            // add binary operators
            if (opName == "OR" || opName == "SEQ" || opName == "AND") {
                addBinaryOperatorToQueryPlan(opName, it);
            } else if (opName == "TIMES") { //add times operator
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: add unary operator " + opName)
                auto sourceName = pattern.getSources().at(it->second.getLeftChildId());
                this->queryPlan = QueryPlanBuilder::createQueryPlan(sourceName);
                NES_DEBUG("NesCEPQueryPlanCreater: createQueryFromPatternList: created subquery from "
                          + pattern.getSources().at(it->second.getLeftChildId()))

                this->queryPlan = QueryPlanBuilder::addMapNode(Attribute("Count") = 1, this->queryPlan);

                //create window
                auto window = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
                if (window.first.getTime() != TimeMeasure(0).getTime()) {
                    auto windowType =
                        Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), window.first, window.second);

                    //we use a on time trigger as default that triggers on each change of the watermark
                    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();

                    //TimeBasedwindowType
                    auto timeBasedWindow = WindowType::asTimeBasedWindowType(windowType);

                    // check if query contain watermark assigner, and add if missing (as default behaviour)
                    if (this->queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {

                        // we only consider Event time in CEP
                        this->queryPlan->appendOperatorAsNewRoot(
                            LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                                Attribute(timeBasedWindow->getTimeCharacteristic()->getField()->getName()),
                                API::Milliseconds(0),
                                timeBasedWindow->getTimeCharacteristic()->getTimeUnit())));
                    }

                    auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();

                    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

                    std::vector<WindowAggregationPtr> windowAggs;
                    std::shared_ptr<WindowAggregationDescriptor> windowAgg = API::Sum(Attribute("Count"));

                    auto timestamp = timeBasedWindow->getTimeCharacteristic()->getField()->getName();
                    std::shared_ptr<WindowAggregationDescriptor> windowTs = API::Max(Attribute(timestamp));
                    windowAggs.push_back(windowAgg);
                    windowAggs.push_back(windowTs);

                    auto windowDefinition = Windowing::LogicalWindowDefinition::create(windowAggs,
                                                                                       windowType,
                                                                                       distrType,
                                                                                       triggerPolicy,
                                                                                       triggerAction,
                                                                                       0);

                    OperatorNodePtr op = LogicalOperatorFactory::createWindowOperator(windowDefinition);
                    this->queryPlan->appendOperatorAsNewRoot(op);

                    int32_t min = it->second.getMinMax().first;
                    int32_t max = it->second.getMinMax().second;

                    if(min != 0 && max != 0) { //TODO min = 1 max = 0
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

                        this->queryPlan = QueryPlanBuilder::addFilterNode(predicate, this->queryPlan);
                    }
                } else{
                    NES_ERROR("The Iteration operator requires a time window.")
                }
            } else{
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
}

void NesCEPQueryPlanCreator::addFilters() {
    for (auto it = pattern.getExpressions().begin(); it != pattern.getExpressions().end(); ++it) {
        this->queryPlan = QueryPlanBuilder::addFilterNode(*it, this->queryPlan);
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
    this->queryPlan = QueryPlanBuilder::addProjectNode(pattern.getProjectionFields(),this->queryPlan);
   }

QueryPlanPtr NesCEPQueryPlanCreator::getQueryPlan() const { return this->queryPlan; }

std::string NesCEPQueryPlanCreator::keyAssignment(std::string keyName) {
    //first, get unique ids for the key attributes
    auto cepRightId = Util::getNextOperatorId();
    //second, create a unique name for both key attributes
    std::string cepRightKey = keyName + std::to_string(cepRightId);
    return cepRightKey;
}

void NesCEPQueryPlanCreator::addBinaryOperatorToQueryPlan(std::string opName, std::map<int, NebulaPSLOperatorNode>::const_iterator it) {
    NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: add binary operator " + opName)
    // find left (query) and right branch (subquery) of binary operator
    //left query plan
    NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryLeft from "
              + pattern.getSources().at(it->second.getLeftChildId()))
    auto leftSourceName = pattern.getSources().at(it->second.getLeftChildId());
    auto rightSourceName = pattern.getSources().at(it->second.getRightChildId());
    //check if and which source is already consumed
    auto rightQueryPlan = checkIfSourceIsAlreadyConsumedSource(leftSourceName,rightSourceName);

    if (opName == "OR") {
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: addUnionOperator")
        this->queryPlan = QueryPlanBuilder::addUnionOperatorNode(this->queryPlan, rightQueryPlan);
    } else {
        // Seq and And require a window, so first we create and check the window operator
        auto window = transformWindowToTimeMeasurements(pattern.getWindow().first, pattern.getWindow().second);
        if (window.first.getTime() != TimeMeasure(0).getTime()) {
            auto windowType =
                Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")),
                                             window.first,
                                             window.second);

            //Next, we add artificial key attributes to the sources in order to reuse the join-logic later
            std::string cepLeftKey = keyAssignment("cep_leftLeft");
            std::string cepRightKey = keyAssignment("cep_rightkey");

            //next: add Map operator that maps the attributes with value 1 to the left and right source
            this->queryPlan = QueryPlanBuilder::addMapNode(Attribute(cepLeftKey) = 1, this->queryPlan);
           rightQueryPlan = QueryPlanBuilder::addMapNode(Attribute(cepRightKey) = 1, rightQueryPlan);

            //then, define the artificial attributes as key attributes
            NES_DEBUG("Query: add name cepLeftKey " << cepLeftKey << "and name cepRightKey" << cepRightKey);
            ExpressionItem onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
            ExpressionItem onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
            auto leftKeyFieldAccess = onLeftKey.getExpressionNode()->as<FieldAccessExpressionNode>();
            auto rightKeyFieldAccess = onRightKey.getExpressionNode()->as<FieldAccessExpressionNode>();

            this->queryPlan = QueryPlanBuilder::addJoinOperatorNode(this->queryPlan, rightQueryPlan, onLeftKey, onRightKey, windowType, Join::LogicalJoinDefinition::CARTESIAN_PRODUCT);

            if (opName == "SEQ") {
                // for SEQ we need to add additional filter for order by time
                auto timeBasedWindow = WindowType::asTimeBasedWindowType(windowType);
                auto timestamp = timeBasedWindow->getTimeCharacteristic()->getField()->getName();
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
                auto sourceNameLeft = this->queryPlan->getSourceConsumed();
                // in case of composed sources on the left branch
                if (sourceNameLeft.find("_") != std::string::npos) {
                    // we find the most right source and use its timestamp for the filter constraint
                    uint64_t posStart = sourceNameLeft.find_last_of("_");
                    sourceNameLeft = sourceNameLeft.substr(posStart + 1) + "$" + timestamp;
                }// in case the left branch only contains 1 source we can just use it
                else {
                    sourceNameLeft = sourceNameLeft + "$" + timestamp;
                }
                NES_DEBUG("NesCEPQueryPlanCreater: ExpressionItem for Left Source " << sourceNameLeft << "and ExpressionItem for Right Source " << sourceNameRight);
                //create filter expression and add it to queryplan
                this->queryPlan = QueryPlanBuilder::addFilterNode(Attribute(sourceNameLeft) < Attribute(sourceNameRight), this->queryPlan);
            }
        } else {
            NES_ERROR("NesCEPQueryPlanCreater: createQueryFromPatternList: Cannot create " + opName
                      + "without a window.")
        }
    }

}
QueryPlanPtr NesCEPQueryPlanCreator::checkIfSourceIsAlreadyConsumedSource(std::basic_string<char> leftSourceName,
                                                                  std::basic_string<char> rightSourceName) {
    QueryPlanPtr rightQueryPlan = nullptr;
    if (this->queryPlan->getSourceConsumed().find(leftSourceName) != std::string::npos
        && this->queryPlan->getSourceConsumed().find(rightSourceName) != std::string::npos) {
        NES_ERROR("NesCEPQueryPlanCreater: Both sources are already consumed and combined with a binary operator")
    }else if (this->queryPlan->getSourceConsumed().find(leftSourceName) == std::string::npos
        && this->queryPlan->getSourceConsumed().find(rightSourceName) == std::string::npos) {
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: both sources are not in the current queryPlan")
        //make left source current queryPlan
        this->queryPlan = QueryPlanBuilder::createQueryPlan(leftSourceName);
        //return right source as right queryPlan
        return rightQueryPlan = QueryPlanBuilder::createQueryPlan(rightSourceName);
    }
    else if (this->queryPlan->getSourceConsumed().find(leftSourceName) == std::string::npos) {
        // right queryplan
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryRight from " + leftSourceName)
        return rightQueryPlan = QueryPlanBuilder::createQueryPlan(leftSourceName);
    } else if (this->queryPlan->getSourceConsumed().find(rightSourceName) == std::string::npos) {
        // right queryplan
        NES_DEBUG("NesCEPQueryPlanCreater: addBinaryOperatorToQueryPlan: create subqueryRight from " + rightSourceName)
        return rightQueryPlan = QueryPlanBuilder::createQueryPlan(rightSourceName);
    }
    return rightQueryPlan;
}

}// namespace NES::Parsers