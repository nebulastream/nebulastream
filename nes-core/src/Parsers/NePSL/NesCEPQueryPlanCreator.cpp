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
#include <../nes-core/include/Parsers/NePSL/NesCEPQueryPlanCreator.h>

int NesCEPQueryPlanCreator::getDirection() { return direction; }
void NesCEPQueryPlanCreator::setDirection(int direction) { this->direction = direction; }
int NesCEPQueryPlanCreator::getCurrentPointer() { return currentPointer; }
void NesCEPQueryPlanCreator::setCurrentPointer(int currentPointer) { this->currentPointer = currentPointer; }
int NesCEPQueryPlanCreator::getCurrentParent() { return currentParent; }
void NesCEPQueryPlanCreator::setCurrentParent(int currentParent) { this->currentParent = currentParent; }
int NesCEPQueryPlanCreator::getId() { return id; }
void NesCEPQueryPlanCreator::setId(int id) { this->id = id; }
const NES::Query& NesCEPQueryPlanCreator::getQuery() { return query; }

//-------------------------------------------------------------------------------------

void NesCEPQueryPlanCreator::enterListEvents(NesCEPParser::ListEventsContext* context) {
    NePSLPattern* pattern = new NePSLPattern(id);
    pattern->setParent(id);
    this->patterns.insert(this->it, pattern);
    this->currentPointer = id;
    this->currentParent = id;
    id++;
    std::advance(it, 1);
    NesCEPBaseListener::enterListEvents(context);
}

void NesCEPQueryPlanCreator::enterEventElem(NesCEPParser::EventElemContext* context) {
    NePSLPattern* pattern = new NePSLPattern(id);
    pattern->setParent(currentParent);
    this->patterns.push_back(pattern);
    this->currentPointer = id;
    id++;
    std::advance(it, 1);
    NesCEPBaseListener::enterEventElem(context);
}

void NesCEPQueryPlanCreator::exitInputStreams(NesCEPParser::InputStreamsContext* context) {
    std::list<NePSLPattern*> tmpPatterns = patterns;
    patterns.reverse();
    std::list<NePSLPattern*>::iterator tmpIt = patterns.begin();
    while (tmpIt != patterns.end()) {
        NePSLPattern* p = *tmpIt;
        std::string eventRight = p->getEventRight();
        std::string eventLeft = p->getEventLeft();
        std::string op = p->getOp();
        if (!eventRight.empty()) {
            query = NES::Query::from(eventRight);
            p->setQuery(query);
        }
        if (!eventLeft.empty()) {
            query = NES::Query::from(eventLeft);
            p->setQuery(query);
        }
        if (!op.empty()) {
            int rightID = p->getRight();
            int leftID = p->getLeft();
            std::list<NePSLPattern*>::iterator anotherIt = tmpPatterns.begin();
            std::advance(anotherIt, rightID);
            NES::Query right = (*anotherIt)->getQuery();
            anotherIt = tmpPatterns.begin();
            std::advance(anotherIt, leftID);
            NES::Query left = (*anotherIt)->getQuery();
            if (op == "OR") {
                right.orWith(left);
                query = right;
                p->setQuery(query);
            } else if (op == "SEQ") {
                right.seqWith(left);
                query = right;
                p->setQuery(query);
            } else if (op == "AND") {
                right.andWith(left);
                query = right;
                p->setQuery(query);
            } else {
                //No contiguity supported yet: future work
            }
        }
        if (p->isIteration()) {
            query.times(p->getIterMin(), p->getIterMax());
            p->setQuery(query);
        }
        std::advance(tmpIt, 1);
    }
    NesCEPBaseListener::exitInputStreams(context);
}

void NesCEPQueryPlanCreator::exitInputStream(NesCEPParser::InputStreamContext* context) {
    std::string realName = context->getStart()->getText();
    std::string alias = context->getStop()->getText();
    std::list<NePSLPattern*>::iterator tmpIt = patterns.begin();
    while (tmpIt != patterns.end()) {
        NePSLPattern* p = *tmpIt;
        if (p->getEventRight() == alias) {
            p->setEventRight(realName);
        }
        if (p->getEventLeft() == alias) {
            p->setEventLeft(realName);
        }
        std::advance(tmpIt, 1);
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

void NesCEPQueryPlanCreator::enterOutAttribute(NesCEPParser::OutAttributeContext* context) {
    query.map(NES::Attribute(context->NAME()->getText()) = context->attVal()->getText());
}

void NesCEPQueryPlanCreator::exitSinkList(NesCEPParser::SinkListContext* context) {
    const std::vector<NES::OperatorNodePtr>& rootOperators = query.getQueryPlan()->getRootOperators();

    for (std::shared_ptr<NES::SinkDescriptor> tmpItr : sinks) {
        std::shared_ptr<NES::SinkDescriptor> sink = tmpItr;
        query.multipleSink(sink, rootOperators);
    }
    NesCEPBaseListener::exitSinkList(context);
}

void NesCEPQueryPlanCreator::enterSink(NesCEPParser::SinkContext* context) {
    NES::Query tmpQuery = query;
    std::string sinkType = context->sinkType()->getText();
    std::shared_ptr<NES::SinkDescriptor> sinkDescriptorPtr;
    if (sinkType == "Print") {
        sinkDescriptorPtr = NES::PrintSinkDescriptor::create();
    }
    if (sinkType == "Kafka") {
        //TO-DO
        sinkDescriptorPtr = NES::NullOutputSinkDescriptor::create();
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
    if (sinkType == "OPC") {
        //TODO
        sinkDescriptorPtr = NES::NullOutputSinkDescriptor::create();
    }
    if (sinkType == "ZMQ") {
        //TODO
        sinkDescriptorPtr = NES::NullOutputSinkDescriptor::create();
    }
    sinks.push_back(sinkDescriptorPtr);
    NesCEPBaseListener::enterSink(context);
}

void NesCEPQueryPlanCreator::exitEventElem(NesCEPParser::EventElemContext* context) {
    std::list<NePSLPattern*>::iterator tmpIt = patterns.begin();
    std::advance(tmpIt, currentParent);
    NePSLPattern* p = *tmpIt;
    if (direction == -1) {
        p->setRight(currentPointer);
    }
    if (direction == 1) {
        p->setLeft(currentPointer);
        setDirection(-1);
    }

    int oldCurrentParent = currentParent;
    setCurrentParent(p->getParent());
    setCurrentPointer(oldCurrentParent);
    NesCEPBaseListener::exitEventElem(context);
}

void NesCEPQueryPlanCreator::enterEvent(NesCEPParser::EventContext* context) {
    std::list<NePSLPattern*>::iterator tmpIt = patterns.begin();
    std::advance(tmpIt, currentPointer);
    NePSLPattern* event = *tmpIt;
    if (this->direction == -1) {
        event->setEventRight(context->getStart()->getText());
    } else if (this->direction == 1) {
        event->setEventLeft(context->getStart()->getText());
    }
}

void NesCEPQueryPlanCreator::enterQuantifiers(NesCEPParser::QuantifiersContext* context) {
    std::list<NePSLPattern*>::iterator tmpIt = patterns.begin();
    std::advance(tmpIt, currentPointer);
    NePSLPattern* event = *tmpIt;
    event->setIteration(true);
    if (context->STAR()) {
        event->setIterMin(0);
        event->setIterMax(LLONG_MAX);
    } else if (context->PLUS() && !context->LBRACKET()) {
        event->setIterMin(1);
        event->setIterMax(LLONG_MAX);
    } else if (context->D_POINTS()) {
        //Consecutive options not yet supported
        event->setIterMin(stoi(context->iterMin()->INT()->getText()));
        event->setIterMax(stoi(context->iterMax()->INT()->getText()));
    } else if (context->PLUS() && context->LBRACKET()) {
        event->setIterMin(stoi(context->INT()->getText()));
        event->setIterMax(LLONG_MAX);
    } else {
        event->setIterMin(stoi(context->INT()->getText()));
        event->setIterMax(stoi(context->INT()->getText()));
    }
    NesCEPBaseListener::enterQuantifiers(context);
}

void NesCEPQueryPlanCreator::enterOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    setDirection(0);
    std::list<NePSLPattern*>::iterator tmpIt = patterns.begin();
    std::advance(tmpIt, currentParent);
    NePSLPattern* p = *tmpIt;
    p->setOp(context->getText());
}

void NesCEPQueryPlanCreator::exitOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    setDirection(1);
    NesCEPBaseListener::exitOperatorRule(context);
}

void NesCEPQueryPlanCreator::exitBinaryComparasionPredicate(NesCEPParser::BinaryComparasionPredicateContext* context) {
    std::string comparisonOp = context->comparisonOperator()->getText();
    if (comparisonOp == "<") {
        auto lessExpression = NES::LessExpressionNode::create(NES::Attribute(currentLeftExp).getExpressionNode(),
                                                              NES::Attribute(currentRightExp).getExpressionNode());
        query.filter(lessExpression);
    }
}

void NesCEPQueryPlanCreator::enterAttribute(NesCEPParser::AttributeContext* context) {
    if (inWhere) {
        if (leftFilter)
            currentLeftExp = context->getText();
        else
            currentRightExp = context->getText();
    }
}