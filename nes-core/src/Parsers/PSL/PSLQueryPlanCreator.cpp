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
#include <Parsers/PSL/PSLQueryPlanCreator.hpp>

namespace NES::Parsers {

int PSLQueryPlanCreator::getDirection() { return direction; }
void PSLQueryPlanCreator::setDirection(int direction) { this->direction = direction; }
int PSLQueryPlanCreator::getCurrentPointer() { return currentPointer; }
void PSLQueryPlanCreator::setCurrentPointer(int currentPointer) { this->currentPointer = currentPointer; }
int PSLQueryPlanCreator::getCurrentParent() { return currentParent; }
void PSLQueryPlanCreator::setCurrentParent(int currentParent) { this->currentParent = currentParent; }
int PSLQueryPlanCreator::getId() { return id; }
void PSLQueryPlanCreator::setId(int id) { this->id = id; }
const NES::Query& PSLQueryPlanCreator::getQuery() { return query; }

//-------------------------------------------------------------------------------------
/**
     * @brief creates a subPattern
     * @param context
    */
void PSLQueryPlanCreator::enterListEvents(NesCEPParser::ListEventsContext* context) {
    PSLPattern* pattern = new PSLPattern(id);
    pattern->setParent(id);
    this->patterns.insert(this->it, pattern);
    this->currentPointer = id;
    this->currentParent = id;
    id++;
    std::advance(it, 1);
    NesCEPBaseListener::enterListEvents(context);
}

/**
     * @brief creates a subPattern
     * @param context
    */
void PSLQueryPlanCreator::enterEventElem(NesCEPParser::EventElemContext* context) {
    PSLPattern* pattern = new PSLPattern(id);
    pattern->setParent(currentParent);
    this->patterns.push_back(pattern);
    this->currentPointer = id;
    id++;
    std::advance(it, 1);
    NesCEPBaseListener::enterEventElem(context);
}

/**
     * @brief append the sources and the operators that connect them to the query plan
     * @param context
    */
void PSLQueryPlanCreator::exitInputStreams(NesCEPParser::InputStreamsContext* context) {
    std::list<PSLPattern*> tmpPatterns = patterns;
    patterns.reverse();
    std::list<PSLPattern*>::iterator tmpIt = patterns.begin();
    while (tmpIt != patterns.end()) {
        PSLPattern* p = *tmpIt;
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
            std::list<PSLPattern*>::iterator anotherIt = tmpPatterns.begin();
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

/**
     * @brief substitues the input stream alias by its realname in every subPattern
     * @param context
    */
void PSLQueryPlanCreator::exitInputStream(NesCEPParser::InputStreamContext* context) {
    std::string realName = context->getStart()->getText();
    std::string alias = context->getStop()->getText();
    std::list<PSLPattern*>::iterator tmpIt = patterns.begin();
    while (tmpIt != patterns.end()) {
        PSLPattern* p = *tmpIt;
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

/**
     * @brief marks that the walker is in the WHERE clause
     * @param context
    */
void PSLQueryPlanCreator::enterWhereExp(NesCEPParser::WhereExpContext* context) {
    inWhere = true;
    NesCEPBaseListener::enterWhereExp(context);
}

/**
     * @brief marks that the walker is no more in the WHERE clause
     * @param context
    */
void PSLQueryPlanCreator::exitWhereExp(NesCEPParser::WhereExpContext* context) {
    inWhere = false;
    NesCEPBaseListener::exitWhereExp(context);
}

/**
     * @brief append a map operator to the query plan
     * @param context
    */
void PSLQueryPlanCreator::enterOutAttribute(NesCEPParser::OutAttributeContext* context) {
    query.map(NES::Attribute(context->NAME()->getText()) = context->attVal()->getText());
}

/**
     * @brief append the sink operators to the query plan
     * @param context
    */
void PSLQueryPlanCreator::exitSinkList(NesCEPParser::SinkListContext* context) {
    const std::vector<NES::OperatorNodePtr>& rootOperators = query.getQueryPlan()->getRootOperators();

    for (std::shared_ptr<NES::SinkDescriptor> tmpItr : sinks) {
        std::shared_ptr<NES::SinkDescriptor> sink = tmpItr;
        query.multipleSink(sink, rootOperators);
    }
    NesCEPBaseListener::exitSinkList(context);
}

/**
     * @brief add a sink operator to the sink list
     * @param context
    */
void PSLQueryPlanCreator::enterSink(NesCEPParser::SinkContext* context) {
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
        //TO-DO
        sinkDescriptorPtr = NES::NullOutputSinkDescriptor::create();
    }
    if (sinkType == "ZMQ") {
        //TO-DO
        sinkDescriptorPtr = NES::NullOutputSinkDescriptor::create();
    }
    sinks.push_back(sinkDescriptorPtr);
    NesCEPBaseListener::enterSink(context);
}

/**
     * @brief mark currentPointer subPattern as a child
     *        of the currentParent subPattern and move one step up in the hierarchy
     * @param context
    */
void PSLQueryPlanCreator::exitEventElem(NesCEPParser::EventElemContext* context) {
    std::list<PSLPattern*>::iterator tmpIt = patterns.begin();
    std::advance(tmpIt, currentParent);
    PSLPattern* p = *tmpIt;
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

/**
     * @brief mark the position of the event inside of the currentPointer subPattern
     * @param context
    */
void PSLQueryPlanCreator::enterEvent(NesCEPParser::EventContext* context) {
    std::list<PSLPattern*>::iterator tmpIt = patterns.begin();
    std::advance(tmpIt, currentPointer);
    PSLPattern* event = *tmpIt;
    if (this->direction == -1) {
        event->setEventRight(context->getStart()->getText());
    } else if (this->direction == 1) {
        event->setEventLeft(context->getStart()->getText());
    }
}

/**
     * @brief add the appropriate iteration to the currentPointer subPattern
     * @param context
    */
void PSLQueryPlanCreator::enterQuantifiers(NesCEPParser::QuantifiersContext* context) {
    std::list<PSLPattern*>::iterator tmpIt = patterns.begin();
    std::advance(tmpIt, currentPointer);
    PSLPattern* event = *tmpIt;
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

/**
     * @brief add the appropriate operator to the currentParent subPattern
     * @param context
    */
void PSLQueryPlanCreator::enterOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    setDirection(0);
    std::list<PSLPattern*>::iterator tmpIt = patterns.begin();
    std::advance(tmpIt, currentParent);
    PSLPattern* p = *tmpIt;
    p->setOp(context->getText());
}

/**
     * @brief change direction to right
     * @param context
    */
void PSLQueryPlanCreator::exitOperatorRule(NesCEPParser::OperatorRuleContext* context) {
    setDirection(1);
    NesCEPBaseListener::exitOperatorRule(context);
}

/**
     * @brief add a "<" filter to the query plan when needed
     * @param context
    */
void PSLQueryPlanCreator::exitBinaryComparasionPredicate(NesCEPParser::BinaryComparasionPredicateContext* context) {
    std::string comparaisonOp = context->comparisonOperator()->getText();
    if (comparaisonOp == "<") {
        auto lessExpression = NES::LessExpressionNode::create(NES::Attribute(currentLeftExp).getExpressionNode(),
                                                              NES::Attribute(currentRightExp).getExpressionNode());
        query.filter(lessExpression);
    }
}

/**
     * @brief if walker is inside of WHERE clause mark current position of the Attribute
     * @param context
    */
void PSLQueryPlanCreator::enterAttribute(NesCEPParser::AttributeContext* context) {
    if (inWhere) {
        if (leftFilter)
            currentLeftExp = context->getText();
        else
            currentRightExp = context->getText();
    }
}
}// namespace NES::Parsers