#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <iostream>

namespace NES {

QueryPlanPtr QueryPlan::create(std::string sourceStreamName, OperatorNodePtr rootOperator) {
    return std::make_shared<QueryPlan>(QueryPlan(sourceStreamName, rootOperator));
}

QueryPlanPtr QueryPlan::create() {
    return std::make_shared<QueryPlan>(QueryPlan());
}

QueryPlan::QueryPlan(std::string sourceStreamName, OperatorNodePtr rootOperator)
    : rootOperator(rootOperator), currentOperatorId(0) {
    bfsIterator = std::make_shared<BreadthFirstNodeIterator>(rootOperator);
    int operatorId = getNextOperatorId();
    rootOperator->setId(operatorId);
    this->sourceStreamName = sourceStreamName;
}

std::vector<SourceLogicalOperatorNodePtr> QueryPlan::getSourceOperators() {
    return rootOperator->getNodesByType<SourceLogicalOperatorNode>();
}

std::vector<SinkLogicalOperatorNodePtr> QueryPlan::getSinkOperators() {
    return rootOperator->getNodesByType<SinkLogicalOperatorNode>();
}

size_t QueryPlan::getNextOperatorId() {
    return currentOperatorId++;
}

void QueryPlan::appendOperator(OperatorNodePtr operatorNode) {
    operatorNode->setId(getNextOperatorId());
    rootOperator->addParent(operatorNode);
    rootOperator = operatorNode;
}

std::string QueryPlan::toString() {
    std::stringstream ss;
    DumpContextPtr dumpContext;
    dumpContext->dump(rootOperator, ss);
    return ss.str();
}

OperatorNodePtr QueryPlan::getRootOperator() const {
    return rootOperator;
}

const std::string QueryPlan::getSourceStreamName() const {
    return sourceStreamName;
}

bool QueryPlan::hasOperator(OperatorNodePtr operatorNode) {
    auto itr = bfsIterator->begin();
    while (itr != bfsIterator->end()){
        if(operatorNode == (*itr)) {
            return true;
        }
    }
    return false;
}

const std::string& QueryPlan::getQueryId() const {
    return queryId;
}

void QueryPlan::setQueryId(std::string& queryId) {
    QueryPlan::queryId = queryId;
}

}// namespace NES