#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <iostream>
namespace NES {

QueryPlanPtr QueryPlan::create(std::string sourceStreamName, OperatorNodePtr rootOperator) {
    return std::make_shared<QueryPlan>(QueryPlan(sourceStreamName, rootOperator));
}

QueryPlan::QueryPlan(std::string sourceStreamName, OperatorNodePtr rootOperator)
    : rootOperator(rootOperator), currentOperatorId(0) {
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

void QueryPlan::appendOperator(OperatorNodePtr op) {
    op->setId(getNextOperatorId());
    rootOperator->addParent(op);
    rootOperator = op;
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

}// namespace NES