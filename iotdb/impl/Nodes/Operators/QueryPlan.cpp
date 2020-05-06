#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <iostream>
namespace NES {

QueryPlanPtr QueryPlan::create(OperatorNodePtr rootOperator, StreamPtr stream) {
    return std::make_shared<QueryPlan>(QueryPlan(rootOperator, stream));
}

QueryPlan::QueryPlan(OperatorNodePtr rootOperator, StreamPtr sourceStream)
    : rootOperator(rootOperator), sourceStream(sourceStream) {
    int operatorId = getNextOperatorId();
    rootOperator->setId(operatorId);
}

const StreamPtr QueryPlan::getSourceStream() const { return sourceStream; }

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

}// namespace NES