#include <Nodes/Node.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <set>

namespace NES {

QueryPlanPtr QueryPlan::create(std::string sourceStreamName, OperatorNodePtr rootOperator) {
    return std::make_shared<QueryPlan>(QueryPlan(sourceStreamName, rootOperator));
}

QueryPlanPtr QueryPlan::create() {
    return std::make_shared<QueryPlan>(QueryPlan());
}

QueryPlan::QueryPlan(std::string sourceStreamName, OperatorNodePtr rootOperator) {
    currentOperatorId = 0;
    rootOperator->setId(getNextOperatorId());
    rootOperators.push_back(rootOperator);
    this->sourceStreamName = sourceStreamName;
}

QueryPlan::QueryPlan() {}

std::vector<SourceLogicalOperatorNodePtr> QueryPlan::getSourceOperators() {
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators;
    for (auto rootOperator : rootOperators) {
        auto sourceOptrs = rootOperator->getNodesByType<SourceLogicalOperatorNode>();
        sourceOperators.insert(sourceOperators.end(), sourceOptrs.begin(), sourceOptrs.end());
    }
    return sourceOperators;
}

std::vector<SinkLogicalOperatorNodePtr> QueryPlan::getSinkOperators() {
    std::vector<SinkLogicalOperatorNodePtr> sinkOperators;
    for (auto rootOperator : rootOperators) {
        auto sinkOptrs = rootOperator->getNodesByType<SinkLogicalOperatorNode>();
        sinkOperators.insert(sinkOperators.end(), sinkOptrs.begin(), sinkOptrs.end());
    }
    return sinkOperators;
}

size_t QueryPlan::getNextOperatorId() {
    return currentOperatorId++;
}

void QueryPlan::appendOperator(OperatorNodePtr operatorNode) {
    for (auto rootOperator : rootOperators) {
        operatorNode->setId(getNextOperatorId());
        rootOperator->addParent(operatorNode);
    }
    rootOperators.clear();
    rootOperators.push_back(operatorNode);
}

void QueryPlan::appendPreExistingOperator(OperatorNodePtr operatorNode) {
    for (auto rootOperator : rootOperators) {
        if (!rootOperator->addParent(operatorNode)) {
            NES_THROW_RUNTIME_ERROR("QueryPlan: Enable to add operator " + operatorNode->toString() + " as parent to " + rootOperator->toString());
        }
    }
    rootOperators.clear();
    rootOperators.push_back(operatorNode);
}

void QueryPlan::prependPreExistingOperator(OperatorNodePtr operatorNode) {
    auto leafOperators = getLeafOperators();
    if (leafOperators.empty()) {
        NES_DEBUG("QueryPlan: Found empty query plan. Adding operator as root.");
        rootOperators.push_back(operatorNode);
    } else {
        NES_DEBUG("QueryPlan: Adding operator as child to all the leaf nodes.");
        for (auto leafOperator : leafOperators) {
            leafOperator->addChild(operatorNode);
        }
    }
}

std::string QueryPlan::toString() {
    std::stringstream ss;
    auto dumpHandler = ConsoleDumpHandler::create();
    for (auto rootOperator : rootOperators) {
        dumpHandler->dump(rootOperator, ss);
    }
    return ss.str();
}

std::vector<OperatorNodePtr> QueryPlan::getRootOperators() {
    return rootOperators;
}

std::vector<OperatorNodePtr> QueryPlan::getLeafOperators() {

    NES_DEBUG("QueryPlan: Get all leaf nodes in the query plan.");
    std::vector<OperatorNodePtr> leafOperators;
    // Maintain a list of visited nodes as there are multiple root nodes
    std::set<u_int64_t> visitedOpIds;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (auto rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != bfsIterator.end(); ++itr) {
            auto visitingOp = (*itr)->as<OperatorNode>();
            if (visitedOpIds.find(visitingOp->getId()) != visitedOpIds.end()) {
                // skip rest of the steps as the node found in already visited node list
                continue;
            }
            visitedOpIds.insert(visitingOp->getId());
            if (visitingOp->getChildren().empty()) {
                NES_DEBUG("QueryPlan: Found leaf node. Adding to the collection of leaf nodes.");
                leafOperators.push_back(visitingOp);
            }
        }
    }
    return leafOperators;
}

const std::string QueryPlan::getSourceStreamName() const {
    return sourceStreamName;
}

bool QueryPlan::hasOperator(OperatorNodePtr operatorNode) {

    NES_DEBUG("QueryPlan: Checking if the operator exists in the query plan or not");
    if (operatorNode->getId() == SYS_SOURCE_OPERATOR_ID || operatorNode->getId() == SYS_SINK_OPERATOR_ID) {
        NES_DEBUG("QueryPlan: If the operator is a system generated one then we ignore this check");
        return false;
    }

    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator");
    for (auto rootOperator : rootOperators) {
        if(rootOperator->findRecursively(rootOperator, operatorNode)) {
            return true;
        }
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return false;
}// namespace NES

const std::string& QueryPlan::getQueryId() const {
    return queryId;
}

void QueryPlan::setQueryId(std::string& queryId) {
    QueryPlan::queryId = queryId;
}

}// namespace NES