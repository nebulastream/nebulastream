#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Util/DumpContext.hpp>
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
        rootOperator->addParent(operatorNode);
    }
    rootOperators.clear();
    rootOperators.push_back(operatorNode);
}

void QueryPlan::prependPreExistingOperator(OperatorNodePtr operatorNode) {
    auto leafOperators = getLeafOperators();
    for (auto leafOperator : leafOperators) {
        leafOperator->addChild(operatorNode);
    }
}

std::string QueryPlan::toString() {
    std::stringstream ss;
    DumpContextPtr dumpContext;

    // FIXME: I am not sure what this will result in or if this is correct
    for (auto rootOperator : rootOperators) {
        dumpContext->dump(rootOperator, ss);
    }
    return ss.str();
}

std::vector<OperatorNodePtr> QueryPlan::getRootOperators() {
    return rootOperators;
}

std::vector<OperatorNodePtr> QueryPlan::getLeafOperators() {

    std::vector<OperatorNodePtr> leafOperators;
    std::set<u_int64_t> visitedOpIds;
    for (auto rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        auto itr = bfsIterator.begin();
        while (itr != bfsIterator.end()) {
            auto visitingOp = (*itr)->as<OperatorNode>();
            ++itr;
            if (visitedOpIds.find(visitingOp->getId()) != visitedOpIds.end()) {
                continue;
            }
            visitedOpIds.insert(visitingOp->getId());
            if (visitingOp->getChildren().empty()) {
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

    //FIXME: its a very inefficient way of doing it as we have to check the
    // traversed Operator list for each iterated operator and also store the
    // information about the operator in the vector.
    std::set<u_int64_t> visitedOpIds;
    for (auto rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        auto itr = bfsIterator.begin();
        while (itr != bfsIterator.end()) {
            auto visitingOp = (*itr)->as<OperatorNode>();
            ++itr;
            if (visitedOpIds.find(visitingOp->getId()) != visitedOpIds.end()) {
                continue;
            }
            visitedOpIds.insert(visitingOp->getId());
            if (operatorNode == visitingOp) {
                return true;
            }
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