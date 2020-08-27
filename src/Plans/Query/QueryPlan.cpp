#include <Nodes/Node.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <set>
#include <utility>

namespace NES {

QueryPlanPtr QueryPlan::create(OperatorNodePtr rootOperator) {
    return std::make_shared<QueryPlan>(QueryPlan(std::move(rootOperator)));
}

QueryPlanPtr QueryPlan::create() {
    return std::make_shared<QueryPlan>(QueryPlan());
}

QueryPlan::QueryPlan() : queryId(INVALID_QUERY_ID) {}

QueryPlan::QueryPlan(OperatorNodePtr rootOperator) : queryId(INVALID_QUERY_ID) {
    rootOperator->setId(UtilityFunctions::getNextOperatorId());
    rootOperators.push_back(std::move(rootOperator));
}

std::vector<SourceLogicalOperatorNodePtr> QueryPlan::getSourceOperators() {
    NES_DEBUG("QueryPlan: Get all source operators by traversing all the root nodes.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators;
    for (const auto& rootOperator : rootOperators) {
        auto sourceOptrs = rootOperator->getNodesByType<SourceLogicalOperatorNode>();
        NES_DEBUG("QueryPlan: insert all source operators to the collection");
        sourceOperators.insert(sourceOperators.end(), sourceOptrs.begin(), sourceOptrs.end());
    }
    NES_DEBUG("QueryPlan: Found " << sourceOperators.size() << " source operators.");
    return sourceOperators;
}

std::vector<SinkLogicalOperatorNodePtr> QueryPlan::getSinkOperators() {
    NES_DEBUG("QueryPlan: Get all sink operators by traversing all the root nodes.");
    std::vector<SinkLogicalOperatorNodePtr> sinkOperators;
    for (const auto& rootOperator : rootOperators) {
        auto sinkOptrs = rootOperator->getNodesByType<SinkLogicalOperatorNode>();
        NES_DEBUG("QueryPlan: insert all sink operators to the collection");
        sinkOperators.insert(sinkOperators.end(), sinkOptrs.begin(), sinkOptrs.end());
    }
    NES_DEBUG("QueryPlan: Found " << sinkOperators.size() << " sink operators.");
    return sinkOperators;
}

void QueryPlan::appendOperator(OperatorNodePtr operatorNode) {
    NES_DEBUG("QueryPlan: Appending operator " << operatorNode->toString() << " as new root of the plan.");
    for (auto rootOperator : rootOperators) {
        operatorNode->setId(UtilityFunctions::getNextOperatorId());
        rootOperator->addParent(operatorNode);
    }
    NES_DEBUG("QueryPlan: Clearing current root operators.");
    rootOperators.clear();
    NES_DEBUG("QueryPlan: Pushing input operator node as new root.");
    rootOperators.push_back(operatorNode);
}

void QueryPlan::appendPreExistingOperator(OperatorNodePtr operatorNode) {
    NES_DEBUG("QueryPlan: Appending pre-existing operator " << operatorNode->toString() << " as new root of the plan.");
    for (auto rootOperator : rootOperators) {
        if (!rootOperator->addParent(operatorNode)) {
            NES_THROW_RUNTIME_ERROR("QueryPlan: Enable to add operator " + operatorNode->toString() + " as parent to " + rootOperator->toString());
        }
    }
    NES_DEBUG("QueryPlan: Clearing current root operators.");
    rootOperators.clear();
    NES_DEBUG("QueryPlan: Pushing input operator node as new root.");
    rootOperators.push_back(operatorNode);
}

void QueryPlan::prependPreExistingOperator(OperatorNodePtr operatorNode) {
    NES_DEBUG("QueryPlan: Prepending operator " << operatorNode->toString() << " as new leaf of the plan.");
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
    NES_DEBUG("QueryPlan: Get query plan as string.");
    std::stringstream ss;
    auto dumpHandler = ConsoleDumpHandler::create();
    for (auto rootOperator : rootOperators) {
        dumpHandler->dump(rootOperator, ss);
    }
    return ss.str();
}

std::vector<OperatorNodePtr> QueryPlan::getRootOperators() {
    NES_DEBUG("QueryPlan: Get all root operators.");
    return rootOperators;
}

std::vector<OperatorNodePtr> QueryPlan::getLeafOperators() {
    // Find all the leaf nodes in the query plan
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
            NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
            visitedOpIds.insert(visitingOp->getId());
            if (visitingOp->getChildren().empty()) {
                NES_DEBUG("QueryPlan: Found leaf node. Adding to the collection of leaf nodes.");
                leafOperators.push_back(visitingOp);
            }
        }
    }
    return leafOperators;
}

bool QueryPlan::hasOperator(OperatorNodePtr operatorNode) {

    NES_DEBUG("QueryPlan: Checking if the operator exists in the query plan or not");
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator");
    for (auto rootOperator : rootOperators) {
        if (rootOperator->findRecursively(rootOperator, operatorNode)) {
            NES_DEBUG("QueryPlan: Found operator " << operatorNode->toString() << " in the query plan");
            return true;
        }
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return false;
}

const QueryId QueryPlan::getQueryId() const {
    return queryId;
}

void QueryPlan::setQueryId(QueryId queryId) {
    QueryPlan::queryId = queryId;
}

void QueryPlan::addRootOperator(std::shared_ptr<OperatorNode> root) {
    rootOperators.push_back(root);
}
}// namespace NES