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

QueryPlan::QueryPlan() : queryId(INVALID_QUERY_ID), querySubPlanId(INVALID_QUERY_SUB_PLAN_ID) {}

QueryPlan::QueryPlan(OperatorNodePtr rootOperator) : queryId(INVALID_QUERY_ID), querySubPlanId(INVALID_QUERY_SUB_PLAN_ID) {
    if (rootOperator->getId() == 0) {
        rootOperator->setId(UtilityFunctions::getNextOperatorId());
    }
    rootOperators.push_back(std::move(rootOperator));
}

std::vector<SourceLogicalOperatorNodePtr> QueryPlan::getSourceOperators() {
    NES_DEBUG("QueryPlan: Get all source operators by traversing all the root nodes.");
    std::set<SourceLogicalOperatorNodePtr> sourceOperatorsSet;
    for (const auto& rootOperator : rootOperators) {
        auto sourceOptrs = rootOperator->getNodesByType<SourceLogicalOperatorNode>();
        NES_DEBUG("QueryPlan: insert all source operators to the collection");
        sourceOperatorsSet.insert(sourceOptrs.begin(), sourceOptrs.end());
    }
    NES_DEBUG("QueryPlan: Found " << sourceOperatorsSet.size() << " source operators.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators{sourceOperatorsSet.begin(), sourceOperatorsSet.end()};
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

void QueryPlan::appendOperatorAsNewRoot(OperatorNodePtr operatorNode) {
    NES_DEBUG("QueryPlan: Appending operator " << operatorNode->toString() << " as new root of the plan.");
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

void QueryPlan::prependOperatorAsLeafNode(OperatorNodePtr operatorNode) {
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

bool QueryPlan::hasOperatorWithId(uint64_t operatorId) {

    NES_DEBUG("QueryPlan: Checking if the operator exists in the query plan or not");
    for (auto rootOperator : rootOperators) {
        if (rootOperator->getId() == operatorId) {
            NES_DEBUG("QueryPlan: Found operator " << operatorId << " in the query plan");
            return true;
        } else {
            for (auto& child : rootOperator->getChildren()) {
                if (child->as<OperatorNode>()->getChildWithOperatorId(operatorId)) {
                    return true;
                }
            }
        }
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return false;
}

OperatorNodePtr QueryPlan::getOperatorWithId(uint64_t operatorId) {
    NES_DEBUG("QueryPlan: Checking if the operator exists in the query plan or not");
    for (auto rootOperator : rootOperators) {
        if (rootOperator->getId() == operatorId) {
            NES_DEBUG("QueryPlan: Found operator " << operatorId << " in the query plan");
            return rootOperator;
        } else {
            for (auto& child : rootOperator->getChildren()) {
                NodePtr found = child->as<OperatorNode>()->getChildWithOperatorId(operatorId);
                if (found) {
                    return found->as<OperatorNode>();
                }
            }
        }
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return nullptr;
}

const QueryId QueryPlan::getQueryId() const {
    return queryId;
}

void QueryPlan::setQueryId(QueryId queryId) {
    QueryPlan::queryId = queryId;
}

void QueryPlan::addRootOperator(OperatorNodePtr root) {
    rootOperators.push_back(root);
}

QuerySubPlanId QueryPlan::getQuerySubPlanId() {
    return querySubPlanId;
}

void QueryPlan::setQuerySubPlanId(uint64_t querySubPlanId) {
    this->querySubPlanId = querySubPlanId;
}

void QueryPlan::removeAsRootOperator(OperatorNodePtr root) {

    NES_DEBUG("QueryPlan: removing operator "<< root->toString() << " as root operator.");

    auto found = std::find_if(rootOperators.begin(), rootOperators.end(), [&](OperatorNodePtr rootOperator){
        return rootOperator->getId() == root->getId();
    });

    if(found != rootOperators.end()){
        NES_TRACE("QueryPlan: Found root operator "<< root->toString() << " in the root operator list. Removing the operator as the root of the query plan.");
        rootOperators.erase(found);
    }
}
}// namespace NES