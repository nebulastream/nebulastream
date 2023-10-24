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

#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <algorithm>
#include <set>
#include <stack>
#include <utility>

namespace NES {

QueryPlanPtr QueryPlan::create(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<OperatorNodePtr> rootOperators) {
    return std::make_shared<QueryPlan>(QueryPlan(queryId, querySubPlanId, std::move(rootOperators)));
}

QueryPlanPtr QueryPlan::create(QueryId queryId, QuerySubPlanId querySubPlanId) {
    return std::make_shared<QueryPlan>(QueryPlan(queryId, querySubPlanId));
}

QueryPlanPtr QueryPlan::create(OperatorNodePtr rootOperator) {
    return std::make_shared<QueryPlan>(QueryPlan(std::move(rootOperator)));
}

QueryPlanPtr QueryPlan::create() { return std::make_shared<QueryPlan>(QueryPlan()); }

QueryPlan::QueryPlan() : queryId(INVALID_QUERY_ID), querySubPlanId(INVALID_QUERY_SUB_PLAN_ID) {}

QueryPlan::QueryPlan(OperatorNodePtr rootOperator) : queryId(INVALID_QUERY_ID), querySubPlanId(INVALID_QUERY_SUB_PLAN_ID) {
    rootOperators.push_back(std::move(rootOperator));
}

QueryPlan::QueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<OperatorNodePtr> rootOperators)
    : rootOperators(std::move(rootOperators)), queryId(queryId), querySubPlanId(querySubPlanId) {}

QueryPlan::QueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId) : queryId(queryId), querySubPlanId(querySubPlanId) {}

std::vector<SourceLogicalOperatorNodePtr> QueryPlan::getSourceOperators() {
    NES_DEBUG("QueryPlan: Get all source operators by traversing all the root nodes.");
    std::set<SourceLogicalOperatorNodePtr> sourceOperatorsSet;
    for (const auto& rootOperator : rootOperators) {
        auto sourceOptrs = rootOperator->getNodesByType<SourceLogicalOperatorNode>();
        NES_DEBUG("QueryPlan: insert all source operators to the collection");
        sourceOperatorsSet.insert(sourceOptrs.begin(), sourceOptrs.end());
    }
    NES_DEBUG("QueryPlan: Found {} source operators.", sourceOperatorsSet.size());
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators{sourceOperatorsSet.begin(), sourceOperatorsSet.end()};
    return sourceOperators;
}

std::vector<SinkLogicalOperatorNodePtr> QueryPlan::getSinkOperators() {
    NES_DEBUG("QueryPlan: Get all sink operators by traversing all the root nodes.");
    std::vector<SinkLogicalOperatorNodePtr> sinkOperators;
    for (const auto& rootOperator : rootOperators) {
        auto sinkOperator = rootOperator->as<SinkLogicalOperatorNode>();
        sinkOperators.emplace_back(sinkOperator);
    }
    NES_DEBUG("QueryPlan: Found {} sink operators.", sinkOperators.size());
    return sinkOperators;
}

void QueryPlan::appendOperatorAsNewRoot(const OperatorNodePtr& operatorNode) {
    NES_DEBUG("QueryPlan: Appending operator {} as new root of the plan.", operatorNode->toString());
    for (const auto& rootOperator : rootOperators) {
        if (!rootOperator->addParent(operatorNode)) {
            NES_THROW_RUNTIME_ERROR("QueryPlan: Unable to add operator " + operatorNode->toString() + " as parent to "
                                    + rootOperator->toString());
        }
    }
    NES_DEBUG("QueryPlan: Clearing current root operators.");
    rootOperators.clear();
    NES_DEBUG("QueryPlan: Pushing input operator node as new root.");
    rootOperators.push_back(operatorNode);
}

void QueryPlan::prependOperatorAsLeafNode(const OperatorNodePtr& operatorNode) {
    NES_DEBUG("QueryPlan: Prepending operator {} as new leaf of the plan.", operatorNode->toString());
    auto leafOperators = getLeafOperators();
    if (leafOperators.empty()) {
        NES_DEBUG("QueryPlan: Found empty query plan. Adding operator as root.");
        rootOperators.push_back(operatorNode);
    } else {
        NES_DEBUG("QueryPlan: Adding operator as child to all the leaf nodes.");
        for (const auto& leafOperator : leafOperators) {
            leafOperator->addChild(operatorNode);
        }
    }
}

std::string QueryPlan::toString() {
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler::create(ss);
    for (const auto& rootOperator : rootOperators) {
        dumpHandler->dump(rootOperator);
    }
    return ss.str();
}

std::vector<OperatorNodePtr> QueryPlan::getRootOperators() { return rootOperators; }

std::vector<OperatorNodePtr> QueryPlan::getLeafOperators() {
    // Find all the leaf nodes in the query plan
    NES_DEBUG("QueryPlan: Get all leaf nodes in the query plan.");
    std::vector<OperatorNodePtr> leafOperators;
    // Maintain a list of visited nodes as there are multiple root nodes
    std::set<uint64_t> visitedOpIds;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
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
    for (const auto& rootOperator : rootOperators) {
        if (rootOperator->getId() == operatorId) {
            NES_DEBUG("QueryPlan: Found operator {} in the query plan", queryId);
            return true;
        }
        for (const auto& child : rootOperator->getChildren()) {
            if (child->as<OperatorNode>()->getChildWithOperatorId(operatorId)) {
                return true;
            }
        }
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return false;
}

OperatorNodePtr QueryPlan::getOperatorWithId(uint64_t operatorId) {
    NES_DEBUG("QueryPlan: Checking if the operator with id {} exists in the query plan or not", operatorId);
    for (auto rootOperator : rootOperators) {
        if (rootOperator->getId() == operatorId) {
            NES_DEBUG("QueryPlan: Found operator {} in the query plan", operatorId);
            return rootOperator;
        }
        for (const auto& child : rootOperator->getChildren()) {
            NES_TRACE("QueryPlan: Searching for {} in the children", operatorId);
            NodePtr found = child->as<OperatorNode>()->getChildWithOperatorId(operatorId);
            if (found) {
                return found->as<OperatorNode>();
            }
        }
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return nullptr;
}

QueryId QueryPlan::getQueryId() const { return queryId; }

void QueryPlan::setQueryId(QueryId queryId) { QueryPlan::queryId = queryId; }

void QueryPlan::addRootOperator(const OperatorNodePtr& root) { rootOperators.push_back(root); }

QuerySubPlanId QueryPlan::getQuerySubPlanId() const { return querySubPlanId; }

void QueryPlan::setQuerySubPlanId(uint64_t querySubPlanId) { this->querySubPlanId = querySubPlanId; }

void QueryPlan::removeAsRootOperator(OperatorNodePtr root) {
    NES_DEBUG("QueryPlan: removing operator {} as root operator.", root->toString());
    auto found = std::find_if(rootOperators.begin(), rootOperators.end(), [&](const OperatorNodePtr& rootOperator) {
        return rootOperator->getId() == root->getId();
    });
    if (found != rootOperators.end()) {
        NES_TRACE(
            "QueryPlan: Found root operator in the root operator list. Removing the operator as the root of the query plan.",
            root->toString());
        rootOperators.erase(found);
    }
}

bool QueryPlan::replaceRootOperator(const OperatorNodePtr& oldRoot, const OperatorNodePtr& newRoot) {
    for (auto& rootOperator : rootOperators) {
        // compares the pointers and checks if we found the correct operator.
        if (rootOperator == oldRoot) {
            rootOperator = newRoot;
            return true;
        }
    }
    return false;
}

bool QueryPlan::replaceOperator(const OperatorNodePtr& oldOperator, const OperatorNodePtr& newOperator) {
    replaceRootOperator(oldOperator, newOperator);
    oldOperator->replace(newOperator);
    return true;
}

QueryPlanPtr QueryPlan::copy() {
    NES_INFO("QueryPlan: make copy of this query plan");
    // 1. We start by copying the root operators of this query plan to the queue of operators to be processed
    std::map<uint64_t, OperatorNodePtr> operatorIdToOperatorMap;
    std::deque<NodePtr> operatorsToProcess{rootOperators.begin(), rootOperators.end()};
    while (!operatorsToProcess.empty()) {
        auto operatorNode = operatorsToProcess.front()->as<OperatorNode>();
        operatorsToProcess.pop_front();
        uint64_t operatorId = operatorNode->getId();
        // 2. We add each non existing operator to a map and skip adding the operator that already exists in the map.
        // 3. We use the already existing operator whenever available other wise we create a copy of the operator and add it to the map.
        if (operatorIdToOperatorMap[operatorId]) {
            NES_TRACE("QueryPlan: Operator was processed previously");
            operatorNode = operatorIdToOperatorMap[operatorId];
        } else {
            NES_TRACE("QueryPlan: Adding the operator into map");
            operatorIdToOperatorMap[operatorId] = operatorNode->copy();
        }

        // 4. We then check the parent operators of the current operator by looking into the map and add them as the parent of the current operator.
        for (const auto& parentNode : operatorNode->getParents()) {
            auto parentOperator = parentNode->as<OperatorNode>();
            uint64_t parentOperatorId = parentOperator->getId();
            if (operatorIdToOperatorMap[parentOperatorId]) {
                NES_TRACE("QueryPlan: Found the parent operator. Adding as parent to the current operator.");
                parentOperator = operatorIdToOperatorMap[parentOperatorId];
                auto copyOfOperatorNode = operatorIdToOperatorMap[operatorNode->getId()];
                copyOfOperatorNode->addParent(parentOperator);
            } else {
                NES_ERROR("QueryPlan: unable to find the parent operator. This should not have occurred!");
                return nullptr;
            }
        }

        NES_TRACE("QueryPlan: add the child global query nodes for further processing.");
        // 5. We push the children operators to the queue of operators to be processed.
        for (const auto& childrenOperator : operatorNode->getChildren()) {
            operatorsToProcess.push_back(childrenOperator);
        }
    }

    std::vector<OperatorNodePtr> duplicateRootOperators;
    for (const auto& rootOperator : rootOperators) {
        NES_TRACE("QueryPlan: Finding the operator with same id in the map.");
        duplicateRootOperators.push_back(operatorIdToOperatorMap[rootOperator->getId()]);
    }
    operatorIdToOperatorMap.clear();
    auto newQueryPlan = QueryPlan::create(queryId, INVALID_QUERY_ID, duplicateRootOperators);
    newQueryPlan->setSourceConsumed(sourceConsumed);
    newQueryPlan->setFaultToleranceType(faultToleranceType);
    newQueryPlan->setLineageType(lineageType);
    newQueryPlan->setPlacementStrategy(placementStrategy);
    return newQueryPlan;
}

std::string QueryPlan::getSourceConsumed() const { return sourceConsumed; }

void QueryPlan::setSourceConsumed(const std::string& sourceName) { sourceConsumed = sourceName; }

FaultToleranceType QueryPlan::getFaultToleranceType() const { return faultToleranceType; }

void QueryPlan::setFaultToleranceType(FaultToleranceType faultToleranceType) { this->faultToleranceType = faultToleranceType; }

LineageType QueryPlan::getLineageType() const { return lineageType; }

void QueryPlan::setLineageType(LineageType lineageType) { this->lineageType = lineageType; }

Optimizer::PlacementStrategy QueryPlan::getPlacementStrategy() const { return placementStrategy; }

void QueryPlan::setPlacementStrategy(Optimizer::PlacementStrategy placementStrategy) {
    this->placementStrategy = placementStrategy;
}

std::set<OperatorNodePtr> QueryPlan::findAllOperatorsBetween(const std::set<OperatorNodePtr>& downstreamOperators,
                                                             const std::set<OperatorNodePtr>& upstreamOperators) {

    std::set<OperatorNodePtr> operatorsBetween;

    //initialize the operators to visit with upstream operators of all downstream operators
    for (const auto& downStreamOperator : downstreamOperators) {
        auto operatorsBetweenChildAndTargetUpstream =
            findOperatorsBetweenSourceAndTargetOperators(downStreamOperator->as_if<OperatorNode>(), upstreamOperators);
        operatorsBetween.insert(operatorsBetweenChildAndTargetUpstream.begin(), operatorsBetweenChildAndTargetUpstream.end());
    }

    if (!operatorsBetween.empty()) {
        //Remove upstream operators
        for (const auto& upstreamOperator : upstreamOperators) {
            erase_if(operatorsBetween, [upstreamOperator](const auto& operatorToErase) {
                return operatorToErase->getId() == upstreamOperator->getId();
            });
        }

        //Remove downstream operators
        for (const auto& downstreamOperator : downstreamOperators) {
            erase_if(operatorsBetween, [downstreamOperator](const auto& operatorToErase) {
                return operatorToErase->getId() == downstreamOperator->getId();
            });
        }
    }

    return operatorsBetween;
}

std::set<OperatorNodePtr>
QueryPlan::findOperatorsBetweenSourceAndTargetOperators(const OperatorNodePtr& sourceOperator,
                                                        const std::set<OperatorNodePtr>& targetOperators) {

    //Find if downstream operator is also in the vector of target operators
    auto found = std::find_if(targetOperators.begin(), targetOperators.end(), [&](const auto& upstreamOperator) {
        return upstreamOperator->getId() == sourceOperator->getId();
    });

    //If downstream operator is in the list of target operators then return the operator
    if (found != targetOperators.end()) {
        return {sourceOperator};
    }

    bool foundTargetUpstreamOperator = false;
    std::set<OperatorNodePtr> operatorsBetween;
    //Check further upstream operators if they are in the target operator vector
    for (const auto& nextUpstreamOperatorToCheck : sourceOperator->getChildren()) {

        //Fetch the operators between upstream and target operators
        auto operatorsBetweenUpstreamAndTargetUpstream =
            findOperatorsBetweenSourceAndTargetOperators(nextUpstreamOperatorToCheck->as_if<OperatorNode>(), targetOperators);

        //If there are operators between upstream and target operators then mark the input down stream operator for return
        if (!operatorsBetweenUpstreamAndTargetUpstream.empty()) {
            foundTargetUpstreamOperator = true;
            operatorsBetween.insert(operatorsBetweenUpstreamAndTargetUpstream.begin(),
                                    operatorsBetweenUpstreamAndTargetUpstream.end());
        }
    }

    //Add downstream operator
    if (foundTargetUpstreamOperator) {
        operatorsBetween.insert(sourceOperator);
    }
    return operatorsBetween;
}

}// namespace NES