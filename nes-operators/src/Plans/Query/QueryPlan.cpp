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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <algorithm>
#include <set>
#include <stack>
#include <utility>

namespace NES {

QueryPlanPtr QueryPlan::create(QueryId queryId, std::vector<OperatorPtr> rootOperators) {
    return std::make_shared<QueryPlan>(QueryPlan(queryId, std::move(rootOperators)));
}

QueryPlanPtr QueryPlan::create(QueryId queryId) { return std::make_shared<QueryPlan>(QueryPlan(queryId)); }

QueryPlanPtr QueryPlan::create(OperatorPtr rootOperator) {
    return std::make_shared<QueryPlan>(QueryPlan(std::move(rootOperator)));
}

QueryPlanPtr QueryPlan::create() { return std::make_shared<QueryPlan>(QueryPlan()); }

QueryPlan::QueryPlan() {}

QueryPlan::QueryPlan(OperatorPtr rootOperator) : queryId(INVALID_QUERY_ID) { rootOperators.push_back(std::move(rootOperator)); }

QueryPlan::QueryPlan(QueryId queryId, std::vector<OperatorPtr> rootOperators)
    : rootOperators(std::move(rootOperators)), queryId(queryId) {}

QueryPlan::QueryPlan(QueryId queryId) : queryId(queryId) {}

std::vector<SourceLogicalOperatorPtr> QueryPlan::getSourceOperators() const {
    NES_DEBUG("QueryPlan: Get all source operators by traversing all the root nodes.");
    std::set<SourceLogicalOperatorPtr> sourceOperatorsSet;
    for (const auto& rootOperator : rootOperators) {
        auto sourceOptrs = rootOperator->getNodesByType<SourceLogicalOperator>();
        NES_DEBUG("QueryPlan: insert all source operators to the collection");
        sourceOperatorsSet.insert(sourceOptrs.begin(), sourceOptrs.end());
    }
    NES_DEBUG("QueryPlan: Found {} source operators.", sourceOperatorsSet.size());
    std::vector<SourceLogicalOperatorPtr> sourceOperators{sourceOperatorsSet.begin(), sourceOperatorsSet.end()};
    return sourceOperators;
}

std::vector<SinkLogicalOperatorPtr> QueryPlan::getSinkOperators() const {
    NES_DEBUG("QueryPlan: Get all sink operators by traversing all the root nodes.");
    std::vector<SinkLogicalOperatorPtr> sinkOperators;
    for (const auto& rootOperator : rootOperators) {
        auto sinkOperator = rootOperator->as<SinkLogicalOperator>();
        sinkOperators.emplace_back(sinkOperator);
    }
    NES_DEBUG("QueryPlan: Found {} sink operators.", sinkOperators.size());
    return sinkOperators;
}

void QueryPlan::appendOperatorAsNewRoot(const OperatorPtr& operatorNode) {
    NES_DEBUG("QueryPlan: Appending operator {} as new root of the plan.", operatorNode->toString());
    for (const auto& rootOperator : rootOperators) {
        if (!rootOperator->addParent(operatorNode)) {
            NES_THROW_RUNTIME_ERROR("QueryPlan: Unable to add operator " + operatorNode->toString() + " as parent to "
                                    + rootOperator->toString());
        }
    }
    NES_DEBUG("QueryPlan: Clearing current root operators.");
    clearRootOperators();
    NES_DEBUG("QueryPlan: Pushing input operator node as new root.");
    rootOperators.push_back(operatorNode);
}

void QueryPlan::clearRootOperators() { rootOperators.clear(); }

std::string QueryPlan::toString() const {
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler::create(ss);
    for (const auto& rootOperator : rootOperators) {
        dumpHandler->dump(rootOperator);
    }
    return ss.str();
}

std::vector<OperatorPtr> QueryPlan::getRootOperators() const { return rootOperators; }

std::vector<OperatorPtr> QueryPlan::getLeafOperators() const {
    // Find all the leaf nodes in the query plan
    NES_DEBUG("QueryPlan: Get all leaf nodes in the query plan.");
    std::vector<OperatorPtr> leafOperators;
    // Maintain a list of visited nodes as there are multiple root nodes
    std::set<OperatorId> visitedOpIds;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
            auto visitingOp = (*itr)->as<Operator>();
            if (visitedOpIds.contains(visitingOp->getId())) {
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

std::unordered_set<OperatorPtr> QueryPlan::getAllOperators() const {
    // Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<OperatorPtr> visitedOperators;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
            auto visitingOp = (*itr)->as<Operator>();
            if (visitedOperators.contains(visitingOp)) {
                // skip rest of the steps as the node found in already visited node list
                continue;
            }
            NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
            visitedOperators.insert(visitingOp);
        }
    }
    return visitedOperators;
}

bool QueryPlan::hasOperatorWithId(OperatorId operatorId) {
    NES_DEBUG("QueryPlan: Checking if the operator exists in the query plan or not");
    if (getOperatorWithOperatorId(operatorId)) {
        return true;
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return false;
}

OperatorPtr QueryPlan::getOperatorWithOperatorId(OperatorId operatorId) const {
    NES_DEBUG("QueryPlan: Checking if the operator with id {} exists in the query plan or not", operatorId);
    for (auto rootOperator : rootOperators) {

        if (rootOperator->getId() == operatorId) {
            NES_DEBUG("QueryPlan: Found operator {} in the query plan", operatorId);
            return rootOperator;
        }

        //Look up in the child operators
        auto matchingOperator = rootOperator->getChildWithOperatorId(operatorId);
        if (matchingOperator) {
            return matchingOperator->as<Operator>();
        }
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return nullptr;
}

OperatorPtr QueryPlan::getOperatorWithStatisticId(StatisticId statisticId) const {
    NES_DEBUG("QueryPlan: Checking if the operator with statisticId {} exists in the query plan or not", statisticId);
    for (auto rootOperator : rootOperators) {
        if (rootOperator->getStatisticId() == statisticId) {
            NES_DEBUG("QueryPlan: Found operator {} in the query plan", rootOperator->toString());
            return rootOperator;
        }

        //Look up in the child operators
        auto matchingOperator = rootOperator->getChildWithStatisticId(statisticId);
        if (matchingOperator) {
            NES_DEBUG("QueryPlan: Found operator {} in the query plan", matchingOperator->toString());
            return matchingOperator->as<Operator>();
        }
    }
    NES_WARNING("QueryPlan: Unable to find operator with matching statisticId {}", statisticId);
    return nullptr;
}

QueryId QueryPlan::getQueryId() const { return queryId; }

void QueryPlan::setQueryId(QueryId queryId) { this->queryId = queryId; }

void QueryPlan::addRootOperator(const OperatorPtr& newRootOperator) {
    //Check if a root with the id already present
    auto found = std::find_if(rootOperators.begin(), rootOperators.end(), [&](const OperatorPtr& root) {
        return newRootOperator->getId() == root->getId();
    });

    // If not present then add it
    if (found == rootOperators.end()) {
        rootOperators.push_back(newRootOperator);
    } else {
        NES_WARNING("Root operator with id {} already present in the plan", newRootOperator->getId());
    }
}

void QueryPlan::removeAsRootOperator(OperatorPtr root) {
    NES_DEBUG("QueryPlan: removing operator {} as root operator.", root->toString());
    auto found = std::find_if(rootOperators.begin(), rootOperators.end(), [&](const OperatorPtr& rootOperator) {
        return rootOperator->getId() == root->getId();
    });
    if (found != rootOperators.end()) {
        NES_DEBUG(
            "QueryPlan: Found root operator in the root operator list. Removing the operator as the root of the query plan.",
            root->toString());
        rootOperators.erase(found);
    }
}

QueryPlanPtr QueryPlan::copy() {
    NES_INFO("QueryPlan: make copy of this query plan");
    // 1. We start by copying the root operators of this query plan to the queue of operators to be processed
    std::map<OperatorId, OperatorPtr> operatorIdToOperatorMap;
    std::deque<NodePtr> operatorsToProcess{rootOperators.begin(), rootOperators.end()};
    while (!operatorsToProcess.empty()) {
        auto operatorNode = operatorsToProcess.front()->as<Operator>();
        operatorsToProcess.pop_front();
        auto operatorId = operatorNode->getId();
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
            auto parentOperator = parentNode->as<Operator>();
            auto parentOperatorId = parentOperator->getId();
            if (operatorIdToOperatorMap[parentOperatorId]) {
                NES_TRACE("QueryPlan: Found the parent operator. Adding as parent to the current operator.");
                parentOperator = operatorIdToOperatorMap[parentOperatorId];
                auto copyOfOperator = operatorIdToOperatorMap[operatorNode->getId()];
                copyOfOperator->addParent(parentOperator);
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

    std::vector<OperatorPtr> duplicateRootOperators;
    for (const auto& rootOperator : rootOperators) {
        NES_TRACE("QueryPlan: Finding the operator with same id in the map.");
        duplicateRootOperators.push_back(operatorIdToOperatorMap[rootOperator->getId()]);
    }
    operatorIdToOperatorMap.clear();
    auto newQueryPlan = QueryPlan::create(queryId, duplicateRootOperators);
    newQueryPlan->setSourceConsumed(sourceConsumed);
    newQueryPlan->setPlacementStrategy(placementStrategy);
    newQueryPlan->setFaultTolerance(faultTolerance);
    newQueryPlan->setQueryState(currentState);
    return newQueryPlan;
}

std::string QueryPlan::getSourceConsumed() const { return sourceConsumed; }

void QueryPlan::setSourceConsumed(std::string_view sourceName) { sourceConsumed = sourceName; }

Optimizer::PlacementStrategy QueryPlan::getPlacementStrategy() const { return placementStrategy; }

void QueryPlan::setPlacementStrategy(Optimizer::PlacementStrategy placementStrategy) {
    this->placementStrategy = placementStrategy;
}

void QueryPlan::setFaultTolerance(FaultToleranceType faultTolerance) {
    this->faultTolerance = faultTolerance;
}

FaultToleranceType QueryPlan::getFaultTolerance() const { return faultTolerance; }

std::set<OperatorPtr> QueryPlan::findAllOperatorsBetween(const std::set<OperatorPtr>& downstreamOperators,
                                                         const std::set<OperatorPtr>& upstreamOperators) {

    std::set<OperatorPtr> operatorsBetween;

    //initialize the operators to visit with upstream operators of all downstream operators
    for (const auto& downStreamOperator : downstreamOperators) {
        auto operatorsBetweenChildAndTargetUpstream =
            findOperatorsBetweenSourceAndTargetOperators(downStreamOperator->as_if<Operator>(), upstreamOperators);
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

bool QueryPlan::compare(QueryPlanPtr& otherPlan) {
    auto leftRootOperators = this->getRootOperators();
    auto rightRootOperators = otherPlan->getRootOperators();

    if (leftRootOperators.size() != rightRootOperators.size())
        return false;

    // add all root-operators to stack
    std::stack<std::pair<OperatorPtr, OperatorPtr>> stack;
    for (size_t i = 0; i < leftRootOperators.size(); ++i) {
        stack.push(std::make_pair(leftRootOperators[i], rightRootOperators[i]));
    }

    // iterate over stack
    while (!stack.empty()) {
        // get last discovered left and right operator
        auto [leftOperator, rightOperator] = stack.top();
        stack.pop();

        auto leftChildren = leftOperator->getChildren();
        auto rightChildren = rightOperator->getChildren();

        if (leftChildren.size() != rightChildren.size())
            return false;

        // discover children and add them to stack
        for (size_t j = 0; j < leftChildren.size(); ++j) {
            auto leftChild = leftChildren[j]->as<Operator>();
            auto rightChild = rightChildren[j]->as<Operator>();
            if (!leftChild || !rightChild)
                return false;
            stack.push(std::make_pair(leftChild, rightChild));
        }

        // comparison of both operators
        if (!leftOperator->equal(rightOperator))
            return false;
    }
    return true;
}

std::set<OperatorPtr> QueryPlan::findOperatorsBetweenSourceAndTargetOperators(const OperatorPtr& sourceOperator,
                                                                              const std::set<OperatorPtr>& targetOperators) {

    //Find if downstream operator is also in the vector of target operators
    auto found = std::find_if(targetOperators.begin(), targetOperators.end(), [&](const auto& upstreamOperator) {
        return upstreamOperator->getId() == sourceOperator->getId();
    });

    //If downstream operator is in the list of target operators then return the operator
    if (found != targetOperators.end()) {
        return {sourceOperator};
    }

    bool foundTargetUpstreamOperator = false;
    std::set<OperatorPtr> operatorsBetween;
    //Check further upstream operators if they are in the target operator vector
    for (const auto& nextUpstreamOperatorToCheck : sourceOperator->getChildren()) {

        //Fetch the operators between upstream and target operators
        auto operatorsBetweenUpstreamAndTargetUpstream =
            findOperatorsBetweenSourceAndTargetOperators(nextUpstreamOperatorToCheck->as_if<Operator>(), targetOperators);

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

QueryState QueryPlan::getQueryState() const { return currentState; }

void QueryPlan::setQueryState(QueryState newState) { currentState = newState; }

}// namespace NES
