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

#include <Catalogs/Exceptions/PhysicalSourceNotFoundException.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Optimizer/Exceptions/OperatorNotFoundException.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>

namespace NES::Optimizer {

LogicalSourceExpansionRule::LogicalSourceExpansionRule(const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                       bool expandSourceOnly)
    : sourceCatalog(sourceCatalog), expandSourceOnly(expandSourceOnly) {}

LogicalSourceExpansionRulePtr LogicalSourceExpansionRule::create(const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                                 bool expandSourceOnly) {
    return std::make_shared<LogicalSourceExpansionRule>(LogicalSourceExpansionRule(sourceCatalog, expandSourceOnly));
}

QueryPlanPtr LogicalSourceExpansionRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("LogicalSourceExpansionRule: Plan before\n{}", queryPlan->toString());

    std::vector<SourceLogicalOperatorPtr> sourceOperators = queryPlan->getSourceOperators();

    //Compute a map of all blocking operators in the query plan
    std::unordered_map<OperatorId, OperatorPtr> blockingOperators;
    if (expandSourceOnly) {
        //Add downstream operators of the source operators as blocking operator
        for (const auto& sourceOperator : sourceOperators) {
            for (auto& downStreamOp : sourceOperator->getParents()) {
                blockingOperators[downStreamOp->as<Operator>()->getId()] = downStreamOp->as<Operator>();
            }
        }
    } else {
        for (const auto& rootOperator : queryPlan->getRootOperators()) {
            DepthFirstNodeIterator depthFirstNodeIterator(rootOperator);
            for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
                NES_TRACE("LogicalSourceExpansionRule: Iterate and find the predicate with FieldAccessExpression Node");
                auto operatorToIterate = (*itr)->as<Operator>();
                if (isBlockingOperator(operatorToIterate)) {
                    blockingOperators[operatorToIterate->getId()] = operatorToIterate;
                }
            }
        }
    }

    // After we duplicate all non-blocking logical operators, we have the same statistic id for multiple operators.
    // Furthermore, we require to know all statistic ids that belong to one non-duplicated logical operator.
    // Therefore, we do the following two steps:
    //      1. First, we create new statistic id so that the statistic id is unique across the whole system
    //      2. We store for the old statistic ids, all sibling statistic ids in the property. This way, we can still
    //         deduce the new statistic ids from an old one.
    std::unordered_map<StatisticId, std::vector<StatisticId>> siblingStatisticIdToNewStatisticIds;

    //Iterate over all source operators
    for (const auto& sourceOperator : sourceOperators) {
        SourceDescriptorPtr sourceDescriptor = sourceOperator->getSourceDescriptor();
        NES_TRACE("LogicalSourceExpansionRule: Get the number of physical source locations in the topology.");
        auto logicalSourceName = sourceDescriptor->getLogicalSourceName();
        std::vector<Catalogs::Source::SourceCatalogEntryPtr> sourceCatalogEntries =
            sourceCatalog->getPhysicalSources(logicalSourceName);
        NES_TRACE("LogicalSourceExpansionRule: Found {} physical source locations in the topology.", sourceCatalogEntries.size());
        if (sourceCatalogEntries.empty()) {
            throw Exceptions::PhysicalSourceNotFoundException(
                "LogicalSourceExpansionRule: Unable to find physical source locations for the logical source "
                + logicalSourceName);
        }

        if (!expandSourceOnly) {
            removeConnectedBlockingOperators(sourceOperator);
        } else {
            //disconnect all parent operators of the source operator
            for (const auto& downStreamOperator : sourceOperator->getParents()) {
                //If downStreamOperator is blocking then remove source operator as its upstream operator.
                if (!downStreamOperator->removeChild(sourceOperator)) {
                    throw Exceptions::RuntimeException(
                        "LogicalSourceExpansionRule: Unable to remove non-blocking upstream operator from the blocking operator");
                }

                //Add information about blocking operator to the source operator
                addBlockingDownStreamOperator(sourceOperator, downStreamOperator->as<Operator>()->getId());
            }
        }
        NES_TRACE("LogicalSourceExpansionRule: Create {} duplicated logical sub-graph and add to original graph",
                  sourceCatalogEntries.size());

        //Create one duplicate operator for each physical source
        for (const auto& sourceCatalogEntry : sourceCatalogEntries) {
            NES_TRACE("LogicalSourceExpansionRule: Create duplicated logical sub-graph");
            auto duplicateSourceOperator = sourceOperator->duplicate()->as<SourceLogicalOperator>();
            //Add to the source operator the id of the physical node where we have to pin the operator
            //NOTE: This is required at the time of placement to know where the source operator is pinned
            duplicateSourceOperator->addProperty(PINNED_WORKER_ID, sourceCatalogEntry->getTopologyNodeId());
            //Add Physical Source Name to the source descriptor
            auto duplicateSourceDescriptor = sourceDescriptor->copy();
            duplicateSourceDescriptor->setPhysicalSourceName(sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName());
            duplicateSourceOperator->setSourceDescriptor(duplicateSourceDescriptor);
            duplicateSourceOperator->setStatisticId(sourceCatalogEntry->getPhysicalSource()->getStatisticId());

            //Flatten the graph to duplicate and find operators that need to be connected to blocking parents.
            const std::vector<NodePtr>& allOperators = duplicateSourceOperator->getAndFlattenAllAncestors();

            std::unordered_set<OperatorId> visitedOperators;
            for (const auto& node : allOperators) {
                auto operatorNode = node->as<Operator>();

                //Check if the operator has the property containing list of connected blocking downstream operator ids.
                // If so, then connect the operator to the blocking downstream operator
                if (operatorNode->hasProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS)) {
                    //Fetch the blocking upstream operators of this operator
                    const std::any& value = operatorNode->getProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS);
                    auto listOfConnectedBlockingParents = std::any_cast<std::vector<OperatorId>>(value);
                    //Iterate over all blocking parent ids and connect the duplicated operator
                    for (auto blockingParentId : listOfConnectedBlockingParents) {
                        auto blockingOperator = blockingOperators[blockingParentId];
                        if (!blockingOperator) {
                            throw Exceptions::OperatorNotFoundException(
                                fmt::format("LogicalSourceExpansionRule: Unable to find blocking operator with id {}",
                                            blockingParentId));
                        }
                        // Check, if we have visited this operator already
                        if (!visitedOperators.contains(operatorNode->getId())) {
                            visitedOperators.insert(operatorNode->getId());

                            // Getting a new statistic id and storing this statistic id
                            auto newStatisticId = getNextStatisticId();
                            auto siblingStatisticId = operatorNode->getStatisticId();
                            siblingStatisticIdToNewStatisticIds[siblingStatisticId].emplace_back(newStatisticId);
                        }

                        //Assign new operator id
                        operatorNode->setId(getNextOperatorId());
                        blockingOperator->addChild(operatorNode);
                        visitedOperators.insert(operatorNode->getId());
                    }
                    //Remove the property
                    operatorNode->removeProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS);
                } else {
                    // Check, if we have visited this operator already
                    if (!visitedOperators.contains(operatorNode->getId())) {
                        visitedOperators.insert(operatorNode->getId());

                        // Getting a new statistic id and storing this statistic id
                        auto newStatisticId = getNextStatisticId();
                        auto siblingStatisticId = operatorNode->getStatisticId();
                        siblingStatisticIdToNewStatisticIds[siblingStatisticId].emplace_back(newStatisticId);
                    }

                    //Assign new operator id
                    operatorNode->setId(getNextOperatorId());
                    visitedOperators.insert(operatorNode->getId());
                }
            }
        }
    }

    distributeSiblingStatisticId(*queryPlan, siblingStatisticIdToNewStatisticIds);

    NES_DEBUG("LogicalSourceExpansionRule: Plan after\n{}", queryPlan->toString());
    return queryPlan;
}

void LogicalSourceExpansionRule::distributeSiblingStatisticId(
    QueryPlan& queryPlan,
    std::unordered_map<StatisticId, std::vector<StatisticId>>& siblingStatisticIdToNewStatisticIds) const {

    NES_DEBUG("LogicalSourceExpansionRule: Plan before updating the statistic ids\n{}", queryPlan.toString());

    // For each statisticId, we store all new statistic ids in the property map and distribute the new statistic ids
    for (auto& [oldStatisticId, allNewStatisticIds] : siblingStatisticIdToNewStatisticIds) {
        for (auto& newStatisticId : allNewStatisticIds) {
            const auto operatorNode = queryPlan.getOperatorWithStatisticId(oldStatisticId);
            if (operatorNode->instanceOf<SourceLogicalOperator>()) {
                NES_INFO("We do not update the statistic id of a source!");
                continue;
            }

            auto value = operatorNode->as<Operator>()->getProperty(LIST_OF_SIBLING_STATISTIC_IDS);
            std::vector<StatisticId> listOfStatisticIds;
            if (value.has_value()) {
                listOfStatisticIds = std::any_cast<std::vector<StatisticId>>(value);
            } else {
                listOfStatisticIds = std::vector<StatisticId>();
            }
            listOfStatisticIds.insert(listOfStatisticIds.end(), allNewStatisticIds.begin(), allNewStatisticIds.end());
            operatorNode->as<Operator>()->addProperty(LIST_OF_SIBLING_STATISTIC_IDS, listOfStatisticIds);
            operatorNode->setStatisticId(newStatisticId);
        }
    }
}

void LogicalSourceExpansionRule::removeConnectedBlockingOperators(const NodePtr& operatorNode) {

    //Check if downstream (parent) operator of this operator is blocking or not if not then recursively call this method for the
    // downstream operator
    auto downStreamOperators = operatorNode->getParents();
    NES_TRACE("LogicalSourceExpansionRule: For each parent look if their ancestor has a n-ary operator or a sink operator.");
    for (const auto& downStreamOperator : downStreamOperators) {

        //Check if the downStreamOperator operator is a blocking operator or not
        if (!isBlockingOperator(downStreamOperator)) {
            removeConnectedBlockingOperators(downStreamOperator);
        } else {
            // If downStreamOperator is blocking then remove current operator as its upstream operator.
            if (!downStreamOperator->removeChild(operatorNode)) {
                throw Exceptions::RuntimeException(
                    "LogicalSourceExpansionRule: Unable to remove non-blocking upstream operator from the blocking operator");
            }

            // Add to the current operator information about operator id of the removed downStreamOperator.
            // We will use this information post expansion to re-add the connection later.
            addBlockingDownStreamOperator(operatorNode, downStreamOperator->as_if<Operator>()->getId());
        }
    }
}

void LogicalSourceExpansionRule::addBlockingDownStreamOperator(const NodePtr& operatorNode, OperatorId downStreamOperatorId) {
    //extract the list of connected blocking parents and add the current parent to the list
    std::any value = operatorNode->as_if<Operator>()->getProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS);
    if (value.has_value()) {//update the existing list
        auto listOfConnectedBlockingParents = std::any_cast<std::vector<OperatorId>>(value);
        listOfConnectedBlockingParents.emplace_back(downStreamOperatorId);
        operatorNode->as_if<Operator>()->addProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS, listOfConnectedBlockingParents);
    } else {//create a new entry if value doesn't exist
        std::vector<OperatorId> listOfConnectedBlockingParents;
        listOfConnectedBlockingParents.emplace_back(downStreamOperatorId);
        operatorNode->as_if<Operator>()->addProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS, listOfConnectedBlockingParents);
    }
}

bool LogicalSourceExpansionRule::isBlockingOperator(const NodePtr& operatorNode) {
    return (operatorNode->instanceOf<SinkLogicalOperator>() || operatorNode->instanceOf<LogicalWindowOperator>()
            || operatorNode->instanceOf<LogicalUnionOperator>() || operatorNode->instanceOf<LogicalJoinOperator>()
            || operatorNode->instanceOf<FlatMapUDFLogicalOperator>()
            || operatorNode->instanceOf<Experimental::LogicalBatchJoinOperator>());
}

}// namespace NES::Optimizer
