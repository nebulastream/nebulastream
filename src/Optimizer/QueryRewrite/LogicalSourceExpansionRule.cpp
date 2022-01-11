/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
#include <utility>

namespace NES::Optimizer {

LogicalSourceExpansionRule::LogicalSourceExpansionRule(StreamCatalogPtr streamCatalog)
    : streamCatalog(std::move(streamCatalog)) {}

LogicalSourceExpansionRulePtr LogicalSourceExpansionRule::create(StreamCatalogPtr streamCatalog) {
    return std::make_shared<LogicalSourceExpansionRule>(LogicalSourceExpansionRule(std::move(streamCatalog)));
}

QueryPlanPtr LogicalSourceExpansionRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("LogicalSourceExpansionRule: Apply Logical source expansion rule for the query.");
    NES_DEBUG("LogicalSourceExpansionRule: Get all logical source operators in the query for plan=" << queryPlan->toString());

    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    //Compute a map of all blocking operators in the query plan
    std::unordered_map<uint64_t, OperatorNodePtr> blockingOperators;
    for (auto rootOperator : queryPlan->getRootOperators()) {
        DepthFirstNodeIterator depthFirstNodeIterator(rootOperator);
        for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
            NES_TRACE("FilterPushDownRule: Iterate and find the predicate with FieldAccessExpression Node");
            auto operatorToIterate = (*itr)->as<OperatorNode>();
            if (isBlockingOperator(operatorToIterate)) {
                blockingOperators[operatorToIterate->getId()] = operatorToIterate;
            }
        }
    }

    //Iterate over all source operators
    for (auto& sourceOperator : sourceOperators) {
        SourceDescriptorPtr sourceDescriptor = sourceOperator->getSourceDescriptor();
        NES_TRACE("LogicalSourceExpansionRule: Get the number of physical source locations in the topology.");
        std::vector<TopologyNodePtr> sourceLocations =
            streamCatalog->getSourceNodesForLogicalStream(sourceDescriptor->getStreamName());
        NES_TRACE("LogicalSourceExpansionRule: Found " << sourceLocations.size()
                                                       << " physical source locations in the topology.");

        removeConnectedBlockingOperators(sourceOperator);
        NES_TRACE("LogicalSourceExpansionRule: Create " << sourceLocations.size() - 1
                                                        << " duplicated logical sub-graph and add to original graph");

        //Create one duplicate operator fro each physical source
        for (auto& sourceLocation : sourceLocations) {
            NES_TRACE("LogicalSourceExpansionRule: Create duplicated logical sub-graph");
            OperatorNodePtr duplicateOperator = sourceOperator->duplicate();
            //Add to the source operator the id of the physical node where we have to pin the operator
            duplicateOperator->getAllLeafNodes();
            //NOTE: This is required at the time of placement to know where the source operator is pinned
            duplicateOperator->addProperty(PINNED_NODE_ID, sourceLocation->getId());

            //Flatten the graph to duplicate and find operators that need to be connected to blocking parents.
            const std::vector<NodePtr>& allOperators = duplicateOperator->getAndFlattenAllAncestors();

            for (auto& node : allOperators) {
                auto operatorNode = node->as<OperatorNode>();
                //Assign new operator id
                operatorNode->setId(Util::getNextOperatorId());

                //Fetch the connected blocking operators to the operator
                const std::any& value = operatorNode->getProperty(LIST_OF_BLOCKING_UPSTREAM_OPERATOR_IDS);
                //Check if the operator need to be connected to a blocking parent
                if (value.has_value()) {
                    auto listOfConnectedBlockingParents = std::any_cast<std::vector<uint64_t>>(value);
                    //Iterate over all blocking parent ids and connect the duplicated operator
                    for (auto blockingParentId : listOfConnectedBlockingParents) {
                        auto blockingOperator = blockingOperators[blockingParentId];
                        if (!blockingOperator) {
                            //Throw an exception as this should never occur
                        }
                        blockingOperator->addChild(operatorNode);
                    }
                }
                //Remove the property
                operatorNode->removeProperty(LIST_OF_BLOCKING_UPSTREAM_OPERATOR_IDS);
            }
        }
    }
    NES_DEBUG("LogicalSourceExpansionRule: Finished applying LogicalSourceExpansionRule plan=" << queryPlan->toString());
    return queryPlan;
}

void LogicalSourceExpansionRule::removeConnectedBlockingOperators(const OperatorNodePtr& operatorNode) {

    //Check if upstream (parent) operator of this operator is blocking or not if not then recursively call this method for the upstream
    // operator
    auto parentOperators = operatorNode->getParents();
    NES_TRACE("LogicalSourceExpansionRule: For each parent look if their ancestor has a n-ary operator or a sink operator.");
    for (const auto& parent : parentOperators) {

        //Check if the parent operator is a blocking operator or not
        auto parentOperator = parent->as<OperatorNode>();
        if (!isBlockingOperator(parentOperator)) {
            removeConnectedBlockingOperators(parentOperator);
        } else {
            //If parent is blocking then remove current operator as its child.
            // Add to the current operator information about operator id of the removed parent.
            // We use this information post expansion to re-add the connection later.

            if (!parent->removeChild(operatorNode)) {
                //Throw exception as we expect to freely add or remove nodes in a query plan
            }

            //extract the list of connected blocking parents and add the current parent to the list
            std::any value = operatorNode->getProperty(LIST_OF_BLOCKING_UPSTREAM_OPERATOR_IDS);
            if (value.has_value()) {//update the existing list
                auto listOfConnectedBlockingParents = std::any_cast<std::vector<uint64_t>>(value);
                listOfConnectedBlockingParents.emplace_back(parentOperator->getId());
                operatorNode->addProperty(LIST_OF_BLOCKING_UPSTREAM_OPERATOR_IDS, listOfConnectedBlockingParents);
            } else {//create a new entry if value doesn't exist
                std::vector<uint64_t> listOfConnectedBlockingParents;
                listOfConnectedBlockingParents.emplace_back(parentOperator->getId());
                operatorNode->addProperty(LIST_OF_BLOCKING_UPSTREAM_OPERATOR_IDS, listOfConnectedBlockingParents);
            }
        }
    }
}

bool LogicalSourceExpansionRule::isBlockingOperator(const OperatorNodePtr& operatorNode) {
    return (operatorNode->instanceOf<SinkLogicalOperatorNode>() || operatorNode->instanceOf<WindowLogicalOperatorNode>()
            || operatorNode->instanceOf<UnionLogicalOperatorNode>() || operatorNode->instanceOf<JoinLogicalOperatorNode>());
}

}// namespace NES::Optimizer
