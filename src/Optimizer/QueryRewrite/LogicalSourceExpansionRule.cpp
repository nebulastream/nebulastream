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

#include "Nodes/Util/Iterators/DepthFirstNodeIterator.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
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
        NES_DEBUG("LogicalSourceExpansionRule: Get the number of physical source locations in the topology.");
        std::vector<TopologyNodePtr> sourceLocations =
            streamCatalog->getSourceNodesForLogicalStream(sourceDescriptor->getStreamName());

        NES_DEBUG("LogicalSourceExpansionRule: Found " << sourceLocations.size()
                                                       << " physical source locations in the topology.");

        NES_TRACE("LogicalSourceExpansionRule: Find the logical sub-graph to be duplicated for the source operator.");
        OperatorNodePtr operatorToDuplicate = getLogicalGraphToDuplicate(sourceOperator);
        NES_TRACE("LogicalSourceExpansionRule: Create " << sourceLocations.size() - 1
                                                        << " duplicated logical sub-graph and add to original graph");

        for (auto& sourceLocation : sourceLocations) {
            NES_TRACE("LogicalSourceExpansionRule: Create duplicated logical sub-graph");
            OperatorNodePtr duplicateOperator = operatorToDuplicate->duplicate();
            //Add to the source operator the id of the physical node where we have to pin the operator
            duplicateOperator->getAllLeafNodes();
            duplicateOperator->addProperty("PinnedNodeId", sourceLocation->getId());

            //Flatten the graph to duplicate and find operators that need to be connected to blocking parents.
            const std::vector<NodePtr>& allOperators = duplicateOperator->getAndFlattenAllAncestors();

            for (auto& node : allOperators) {
                auto operatorNode = node->as<OperatorNode>();
                const std::any& value = operatorNode->getProperty("ListOfBlockingParents");
                //Check if the operator need to be connected to a blocking parent
                if (value.has_value()) {
                    auto listOfConnectedBlockingParents = std::any_cast<std::vector<uint64_t>>(value);
                    //Iterate over all blocking parent ids and connect the duplicated operator
                    for (auto blockingParentId : listOfConnectedBlockingParents) {
                        auto blockingOperator = blockingOperators[blockingParentId];
                        if (!blockingOperator) {
                            //Throw an exception as this should never occur
                        }
                        //Assign new operator id
                        operatorNode->setId(Util::getNextOperatorId());
                        blockingOperator->addChild(operatorNode);
                    }
                }
            }
        }
    }
    NES_DEBUG("LogicalSourceExpansionRule: Finished applying LogicalSourceExpansionRule plan=" << queryPlan->toString());
    return queryPlan;
}

OperatorNodePtr LogicalSourceExpansionRule::getLogicalGraphToDuplicate(const OperatorNodePtr& operatorNode) {
    NES_DEBUG("LogicalSourceExpansionRule: Get the logical graph to duplicate.");
    NES_TRACE("LogicalSourceExpansionRule: For each parent look if their ancestor has a n-ary operator or a sink operator.");
    auto parentOperators = operatorNode->getParents();
    for (const auto& parent : parentOperators) {

        //Check if the parent operator is a blocking operator or not
        auto parentOperator = parent->as<OperatorNode>();
        if (!isBlockingOperator(parentOperator)) {
            getLogicalGraphToDuplicate(parentOperator);
        } else {
            //If parent is blocking then remove current operator as its child.
            // Add to the current operator information about operator id of the removed parent.
            // We use this information post expansion to re-add the connection later.

            if (!parent->removeChild(operatorNode)) {
                //Throw exception as we expect to freely add or remove nodes in a query plan
            }

            //extract the list of connected blocking parents and add the current parent to the list
            std::any value = operatorNode->getProperty("ListOfBlockingParents");
            if (value.has_value()) {
                auto listOfConnectedBlockingParents = std::any_cast<std::vector<uint64_t>>(value);
                listOfConnectedBlockingParents.emplace_back(parentOperator->getId());
                operatorNode->addProperty("ListOfBlockingParents", listOfConnectedBlockingParents);
            } else {
                std::vector<uint64_t> listOfConnectedBlockingParents;
                listOfConnectedBlockingParents.emplace_back(parentOperator->getId());
                operatorNode->addProperty("ListOfBlockingParents", listOfConnectedBlockingParents);
            }
        }
    }
    return operatorNode;
}

bool LogicalSourceExpansionRule::isBlockingOperator(const OperatorNodePtr& operatorNode) {
    return (operatorNode->instanceOf<SinkLogicalOperatorNode>() || operatorNode->instanceOf<WindowLogicalOperatorNode>()
            || operatorNode->instanceOf<UnionLogicalOperatorNode>() || operatorNode->instanceOf<JoinLogicalOperatorNode>());
}

}// namespace NES::Optimizer
