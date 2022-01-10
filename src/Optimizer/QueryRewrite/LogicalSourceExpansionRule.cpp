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

            //            NES_TRACE("LogicalSourceExpansionRule: Get all root nodes of the duplicated logical sub-graph to connect with "
            //                      "original graph");
            //            std::vector<NodePtr> rootNodes = duplicateOperator->getAllRootNodes();
            //            NES_TRACE("LogicalSourceExpansionRule: Get all nodes in the duplicated logical sub-graph");
            //            std::vector<NodePtr> family = duplicateOperator->getAndFlattenAllAncestors();
            //
            //            NES_TRACE("LogicalSourceExpansionRule: For each original head operator find the duplicate operator and connect "
            //                      "it to the original graph");
            //            for (const auto& originalRootOperator : originalRootOperators) {
            //
            //                NES_TRACE("LogicalSourceExpansionRule: Search the duplicate operator equal to original head operator");
            //                auto found = std::find_if(family.begin(), family.end(), [&](const NodePtr& member) {
            //                    return member->as<OperatorNode>()->getId() == originalRootOperator->getId();
            //                });
            //
            //                if (found != family.end()) {
            //                    NES_TRACE("LogicalSourceExpansionRule: Found the duplicate operator equal to the original head operator");
            //                    NES_TRACE(
            //                        "LogicalSourceExpansionRule: Add the duplicate operator to the parent of the original head operator");
            //                    for (const auto& parent : originalRootOperator->getParents()) {
            //                        NES_TRACE("LogicalSourceExpansionRule: Assign the duplicate operator a new operator id.");
            //                        (*found)->as<OperatorNode>()->setId(Util::getNextOperatorId());
            //                        parent->addChild(*found);
            //                    }
            //                    family.erase(found);
            //                } else {
            //                    //Throw exception
            //                }
            //            }

            //            NES_TRACE("LogicalSourceExpansionRule: Assign all operators in the duplicated operator graph a new operator id.");
            //            for (auto& member : family) {
            //                member->as<OperatorNode>()->setId(Util::getNextOperatorId());
            //            }
        }
    }
    NES_DEBUG("LogicalSourceExpansionRule: Finished applying LogicalSourceExpansionRule plan=" << queryPlan->toString());
    return queryPlan;
}

OperatorNodePtr LogicalSourceExpansionRule::getLogicalGraphToDuplicate(const OperatorNodePtr& operatorNode) {
    NES_DEBUG("LogicalSourceExpansionRule: Get the logical graph to duplicate.");
    //    if (isBlockingOperator(operatorNode)) {
    //        NES_TRACE("LogicalSourceExpansionRule: Found the first binary or sink operator.");
    //        return std::tuple<OperatorNodePtr, std::set<OperatorNodePtr>>();
    //    }

    //    NES_TRACE("LogicalSourceExpansionRule: Create duplicate of the input operator.");
    //    OperatorNodePtr copyOfOperator = operatorNode->copy();
    //    std::set<OperatorNodePtr> originalRootOperators;

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

        //        OperatorNodePtr duplicatedParentOperator;
        //        std::set<OperatorNodePtr> rootOperators;
        //
        //        NES_TRACE("LogicalSourceExpansionRule: Get the duplicated parent operator and its ancestors.");
        //        std::tie(duplicatedParentOperator, rootOperators) = getLogicalGraphToDuplicate(parentOperator);
        //
        //        if (duplicatedParentOperator) {
        //            NES_TRACE("LogicalSourceExpansionRule: Got a duplicated parent operator. Add the duplicate as parent to the copy of "
        //                      "input operator.");
        //            copyOfOperator->addParent(duplicatedParentOperator);
        //            NES_TRACE("LogicalSourceExpansionRule: Add the original head operators to the list of head operators for the input "
        //                      "operator");
        //
        //            originalRootOperators.insert(rootOperators.begin(), rootOperators.end());
        //        } else {
        //            NES_TRACE("LogicalSourceExpansionRule: Parent operator was either n-ary or was of type sink.");
        //            NES_TRACE("LogicalSourceExpansionRule: Add the input operator as original head operator of the duplicated sub-graph");
        //            originalRootOperators.insert(operatorNode);
        //        }
    }
    return operatorNode;
}

bool LogicalSourceExpansionRule::isBlockingOperator(const OperatorNodePtr& operatorNode) {
    return (operatorNode->instanceOf<SinkLogicalOperatorNode>() || operatorNode->instanceOf<WindowLogicalOperatorNode>()
            || operatorNode->instanceOf<UnionLogicalOperatorNode>() || operatorNode->instanceOf<JoinLogicalOperatorNode>());
}

}// namespace NES::Optimizer
