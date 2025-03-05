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

#include <memory>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <ErrorHandling.hpp>

namespace NES::Optimizer
{

LogicalSourceExpansionRule::LogicalSourceExpansionRule(
    const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog, bool expandSourceOnly)
    : sourceCatalog(sourceCatalog), expandSourceOnly(expandSourceOnly)
{
}

std::shared_ptr<LogicalSourceExpansionRule>
LogicalSourceExpansionRule::create(const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog, bool expandSourceOnly)
{
    return std::make_shared<LogicalSourceExpansionRule>(LogicalSourceExpansionRule(sourceCatalog, expandSourceOnly));
}

std::shared_ptr<QueryPlan> LogicalSourceExpansionRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    NES_INFO("LogicalSourceExpansionRule: Plan before\n{}", queryPlan->toString());

    std::vector<std::shared_ptr<SourceNameLogicalOperator>> sourceOperators = queryPlan->getSourceOperators<SourceNameLogicalOperator>();

    /// Compute a map of all blocking operators in the query plan
    std::unordered_map<OperatorId, std::shared_ptr<Operator>> blockingOperators;
    if (expandSourceOnly)
    {
        /// Add downstream operators of the source operators as blocking operator
        for (const auto& sourceOperator : sourceOperators)
        {
            for (auto& downStreamOp : sourceOperator->getParents())
            {
                blockingOperators[NES::Util::as<Operator>(downStreamOp)->getId()] = NES::Util::as<Operator>(downStreamOp);
            }
        }
    }
    else
    {
        for (const auto& rootOperator : queryPlan->getRootOperators())
        {
            DepthFirstNodeIterator depthFirstNodeIterator(rootOperator);
            for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr)
            {
                NES_TRACE("FilterPushDownRule: Iterate and find the predicate with FieldAccessFunction Node");
                auto operatorToIterate = NES::Util::as<Operator>(*itr);
                if (isBlockingOperator(operatorToIterate))
                {
                    blockingOperators[operatorToIterate->getId()] = operatorToIterate;
                }
            }
        }
    }

    /// Iterate over all source operators
    for (const auto& sourceOperator : sourceOperators)
    {
        NES_TRACE("LogicalSourceExpansionRule: Get the number of physical source locations in the topology.");
        auto logicalSourceName = sourceOperator->getLogicalSourceName();

        const std::vector<std::shared_ptr<Catalogs::Source::SourceCatalogEntry>> sourceCatalogEntries
            = sourceCatalog->getPhysicalSources(logicalSourceName);
        NES_TRACE("LogicalSourceExpansionRule: Found {} physical source locations in the topology.", sourceCatalogEntries.size());
        if (sourceCatalogEntries.empty())
        {
            auto ex = PhysicalSourceNotFoundInQueryDescription();
            ex.what() += "LogicalSourceExpansionRule: Unable to find physical source locations for the logical source " + logicalSourceName;
            throw ex;
        }

        if (!expandSourceOnly)
        {
            removeConnectedBlockingOperators(sourceOperator);
        }
        else
        {
            /// disconnect all parent operators of the source operator
            for (const auto& downStreamOperator : sourceOperator->getParents())
            {
                /// If downStreamOperator is blocking then remove source operator as its upstream operator.
                auto success = downStreamOperator->removeChild(sourceOperator);
                INVARIANT(success, "Unable to remove non-blocking upstream operator from the blocking operator");

                /// Add information about blocking operator to the source operator
                addBlockingDownStreamOperator(sourceOperator, NES::Util::as<Operator>(downStreamOperator)->getId());
            }
        }
        NES_TRACE(
            "LogicalSourceExpansionRule: Create {} duplicated logical sub-graph and add to original graph", sourceCatalogEntries.size());

        /// Create one duplicate operator for each physical source
        for (const auto& sourceCatalogEntry : sourceCatalogEntries)
        {
            NES_TRACE("LogicalSourceExpansionRule: Create duplicated logical sub-graph");
            auto duplicateSourceOperator = NES::Util::as<SourceNameLogicalOperator>(sourceOperator->duplicate());
            NES_DEBUG(
                "Catalog schema for current source: {}",
                sourceCatalog->getSchemaForLogicalSource(sourceOperator->getLogicalSourceName())->toString());
            /// Add to the source operator the id of the physical node where we have to pin the operator
            /// NOTE: This is required at the time of placement to know where the source operator is pinned
            duplicateSourceOperator->addProperty(PINNED_WORKER_ID, sourceCatalogEntry->getTopologyNodeId());
            duplicateSourceOperator->setSchema(sourceOperator->getSchema());

            /// Flatten the graph to duplicate and find operators that need to be connected to blocking parents.
            const std::vector<std::shared_ptr<Node>>& allOperators = duplicateSourceOperator->getAndFlattenAllAncestors();

            std::unordered_set<OperatorId> visitedOperators;
            for (const auto& node : allOperators)
            {
                auto operatorNode = NES::Util::as<Operator>(node);

                /// Check if the operator has the property containing list of connected blocking downstream operator ids.
                /// If so, then connect the operator to the blocking downstream operator
                if (operatorNode->hasProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS))
                {
                    /// Fetch the blocking upstream operators of this operator
                    const std::any& value = operatorNode->getProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS);
                    auto listOfConnectedBlockingParents = std::any_cast<std::vector<OperatorId>>(value);
                    /// Iterate over all blocking parent ids and connect the duplicated operator
                    for (auto blockingParentId : listOfConnectedBlockingParents)
                    {
                        auto blockingOperator = blockingOperators[blockingParentId];
                        if (!blockingOperator)
                        {
                            auto ex = OperatorNotFound();
                            ex.what() += fmt::format(
                                " in LogicalSourceExpansionRule: Unable to find blocking operator with id {}", blockingParentId);
                            throw ex;
                        }
                        /// Check, if we have visited this operator already
                        if (!visitedOperators.contains(operatorNode->getId()))
                        {
                            visitedOperators.insert(operatorNode->getId());
                        }

                        /// Assign new operator id
                        operatorNode->setId(getNextOperatorId());
                        blockingOperator->addChild(operatorNode);
                        visitedOperators.insert(operatorNode->getId());
                    }
                    /// Remove the property
                    operatorNode->removeProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS);
                }
                else
                {
                    /// Check, if we have visited this operator already
                    if (!visitedOperators.contains(operatorNode->getId()))
                    {
                        visitedOperators.insert(operatorNode->getId());
                    }

                    /// Assign new operator id
                    operatorNode->setId(getNextOperatorId());
                    visitedOperators.insert(operatorNode->getId());
                }
            }
            auto sourceDescriptor = sourceCatalogEntry->getPhysicalSource()->createSourceDescriptor(duplicateSourceOperator->getSchema());
            auto operatorSourceLogicalDescriptor
                = std::make_shared<SourceDescriptorLogicalOperator>(std::move(sourceDescriptor), duplicateSourceOperator->getId());
            duplicateSourceOperator->replace(operatorSourceLogicalDescriptor, duplicateSourceOperator);
        }
    }

    NES_DEBUG("LogicalSourceExpansionRule: Plan after\n{}", queryPlan->toString());
    return queryPlan;
}

void LogicalSourceExpansionRule::removeConnectedBlockingOperators(const std::shared_ptr<Node>& operatorNode)
{
    /// Check if downstream (parent) operator of this operator is blocking or not if not then recursively call this method for the
    /// downstream operator
    auto downStreamOperators = operatorNode->getParents();
    NES_TRACE("LogicalSourceExpansionRule: For each parent look if their ancestor has a n-ary operator or a sink operator.");
    for (const auto& downStreamOperator : downStreamOperators)
    {
        /// Check if the downStreamOperator operator is a blocking operator or not
        if (!isBlockingOperator(downStreamOperator))
        {
            removeConnectedBlockingOperators(downStreamOperator);
        }
        else
        {
            /// If downStreamOperator is blocking then remove current operator as its upstream operator.
            const auto success = downStreamOperator->removeChild(operatorNode);
            INVARIANT(success, "Unable to remove non-blocking upstream operator from the blocking operator");

            /// Add to the current operator information about operator id of the removed downStreamOperator.
            /// We will use this information post expansion to re-add the connection later.
            addBlockingDownStreamOperator(operatorNode, NES::Util::as_if<Operator>(downStreamOperator)->getId());
        }
    }
}

void LogicalSourceExpansionRule::addBlockingDownStreamOperator(const std::shared_ptr<Node>& operatorNode, OperatorId downStreamOperatorId)
{
    /// extract the list of connected blocking parents and add the current parent to the list
    const std::any value = NES::Util::as_if<Operator>(operatorNode)->getProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS);
    if (value.has_value())
    { /// update the existing list
        auto listOfConnectedBlockingParents = std::any_cast<std::vector<OperatorId>>(value);
        listOfConnectedBlockingParents.emplace_back(downStreamOperatorId);
        NES::Util::as_if<Operator>(operatorNode)->addProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS, listOfConnectedBlockingParents);
    }
    else
    { /// create a new entry if value doesn't exist
        std::vector<OperatorId> listOfConnectedBlockingParents;
        listOfConnectedBlockingParents.emplace_back(downStreamOperatorId);
        NES::Util::as_if<Operator>(operatorNode)->addProperty(LIST_OF_BLOCKING_DOWNSTREAM_OPERATOR_IDS, listOfConnectedBlockingParents);
    }
}

bool LogicalSourceExpansionRule::isBlockingOperator(const std::shared_ptr<Node>& operatorNode)
{
    return (
        NES::Util::instanceOf<SinkLogicalOperator>(operatorNode) || NES::Util::instanceOf<LogicalWindowOperator>(operatorNode)
        || NES::Util::instanceOf<LogicalUnionOperator>(operatorNode) || NES::Util::instanceOf<LogicalJoinOperator>(operatorNode));
}

}
