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
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <LegacyOptimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/QueryPlan.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <Iterators/DFSIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::LegacyOptimizer
{

LogicalSourceExpansionRule::LogicalSourceExpansionRule(
    const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog)
    : sourceCatalog(sourceCatalog)
{
}

std::shared_ptr<LogicalSourceExpansionRule>
LogicalSourceExpansionRule::create(const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog)
{
    return std::make_shared<LogicalSourceExpansionRule>(LogicalSourceExpansionRule(sourceCatalog));
}


template <typename LogicalSourceType>
std::vector<std::shared_ptr<LogicalSourceType>> getSourceOperators(const std::shared_ptr<QueryPlan>& plan)
{
    const auto& sources = plan->getOperatorByType<LogicalSourceType>();
    /// deduplicate
    std::unordered_set<std::shared_ptr<LogicalSourceType>> sourceOperatorsSet(sources.begin(), sources.end());
    std::vector<std::shared_ptr<LogicalSourceType>> sourceOperators{sourceOperatorsSet.begin(), sourceOperatorsSet.end()};
    return sourceOperators;
}


bool removeChild(const std::shared_ptr<Operator> toRemoveFrom, const std::shared_ptr<Operator>& toRemove)
{
    if (!toRemove)
    {
        return false;
    }
    for (auto nodeItr = toRemoveFrom->children.begin(); nodeItr != toRemoveFrom->children.end(); ++nodeItr)
    {
        if ((*nodeItr).get() == toRemove.get())
        {
            for (auto it = (*nodeItr)->parents.begin(); it != (*nodeItr)->parents.end(); it++)
            {
                if (it->get() == toRemoveFrom.get())
                {
                    (*nodeItr)->parents.erase(it);
                    break;
                }
            }
            toRemoveFrom->children.erase(nodeItr);
            return true;
        }
    }
    return false;
}

std::vector<std::shared_ptr<Operator>> getAndFlattenAllAncestors(std::shared_ptr<Operator> op)
{
    std::vector<std::shared_ptr<Operator>> result{op};
    for (auto& parent : op->parents)
    {
        std::vector<std::shared_ptr<Operator>> parentAndAncestors = getAndFlattenAllAncestors(parent);
        result.insert(result.end(), parentAndAncestors.begin(), parentAndAncestors.end());
    }
    return result;
}


bool removeParent(const std::shared_ptr<Operator> op, const std::shared_ptr<Operator>& node)
{
    if (!node)
    {
        return false;
    }
    for (auto nodeItr = op->parents.begin(); nodeItr != op->parents.end(); ++nodeItr)
    {
        if ((*nodeItr).get() == node.get())
        {
            for (auto it = (*nodeItr)->children.begin(); it != (*nodeItr)->children.end(); it++)
            {
                if ((*it) == op)
                {
                    (*nodeItr)->children.erase(it);
                    break;
                }
            }
            op->parents.erase(nodeItr);
            return true;
        }
    }
    return false;
}

std::shared_ptr<Operator> find(const std::vector<std::shared_ptr<Operator>>& nodes, const std::shared_ptr<Operator>& nodeToFind)
{
    for (auto&& currentNode : nodes)
    {
        if (nodeToFind == currentNode)
        {
            return currentNode;
        }
    }
    return nullptr;
}

bool insertBetweenThisAndParentNodes(const std::shared_ptr<Operator> op, const std::shared_ptr<Operator>& newNode)
{
    ///Perform sanity checks
    if (newNode == op)
    {
        return false;
    }

    if (find(op->parents, newNode) != nullptr)
    {
        NES_WARNING("Node: the node is already part of its parents so ignore insertBetweenThisAndParentNodes operation.");
        return false;
    }

    ///replace this with the new node in all its parent
    const std::vector<std::shared_ptr<Operator>> copyOfParents = op->parents;

    for (auto& parent : copyOfParents)
    {
        for (uint64_t i = 0; i < parent->children.size(); i++)
        {
            if (parent->children[i] == op)
            {
                parent->children[i] = newNode;
                newNode->parents.emplace_back(parent);
            }
        }
    }

    for (auto it = op->parents.begin(); it != op->parents.end();)
    {
        if (removeParent(op, *it))
        {
            it = op->parents.begin();
        }
        else
        {
            ++it;
        }
    }

    op->parents.emplace_back(newNode);
    return true;
}

bool removeAndJoinParentAndChildren(const std::shared_ptr<Operator> op)
{
    const std::vector<std::shared_ptr<Operator>> childCopy = op->children;
    const std::vector<std::shared_ptr<Operator>> parentCopy = op->parents;
    for (auto& parent : parentCopy)
    {
        for (auto& child : childCopy)
        {
            parent->children.emplace_back(child);
            removeParent(child, op);
        }
        removeChild(parent, op);
    }
    return true;
}

bool replace(const std::shared_ptr<Operator>& op, const std::shared_ptr<Operator>& newNode, const std::shared_ptr<Operator>& oldNode)
{
    if (!newNode || !oldNode)
    {
        NES_ERROR("Node: Can't replace null node");
        return false;
    }

    if (op == oldNode)
    {
        insertBetweenThisAndParentNodes(op, newNode);
        removeAndJoinParentAndChildren(op);
        return true;
    }

    if (oldNode != newNode)
    {
        /// newNode is already inside children or parents and it's not oldNode
        if (find(op->children, newNode) || find(op->parents, newNode))
        {
            return false;
        }
    }

    bool success = removeChild(op, oldNode);
    if (success)
    {
        op->children.push_back(newNode);
        for (auto&& currentNode : oldNode->children)
        {
            newNode->children.emplace_back(currentNode);
        }
        return true;
    }

    op->parents.emplace_back(oldNode);
    for (auto&& currentNode : oldNode->parents)
    {
        newNode->parents.emplace_back(currentNode);
    }
    return true;
}

std::shared_ptr<QueryPlan> LogicalSourceExpansionRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    std::vector<std::shared_ptr<SourceNameLogicalOperator>> sourceOperators =
        queryPlan->getOperatorByType<NES::SourceNameLogicalOperator>();

    /// Compute a map of all blocking operators in the query plan
    std::unordered_map<OperatorId, std::shared_ptr<Operator>> blockingOperators;
    /// Add downstream operators of the source operators as blocking operators.
    for (const auto& sourceOperator : sourceOperators)
    {
        for (auto& downStreamOp : sourceOperator->parents)
        {
            blockingOperators[NES::Util::as<Operator>(downStreamOp)->id] = NES::Util::as<Operator>(downStreamOp);
        }
    }

    /// Iterate over all source operators
    for (const auto& sourceOperator : sourceOperators)
    {
        std::string logicalSourceName = std::string(sourceOperator->getName());

        const std::vector<std::shared_ptr<Catalogs::Source::SourceCatalogEntry>> sourceCatalogEntries =
            sourceCatalog->getPhysicalSources(logicalSourceName);
        if (sourceCatalogEntries.empty())
        {
            auto ex = PhysicalSourceNotFoundInQueryDescription();
            ex.what() += "LogicalSourceExpansionRule: Unable to find physical source locations for the logical source " + logicalSourceName;
            throw ex;
        }

        /// Disconnect all parent operators of the source operator.
        for (const auto& downStreamOperator : sourceOperator->parents)
        {
            /// If downStreamOperator is blocking then remove the source operator as its upstream operator.
            auto success = removeChild(downStreamOperator, sourceOperator);
            INVARIANT(success, "Unable to remove non-blocking upstream operator from the blocking operator");
            // Previously, we added blocking operator info as a property.
            // That call is removed because properties no longer exist.
        }

        /// Create one duplicate operator for each physical source.
        for (const auto& sourceCatalogEntry : sourceCatalogEntries)
        {
            auto duplicateSourceOperator = NES::Util::as<SourceNameLogicalOperator>(sourceOperator->clone());

            // Set the pinned worker id via a dedicated setter instead of using a property.
            duplicateSourceOperator->setSchema(sourceOperator->getSchema());

            /// Flatten the graph to duplicate and update operator IDs.
            const std::vector<std::shared_ptr<Operator>>& allOperators = getAndFlattenAllAncestors(duplicateSourceOperator);

            std::unordered_set<OperatorId> visitedOperators;
            for (const auto& node : allOperators)
            {
                auto operatorNode = NES::Util::as<Operator>(node);
                // Simply assign a new operator id and mark it as visited.
                ///operatorNode->setId(getNextOperatorId());
                visitedOperators.insert(operatorNode->id);
            }

            auto sourceDescriptor = sourceCatalogEntry->getPhysicalSource()->createSourceDescriptor(duplicateSourceOperator->getSchema());
            auto operatorSourceLogicalDescriptor = std::make_shared<SourceDescriptorLogicalOperator>(std::move(sourceDescriptor));
            replace(duplicateSourceOperator, operatorSourceLogicalDescriptor, duplicateSourceOperator);
        }
    }

    NES_DEBUG("LogicalSourceExpansionRule: Plan after\n{}", queryPlan->toString());
    return queryPlan;
}

}
