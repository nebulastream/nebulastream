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


bool removeChild(Operator* parent, Operator* childToRemove)
{
    if (!childToRemove)
    {
        return false;
    }
    for (auto it = parent->children.begin(); it != parent->children.end(); ++it)
    {
        if (it->get() == childToRemove)
        {
            // Remove the parent's pointer from the child's parent list.
            for (auto pit = childToRemove->parents.begin(); pit != childToRemove->parents.end(); ++pit)
            {
                if (*pit == parent)
                {
                    childToRemove->parents.erase(pit);
                    break;
                }
            }
            parent->children.erase(it);
            return true;
        }
    }
    return false;
}

std::vector<Operator*> getAndFlattenAllAncestors(Operator* op)
{
    std::vector<Operator*> result{op};
    for (Operator* parent : op->parents)
    {
        auto ancestors = getAndFlattenAllAncestors(parent);
        result.insert(result.end(), ancestors.begin(), ancestors.end());
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
        if ((*nodeItr) == node.get())
        {
            for (auto it = (*nodeItr)->children.begin(); it != (*nodeItr)->children.end(); it++)
            {
                if ((*it).get() == op.get())
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

QueryPlan LogicalSourceExpansionRule::apply(QueryPlan queryPlan)
{
    // Suppose getOperatorByType now returns a vector of raw pointers.
    std::vector<SourceNameLogicalOperator*> sourceOperators =
        queryPlan.getOperatorByType<SourceNameLogicalOperator>();

    for (SourceNameLogicalOperator* sourceOp : sourceOperators)
    {
        std::string logicalSourceName = std::string(sourceOp->getName());
        auto sourceCatalogEntries = sourceCatalog->getPhysicalSources(logicalSourceName);
        if (sourceCatalogEntries.empty())
        {
            auto ex = PhysicalSourceNotFoundInQueryDescription();
            ex.what() += "LogicalSourceExpansionRule: Unable to find physical source locations for the logical source " + logicalSourceName;
            throw ex;
        }

        // Disconnect all parent operators from this source operator.
        for (Operator* parent : sourceOp->parents)
        {
            bool success = removeChild(parent, sourceOp);
            INVARIANT(success, "Unable to remove non-blocking upstream operator from the blocking operator");
        }

        // Duplicate the operator for each physical source.
        for (const auto& sourceCatalogEntry : sourceCatalogEntries)
        {
            std::unique_ptr<Operator> duplicateSourceOp = sourceOp->clone();

            auto allNodes = getAndFlattenAllAncestors(duplicateSourceOp.get());
            std::unordered_set<OperatorId> visited;
            for (Operator* node : allNodes)
            {
                visited.insert(node->id);
            }

            auto sourceDescriptor = sourceCatalogEntry->getPhysicalSource()->createSourceDescriptor(sourceOp->getSchema());
            auto logicalDescOp = std::make_unique<SourceDescriptorLogicalOperator>(*sourceDescriptor);
            /// TODO replace(duplicateSourceOp.get(), std::move(logicalDescOp), duplicateSourceOp.get());
        }
    }

    NES_DEBUG("LogicalSourceExpansionRule: Plan after\n{}", queryPlan.toString());
    return queryPlan;
}

}
