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
#include <ranges>
#include <utility>
#include <vector>

#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Optimizer
{

LogicalSourceExpansionRule::LogicalSourceExpansionRule(std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog)
    : sourceCatalog(std::move(sourceCatalog))
{
}

std::shared_ptr<QueryPlan> LogicalSourceExpansionRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    NES_DEBUG("LogicalSourceExpansionRule: Plan before\n{}", queryPlan->toString());
    const std::vector<std::shared_ptr<SourceNameLogicalOperator>> sourceOperators
        = queryPlan->getSourceOperators<SourceNameLogicalOperator>();

    /// Iterate over all source operators
    for (const auto& sourceOperator : sourceOperators)
    {
        NES_TRACE("LogicalSourceExpansionRule: Get the number of physical source locations in the topology.");
        const auto sourceName = sourceOperator->getLogicalSourceName();
        const auto sourceSchema = sourceOperator->getSchema();
        const auto sourceCatalogEntries = sourceCatalog->getPhysicalSources(sourceName);
        /// Check that there is at least one source catalog entry for the name of the logical source.
        if (sourceCatalogEntries.empty())
        {
            throw PhysicalSourceNotFoundInQueryDescription(
                "LogicalSourceExpansionRule: Unable to find physical source locations for the logical source: {}", sourceName);
        }

        /// Replace the SourceNameLogicalOperator with the SourceDescriptorLogicalOperator corresponding to the first entry.
        auto firstSourceDescriptor = sourceCatalogEntries.front()->getPhysicalSource()->createSourceDescriptor(sourceOperator->getSchema());
        auto firstOperatorSourceLogicalDescriptor
            = std::make_shared<SourceDescriptorLogicalOperator>(std::move(firstSourceDescriptor), sourceOperator->getId());
        sourceOperator->replace(firstOperatorSourceLogicalDescriptor, sourceOperator);

        /// Iterate over all subsequent entries, create the corresponding SourceDescriptorLogicalOperators and add them to the query plan.
        for (const auto& sourceCatalogEntry : sourceCatalogEntries | std::views::drop(1))
        {
            auto sourceDescriptor = sourceCatalogEntry->getPhysicalSource()->createSourceDescriptor(sourceSchema);
            auto operatorSourceLogicalDescriptor
                = std::make_shared<SourceDescriptorLogicalOperator>(std::move(sourceDescriptor), getNextOperatorId());
            /// Add the OperatorSourceLogicalDescriptor to the query plan, by adding it as a child to the parent operator(s).
            for (const auto& parentNode : firstOperatorSourceLogicalDescriptor->getParents())
            {
                parentNode->addChild(operatorSourceLogicalDescriptor);
            }
        }
    }

    NES_DEBUG("LogicalSourceExpansionRule: Plan after\n{}", queryPlan->toString());
    return queryPlan;
}

}
