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

#include <string>
#include <vector>
#include <unordered_set>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <LegacyOptimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/QueryPlan.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <ErrorHandling.hpp>

namespace NES::LegacyOptimizer
{

void LogicalSourceExpansionRule::apply(QueryPlan& queryPlan, Catalogs::Source::SourceCatalog& sourceCatalog)
{
    auto sourceOperators = queryPlan.getOperatorByType<SourceNameLogicalOperator>();

    for (SourceNameLogicalOperator sourceOp : sourceOperators)
    {
        std::string logicalSourceName(sourceOp.getName());
        auto sourceCatalogEntries = sourceCatalog.getPhysicalSources(logicalSourceName);
        if (sourceCatalogEntries.empty())
        {
            auto ex = PhysicalSourceNotFoundInQueryDescription();
            ex.what() += "LogicalSourceExpansionRule: Unable to find physical source locations for the logical source " + logicalSourceName;
            throw ex;
        }
        for (const auto& sourceCatalogEntry : sourceCatalogEntries)
        {
            auto sourceDescriptor = sourceCatalogEntry->getPhysicalSource()->createSourceDescriptor(sourceOp.getSchema());
            auto logicalDescOp = SourceDescriptorLogicalOperator(std::move(sourceDescriptor));
            queryPlan.replaceOperator(sourceOp, std::move(logicalDescOp));
        }
    }
}

}
