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
#include <LegacyOptimizer/InlineSourceBindingPhase.hpp>

#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

LogicalOperator InlineSourceBindingPhase::bindInlineSourceLogicalOperators(const LogicalOperator& current) const
{
    std::vector<LogicalOperator> newChildren;
    for (const auto& child : current.getChildren())
    {
        newChildren.emplace_back(bindInlineSourceLogicalOperators(child));
    }

    if (const auto inlineSource = current.tryGetAs<InlineSourceLogicalOperator>())
    {
        const auto type = inlineSource.value()->getSourceType();
        const auto schema = inlineSource.value()->getSchema();
        const auto parserConfig = inlineSource.value()->getParserConfig();
        const auto sourceConfig = inlineSource.value()->getSourceConfig();

        const auto descriptorOpt = sourceCatalog->getInlineSource(type, schema, parserConfig, sourceConfig);

        if (!descriptorOpt.has_value())
        {
            throw InvalidConfigParameter("Could not create an inline source descriptor because of invalid config parameters");
        }
        const auto& descriptor = descriptorOpt.value();
        const SourceDescriptorLogicalOperator sourceDescriptorLogicalOperator{descriptor};
        return sourceDescriptorLogicalOperator.withChildren(newChildren);
    }

    return current.withChildren(newChildren);
}

void InlineSourceBindingPhase::apply(LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& root : queryPlan.getRootOperators())
    {
        newRoots.emplace_back(bindInlineSourceLogicalOperators(root));
    }
    queryPlan = queryPlan.withRootOperators(newRoots);
}

}
