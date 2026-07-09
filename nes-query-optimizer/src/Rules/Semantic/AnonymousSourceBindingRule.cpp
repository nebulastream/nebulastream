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
#include <Rules/Semantic/AnonymousSourceBindingRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

LogicalOperator AnonymousSourceBindingRule::bindAnonymousSourceLogicalOperators(const LogicalOperator& current) const
{
    std::vector<LogicalOperator> newChildren;
    for (const auto& child : current.getChildren())
    {
        newChildren.emplace_back(bindAnonymousSourceLogicalOperators(child));
    }

    if (const auto anonymousSource = current.tryGetAs<AnonymousSourceLogicalOperator>())
    {
        PRECONDITION(std::ranges::empty(anonymousSource->getChildren()), "Anonymous source operator must have no children");
        const auto type = anonymousSource.value()->getSourceType();
        const auto schema = anonymousSource.value()->getSourceSchema();
        const auto parserConfig = anonymousSource.value()->getParserConfig();
        auto sourceConfig = anonymousSource.value()->getSourceConfig();

        /// "host" is not part of the source config — it determines placement, not source behavior.
        /// It is stored in the config map only because AnonymousSourceLogicalOperator lacks a dedicated host field.
        auto hostIt = sourceConfig.find(Identifier::parse("host"));
        if (hostIt == sourceConfig.end())
        {
            throw InvalidConfigParameter("`host`");
        }
        auto host = Host(hostIt->second);
        sourceConfig.erase(hostIt);

        const auto descriptorOpt = sourceCatalog->getAnonymousSource(type, schema, host, parserConfig, sourceConfig);

        if (!descriptorOpt.has_value())
        {
            throw InvalidConfigParameter("Could not create an anonymous source descriptor because of invalid config parameters");
        }
        const auto& descriptor = descriptorOpt.value();
        return SourceDescriptorLogicalOperator::create(descriptor);
    }

    return current.withChildrenUnsafe(newChildren);
}

LogicalPlan AnonymousSourceBindingRule::apply(const LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& root : queryPlan.getRootOperators())
    {
        newRoots.emplace_back(bindAnonymousSourceLogicalOperators(root));
    }
    return queryPlan.withRootOperators(newRoots);
}

}
