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
#include <Rules/Semantic/InlineSourceBindingRule.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigResolution.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

const std::type_info& InlineSourceBindingRule::getType()
{
    return typeid(InlineSourceBindingRule);
}

std::string_view InlineSourceBindingRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> InlineSourceBindingRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> InlineSourceBindingRule::requiredBy() const
{
    return {};
};

bool InlineSourceBindingRule::operator==(const InlineSourceBindingRule& other) const
{
    return sourceCatalog == other.sourceCatalog;
}

LogicalOperator InlineSourceBindingRule::bindInlineSourceLogicalOperators(const LogicalOperator& current) const
{
    std::vector<LogicalOperator> newChildren;
    for (const auto& child : current.getChildren())
    {
        newChildren.emplace_back(bindInlineSourceLogicalOperators(child));
    }

    if (const auto inlineSource = current.tryGetAs<InlineSourceLogicalOperator>())
    {
        PRECONDITION(std::ranges::empty(inlineSource->getChildren()), "Inline source operator must have no children");
        const auto type = inlineSource.value()->getSourceType();
        const auto schema = inlineSource.value()->getSourceSchema();
        const auto parserConfig = inlineSource.value()->getParserConfig();
        auto sourceConfig = inlineSource.value()->getSourceConfig();

        /// "host" is not part of the source config — it determines placement, not source behavior.
        /// It is stored in the config only because InlineSourceLogicalOperator lacks a dedicated host field.
        const auto hostName = QualifiedIdentifier::create(Identifier::parse("host"));
        const auto hostValue = sourceConfig.getFieldByName(hostName);
        if (not hostValue.has_value())
        {
            throw InvalidConfigParameter("`host`");
        }
        const auto hostLiteral = hostValue->getValue();
        const auto* hostString = std::get_if<std::string>(&hostLiteral);
        if (hostString == nullptr)
        {
            throw InvalidConfigParameter("host must be a string literal");
        }
        auto host = Host(*hostString);

        /// Max inflight buffers is a descriptor-level field, not part of the source's own config.
        const auto maxInflightName = QualifiedIdentifier::create(Identifier::parse("MAX_INFLIGHT_BUFFERS"));
        std::optional<size_t> maxInflightBuffers;
        if (const auto maxInflightValue = sourceConfig.getFieldByName(maxInflightName))
        {
            const auto maxInflightLiteral = maxInflightValue->getValue();
            const auto* maxInflight = std::get_if<int64_t>(&maxInflightLiteral);
            if (maxInflight == nullptr || *maxInflight <= 0)
            {
                throw InvalidConfigParameter("MAX_INFLIGHT_BUFFERS must be a positive integer literal");
            }
            maxInflightBuffers = static_cast<size_t>(*maxInflight);
        }

        sourceConfig = Schema<LiteralConfigValue, Ordered>{
            sourceConfig
            | std::views::filter(
                [&](const auto& value)
                { return value.getFullyQualifiedName() != hostName && value.getFullyQualifiedName() != maxInflightName; })
            | std::ranges::to<std::vector>()};

        const auto descriptorOpt = sourceCatalog->getInlineSource(type, schema, host, maxInflightBuffers, parserConfig, sourceConfig);

        if (!descriptorOpt.has_value())
        {
            throw InvalidConfigParameter("Could not create an inline source descriptor because of invalid config parameters");
        }
        const auto& descriptor = descriptorOpt.value();
        return SourceDescriptorLogicalOperator::create(descriptor);
    }

    return current.withChildrenUnsafe(newChildren);
}

LogicalPlan InlineSourceBindingRule::apply(const LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& root : queryPlan.getRootOperators())
    {
        newRoots.emplace_back(bindInlineSourceLogicalOperators(root));
    }
    return queryPlan.withRootOperators(newRoots);
}

}
