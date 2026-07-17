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

    if (const auto inlineSourceOpt = current.tryGetAs<InlineSourceLogicalOperator>())
    {
        const auto& inlineSource = inlineSourceOpt.value();
        PRECONDITION(std::ranges::empty(inlineSource->getChildren()), "Inline source operator must have no children");
        auto descriptorExp
            = PhysicalSourceBuilder{inlineSource->getGeneralSourceConfig(), inlineSource->getPluginSourceConfig(), inlineSource->getInputFormatterDescriptor(), this->sourceCatalog}
                  .build(inlineSource->getSourceSchema());

        if (!descriptorExp.has_value())
        {
            throw InvalidConfigParameter("Could not create an inline source descriptor because of invalid config parameters");
        }
        return SourceDescriptorLogicalOperator::create(std::move(descriptorExp).value());
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
