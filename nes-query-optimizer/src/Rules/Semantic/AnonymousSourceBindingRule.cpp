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
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

LogicalOperator
AnonymousSourceBindingRule::bindAnonymousSources(const LogicalOperator& op, const std::vector<LogicalOperator>& children) const
{
    if (const auto anonymousSource = op.tryGetAs<AnonymousSourceLogicalOperator>())
    {
        PRECONDITION(children.empty(), "Anonymous source operator must have no children");
        const auto type = anonymousSource.value()->getSourceType();
        const auto schema = anonymousSource.value()->getSourceSchema();
        const auto parserConfig = anonymousSource.value()->getParserConfig();
        const auto sourceConfig = anonymousSource.value()->getSourceConfig();
        const auto descriptor = catalog->createAnonymousSource(ConnectorKind::Anonymous, type, schema, sourceConfig, parserConfig);
        return SourceDescriptorLogicalOperator::create(descriptor);
    }

    if (op.tryGetAs<SourceNameLogicalOperator>())
    {
        return op;
    }

    return op.withChildrenUnsafe(children);
}

LogicalPlan AnonymousSourceBindingRule::apply(const LogicalPlan& queryPlan) const
{
    PlanVisitor<> visitor{[this](const LogicalOperator& op, const std::vector<LogicalOperator>& children)
                          { return this->bindAnonymousSources(op, children); }};

    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AnonymousSourceBindingRule::neededBy() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType AnonymousSourceBindingRule::create(PlanRuleRegistryArguments arguments)
{
    return AnonymousSourceBindingRule{arguments.catalog};
}

}
