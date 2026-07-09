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

#include <Rules/Semantic/UDFResolutionRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Functions/LogicalFunction.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/InlineSinkBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <ErrorHandling.hpp>
#include <UdfCatalog.hpp>

namespace NES
{

namespace
{
/// Recursively rewrite a function tree, resolving any UDFCall placeholder against the catalog.
LogicalFunction resolveUdfCalls(const LogicalFunction& function, const UdfCatalog& udfCatalog)
{
    auto newChildren = function.getChildren() | std::views::transform([&](const auto& child) { return resolveUdfCalls(child, udfCatalog); })
        | std::ranges::to<std::vector>();

    if (const auto udfCall = function.tryGetAs<UDFCallLogicalFunction>())
    {
        const auto& udfName = udfCall->get().getUdfName();
        if (!udfCatalog.hasUdf(udfName))
        {
            throw UnknownUdf("UDF '{}' is not registered", udfName);
        }
        return LogicalFunction{UDFCallLogicalFunction{udfName, udfCatalog.load(udfName), std::move(newChildren)}};
    }
    return function.withChildren(newChildren);
}

LogicalOperator recur(const LogicalOperator& op, const UdfCatalog& udfCatalog)
{
    auto newChildren = op.getChildren() | std::views::transform([&](const auto& child) { return recur(child, udfCatalog); })
        | std::ranges::to<std::vector>();

    /// Match the concrete operator type on the original `op` (as InferModelResolutionRule does);
    /// op.withChildren() re-wraps and loses the type for tryGetAs. Rebuild with the resolved children.
    if (const auto selection = op.tryGetAs<SelectionLogicalOperator>(); selection.has_value() && !newChildren.empty())
    {
        auto predicate = resolveUdfCalls(selection->get().getPredicate(), udfCatalog);
        return SelectionLogicalOperator::create(newChildren.front(), std::move(predicate))
            .get()
            .withTraitSet(selection->get().getTraitSet());
    }
    if (const auto projection = op.tryGetAs<ProjectionLogicalOperator>(); projection.has_value() && !newChildren.empty())
    {
        std::vector<ProjectionLogicalOperator::UnboundProjection> newProjections;
        for (const auto& [identifier, function] : projection->get().getUnboundProjections())
        {
            newProjections.emplace_back(identifier, resolveUdfCalls(function, udfCatalog));
        }
        return ProjectionLogicalOperator::create(
                   newChildren.front(), std::move(newProjections), ProjectionLogicalOperator::Asterisk{projection->get().hasAsterisk()})
            .get()
            .withTraitSet(projection->get().getTraitSet());
    }
    return op.withChildren(std::move(newChildren));
}
}

LogicalPlan UDFResolutionRule::apply(const LogicalPlan& queryPlan) const
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Query plan must have exactly one root operator");
    return queryPlan.withRootOperators({recur(queryPlan.getRootOperators().front(), *udfCatalog)});
}

const std::type_info& UDFResolutionRule::getType()
{
    return typeid(UDFResolutionRule);
}

std::string_view UDFResolutionRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> UDFResolutionRule::dependsOn() const
{
    return {typeid(LogicalSourceExpansionRule), typeid(SinkBindingRule), typeid(InlineSinkBindingRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> UDFResolutionRule::requiredBy() const
{
    return {typeid(TypeInferenceRule)};
}

bool UDFResolutionRule::operator==(const UDFResolutionRule& other) const
{
    return udfCatalog == other.udfCatalog;
}

}
