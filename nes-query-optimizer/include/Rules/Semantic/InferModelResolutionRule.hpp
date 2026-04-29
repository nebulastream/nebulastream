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

#pragma once

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>

namespace NES {
    struct PlannerContext;
}

namespace NES
{

/// Resolves InferModelNameLogicalOperator nodes to InferModelLogicalOperator nodes
/// by loading the model from the ModelCatalog.
class InferModelResolutionRule
{
public:
    explicit InferModelResolutionRule(const PlannerContext &ctx) : ctx(ctx) {
    }

    static constexpr std::string_view NAME = "InferModelResolutionRule";

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;
    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    bool operator==(const InferModelResolutionRule& other) const;

private:
    const PlannerContext &ctx;
};

static_assert(RuleConcept<InferModelResolutionRule, LogicalPlan>);
}
