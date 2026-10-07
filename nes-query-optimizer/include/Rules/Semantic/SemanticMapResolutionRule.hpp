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

#include <memory>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <PlanRuleRegistry.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

/// Resolves SemanticMapNameLogicalOperator nodes to SemanticMapLogicalOperator nodes, and
/// SemanticFilterNameLogicalOperator nodes to SemanticFilterLogicalOperator nodes, by loading the
/// model from the SemanticModelCatalog. A model is either a map model (with OUTPUT) or a filter
/// model (without); using it with the other operator throws InvalidSemanticModel.
///
/// Unlike InferModelResolutionRule this rule never infers a schema. It only walks the spine from
/// a resolved operator up to the root and relinks it with withChildrenUnsafe; TypeInferenceRule,
/// which runs after every resolution rule, infers the whole plan afterwards. Any eager inference
/// here would reach into subtrees that other resolution rules have not processed yet (an
/// unresolved MODEL_INFERENCE below or next to a SEM_MAP), whose schema accessors are guarded by
/// PRECONDITION and would take the whole process down.
class SemanticMapResolutionRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);

    explicit SemanticMapResolutionRule(std::shared_ptr<const SemanticModelCatalog> semanticModelCatalog)
        : semanticModelCatalog(std::move(semanticModelCatalog))
    {
    }

    static constexpr std::string_view NAME = "SemanticMapResolutionRule";

    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;

private:
    std::shared_ptr<const SemanticModelCatalog> semanticModelCatalog;
};

static_assert(RuleConcept<SemanticMapResolutionRule, LogicalPlan>);
}
