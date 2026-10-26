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
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <PlanRuleRegistry.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

/// Operator fusion for semantic operators, after the Python reference's `Coordinator._apply_fusion`:
/// a SEM_MAP or SEM_FILTER directly on top of another one becomes a single operator whose model
/// answers both step lists in one prompt — one LLM round trip per batch instead of two.
///
/// A parent is fused into its child when
///   - both models opt in with 'TRUE' AS LLM.FUSION (the reference's `fusion=False` default keeps the
///     measured unfused baselines reproducible),
///   - the child feeds nothing else,
///   - both declare the same INPUT fields in the same order (the reference compares `depends_on`),
///   - both talk to the same model the same way: endpoint, model name, backend, API key variable,
///     payload format, dataset prompt, execution, batch size, concurrency, retries, wait time,
///     timeout and ordering,
///   - and their OUTPUT fields do not collide.
/// The fused model is `fuseSemanticModels(child, parent)`; it runs as SEM_FILTER if any step filters
/// and as SEM_MAP otherwise, and gets a fresh async marker built from the fused model. Fusion works
/// bottom-up, so a chain of fusable operators collapses into one.
///
/// Like SemanticMapResolutionRule it never infers a schema: it relinks the spine with
/// withChildrenUnsafe and leaves inference to TypeInferenceRule.
class SemanticFusionRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);

    static constexpr std::string_view NAME = "SemanticFusionRule";

    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;

    /// Whether `parent` may be fused into `child` (fusion opt-in, inputs, transport, collisions).
    /// The child-has-one-parent condition is the plan's, not the models'; `apply` checks it.
    [[nodiscard]] static bool canFuse(const RegisteredSemanticModel& child, const RegisteredSemanticModel& parent);
};

static_assert(RuleConcept<SemanticFusionRule, LogicalPlan>);
}
