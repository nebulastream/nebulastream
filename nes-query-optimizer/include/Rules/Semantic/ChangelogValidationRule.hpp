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

namespace NES
{

/// Rejects operators that cannot consume an updating stream, i.e. one whose schema carries the row kind field (see RowKind.hpp).
/// Selections, projections, unions, watermark assigners and sinks pass rows through and keep their row kind, so they accept updates.
/// An unbounded aggregation accepts updates if all its aggregations are SUM, COUNT or AVG, which can subtract a withdrawn row.
/// Joins and other windowed operators would treat a retraction like a new row, so they are rejected.
class ChangelogValidationRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "ChangelogValidationRule";

    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;

    /// @throws UnsupportedQuery if an operator that cannot consume updates has an updating input
    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
};

static_assert(RuleConcept<ChangelogValidationRule, LogicalPlan>);
}
