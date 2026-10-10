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
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// Marks the projection that contains an `HTTP_CALL(...)` for asynchronous execution.
///
/// `AsyncExecutionTrait` is the only way into the asynchronous framework, and nothing attaches it
/// generically -- an operator that wants to run there needs a rule that recognises it. This is that
/// rule for `HTTP_CALL`, and it is a plugin on purpose: together with the function and the executor
/// it shows that an operator can be put on the framework without touching nes-sql-parser,
/// nes-logical-operators or nes-query-optimizer at all.
///
/// Having marked the projection, `AsyncOperatorSplitter` replaces it with a handoff sink and an
/// asynchronous source, so the projection never reaches lowering. That is why the rule is strict
/// about the shape it accepts: whatever else that projection computed would silently disappear.
/// Accepted is one `HTTP_CALL` whose result is assigned to a new field, alongside fields that are
/// passed through under their own name.
class HttpCallAsyncRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "HttpCallAsyncRule";

    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
};

static_assert(RuleConcept<HttpCallAsyncRule, LogicalPlan>);

}
