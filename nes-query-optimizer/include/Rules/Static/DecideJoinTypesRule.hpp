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
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/PlanRule.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

/// Decides what join implementation should be used. For now, we support HashJoin or a NestedLoopJoin
class DecideJoinTypesRule final : public PlanRule
{
public:
    explicit DecideJoinTypesRule(const StreamJoinStrategy joinStrategy) : joinStrategy(joinStrategy) { }

    static constexpr std::string_view NAME = "DecideJoinTypesRule";

    [[nodiscard]] const std::type_info& getType() const override;
    [[nodiscard]] std::string_view getName() const override;
    [[nodiscard]] std::set<std::type_index> dependsOn() const override;
    [[nodiscard]] std::set<std::type_index> requiredBy() const override;
    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const override;
    [[nodiscard]] bool equals(const Rule& other) const override;

private:
    [[nodiscard]] LogicalOperator apply(const LogicalOperator& logicalOperator) const;
    StreamJoinStrategy joinStrategy;
};
}
