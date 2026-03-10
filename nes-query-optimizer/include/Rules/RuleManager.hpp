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

#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>

namespace NES
{

template <typename U>
class RuleManager
{
public:
    void addRule(Rule<U> unit)
    {
        dependencies.emplace(unit, unit.getDependencies());
        rules.push_back(std::move(unit));
    }

    [[nodiscard]] U apply(U unit) const
    {
        for (const auto& rule : rules)
        {
            unit = rule.apply(unit);
        }
        return unit;
    }

private:
    std::unordered_map<Rule<U>, std::set<std::type_index>> dependencies;
    std::vector<Rule<U>> rules;
};


using PlanRuleManager = RuleManager<LogicalPlan>;
}

