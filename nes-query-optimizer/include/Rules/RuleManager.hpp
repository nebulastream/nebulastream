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
#include <ErrorHandling.hpp>

namespace NES
{

template <typename U>
class RuleManager
{
public:
    void addRule(Rule<U> rule)
    {
        if (rules.contains(rule.getType()))
        {
            throw UnknownException("Only supports one rule per type currently.");
        }
        for (auto deps: rule.dependsOn())
        {
                dependencies[deps].insert(rule.getType());
        }

        for (auto requiredBy: rule.requiredBy())
        {
            dependencies[rule.getType()].insert(requiredBy);
        }

        rules.emplace(rule.getType(), rule);
    }

    [[nodiscard]] U apply(U unit) const
    {
       const auto sequence = getSequence();

        for (const auto& rule: sequence)
        {
            unit = rule.apply(unit);
        }
        return unit;
    }


    [[nodiscard]] std::vector<Rule<U>> getSequence() const
    {
        std::queue<Rule<U>> candidates;
        std::unordered_map<std::type_index, size_t> indegree;

        std::vector<Rule<U>> sequence;

        for (auto [type, rule]: rules)
        {
            indegree[type] = 0;
        }

        for (const auto& requiredBy : dependencies | std::views::values)
        {
            for (auto type : requiredBy)
            {
                if (!rules.contains(type))
                {
                    throw UnknownException("Unregistered Rule in RuleManager dependency graph: ", type.name());
                }
                indegree[type]++;
            }
        }

        for (auto [type, numberOfRulesItDependsOn]: indegree)
        {
            if (numberOfRulesItDependsOn == 0)
            {
                candidates.push(rules.at(type));
            }
        }

        while (!candidates.empty())
        {
            // 1. Choose any candidate and remove it from list.
            auto next = candidates.front();
            candidates.pop();
            // 2. Add candidate to sequence.
            sequence.emplace_back(next);
            // 3. Reduce indegree of all rules that depend on it by one
            if (dependencies.contains(next.getType()))
            {
                auto rulesThatDependsOnNext = dependencies.at(next.getType());
                for (auto dependentRuleType: rulesThatDependsOnNext)
                {
                    indegree[dependentRuleType] -= 1;
                    if (indegree[dependentRuleType] == 0)
                    {
                        // 4. If indegree of any rule becomes zero, add it to candidates.
                        candidates.push(rules.at(dependentRuleType));
                    }
                }
            }
        }
        if (sequence.size() != rules.size())
        {
            throw UnknownException("Cycle detected in rule dependencies.");
        }
        return sequence;
    }

private:
    std::unordered_map<std::type_index, std::set<std::type_index>> dependencies;
    std::unordered_map<std::type_index, Rule<U>> rules;
};


using PlanRuleManager = RuleManager<LogicalPlan>;
}

