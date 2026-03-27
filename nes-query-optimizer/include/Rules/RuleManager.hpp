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

#include <cstddef>
#include <queue>
#include <set>
#include <typeindex>
#include <unordered_map>
#include <vector>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <ErrorHandling.hpp>

namespace NES
{


/**
 * @brief Collects optimizer rules and produces a dependency-ordered execution sequence.
 *
 * Rules are registered via addRule(). Each rule may declare dependencies on other rule
 * types through Rule::dependsOn() and Rule::requiredBy(). The RuleManager builds a directed
 * acyclic graph (DAG) from these declarations and performs a topological sort when
 * getSequence() is called.
 *
 * Constraints:
 * - At most one rule per concrete type may be registered.
 *
 * @tparam U The plan type that rules operate on (e.g. LogicalPlan).
 */
template <typename U>
class RuleManager
{
public:
    void addRule(Rule<U> rule)
    {
        if (rules.contains(rule.getType()))
        {
            throw InvalidOptimizerRuleset("only supports one rule per type");
        }
        for (const auto& deps : rule.dependsOn())
        {
            directedEdges[deps].insert(rule.getType());
        }

        for (const auto& requiredBy : rule.requiredBy())
        {
            directedEdges[rule.getType()].insert(requiredBy);
        }

        rules.emplace(rule.getType(), rule);
    }

    /// Returns a topological ordering of all registered rules respecting their declared dependencies.
    /// Throws InvalidOptimizerRuleset if any dependency references an unregistered rule or if the graph contains a cycle.
    [[nodiscard]] std::vector<Rule<U>> getSequence() const
    {
        std::queue<Rule<U>> candidates;
        std::unordered_map<std::type_index, size_t> indegree;

        std::vector<Rule<U>> sequence;

        for (const auto& [type, rule] : rules)
        {
            indegree[type] = 0;
        }

        for (const auto& [dependency, requiredBy] : directedEdges)
        {
            if (!rules.contains(dependency))
            {
                throw InvalidOptimizerRuleset("unregistered rule in dependency graph: {}", dependency.name());
            }
            for (auto dependent : requiredBy)
            {
                if (!rules.contains(dependent))
                {
                    throw InvalidOptimizerRuleset("unregistered rule in dependency graph: {}", dependent.name());
                }
                indegree[dependent]++;
            }
        }

        for (const auto& [type, numberOfRulesItDependsOn] : indegree)
        {
            if (numberOfRulesItDependsOn == 0)
            {
                candidates.push(rules.at(type));
            }
        }

        while (!candidates.empty())
        {
            auto next = candidates.front();
            candidates.pop();
            sequence.emplace_back(next);
            if (directedEdges.contains(next.getType()))
            {
                auto dependents = directedEdges.at(next.getType());
                for (auto dependent : dependents)
                {
                    indegree[dependent] -= 1;
                    if (indegree[dependent] == 0)
                    {
                        candidates.push(rules.at(dependent));
                    }
                }
            }
        }
        if (sequence.size() != rules.size())
        {
            throw InvalidOptimizerRuleset("Cycle detected in rule dependencies.");
        }

        return sequence;
    }

private:
    /// Directed edges between rules. key points to rules that depend on it.
    std::unordered_map<std::type_index, std::set<std::type_index>> directedEdges;
    std::unordered_map<std::type_index, Rule<U>> rules;
};

using PlanRuleManager = RuleManager<LogicalPlan>;
}
