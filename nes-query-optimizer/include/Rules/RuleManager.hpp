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

#include <algorithm>
#include <cstddef>
#include <queue>
#include <ranges>
#include <set>
#include <typeindex>
#include <unordered_map>
#include <vector>

#include <Rules/Rule.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{


/**
 * @brief Collects optimizer rules and produces a dependency-ordered execution sequence.
 *
 * Rules are registered via addRule(). Each rule may declare dependencies on other rule
 * types through `Rule::needs()`, `Rule::neededBy()`, `Rule::wants()`, and `Rule::wantedBy()`.
 * The RuleManager builds a directed acyclic graph (DAG) from these declarations and performs
 * a topological sort when getSequence() is called.
 * The order of rules that don't depend on each other is nondeterministic.
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

        rules.emplace(rule.getType(), rule);
    }

    /// Returns a topological ordering of all registered rules respecting their declared dependencies.
    /// Throws InvalidOptimizerRuleset if any dependency references an unregistered rule or if the graph contains a cycle.
    [[nodiscard]] std::vector<Rule<U>> getSequence() const
    {
        std::queue<Rule<U>> candidates;
        std::vector<Rule<U>> sequence;

        const auto downstreamEdges = generateDownstreamEdges();
        auto indegree = initiateIndegrees(downstreamEdges);

        for (const auto& [type, numberOfRulesItNeeds] : indegree)
        {
            if (numberOfRulesItNeeds == 0)
            {
                candidates.push(rules.at(type));
            }
        }

        while (!candidates.empty())
        {
            auto next = candidates.front();
            candidates.pop();
            sequence.emplace_back(next);
            if (auto dependents = downstreamEdges.find(next.getType()); dependents != downstreamEdges.end())
            {
                for (auto dependent : dependents->second)
                {
                    INVARIANT(indegree[dependent] != 0, "dependent must have indegree > 0 since it depends on candidate");

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
            throw InvalidOptimizerRuleset("cycle detected in rule dependencies");
        }
        return sequence;
    }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const
    {
        const auto seq = getSequence();

        if (verbosity == ExplainVerbosity::Short)
        {
            return fmt::format(
                "RuleManager({})", fmt::join(seq | std::views::transform([](const auto& rule) { return rule.getName(); }), ", "));
        }

        std::string out = "RuleManager(\n";
        for (size_t i = 0; i < seq.size(); ++i)
        {
            const auto& rule = seq[i];
            out += fmt::format(
                "\t[{}] RULE({}) NEEDS({}) NEEDED_BY({}) WANTS({}) WANTED_BY({})\n",
                i + 1,
                rule.getName(),
                formatEdges(rule.needs()),
                formatEdges(rule.neededBy()),
                formatEdges(rule.wants()),
                formatEdges(rule.wantedBy()));
        }
        return out + ")";
    }

private:
    std::unordered_map<std::type_index, Rule<U>> rules;

    [[nodiscard]] std::unordered_map<std::type_index, std::set<std::type_index>> generateDownstreamEdges() const
    {
        std::unordered_map<std::type_index, std::set<std::type_index>> downstreamEdges;

        for (const auto& [_, rule] : rules)
        {
            for (const auto& needed : rule.needs())
            {
                downstreamEdges[needed].insert(rule.getType());
            }
            for (const auto& neededBy : rule.neededBy())
            {
                downstreamEdges[rule.getType()].insert(neededBy);
            }
            for (const auto& wanted : rule.wants())
            {
                if (rules.contains(wanted))
                {
                    downstreamEdges[wanted].insert(rule.getType());
                }
            }
            for (const auto& wantedBy : rule.wantedBy())
            {
                if (rules.contains(wantedBy))
                {
                    downstreamEdges[rule.getType()].insert(wantedBy);
                }
            }
        }

        return downstreamEdges;
    }

    [[nodiscard]] std::unordered_map<std::type_index, size_t>
    initiateIndegrees(const std::unordered_map<std::type_index, std::set<std::type_index>>& downstreamEdges) const
    {
        std::unordered_map<std::type_index, size_t> indegree;

        for (const auto& [type, _] : rules)
        {
            indegree[type] = 0;
        }

        for (const auto& [dependency, neededBy] : downstreamEdges)
        {
            if (!rules.contains(dependency))
            {
                throw InvalidOptimizerRuleset("unregistered rule in dependency graph: {}", dependency.name());
            }
            for (auto dependent : neededBy)
            {
                if (!rules.contains(dependent))
                {
                    throw InvalidOptimizerRuleset("unregistered rule in dependency graph: {}", dependent.name());
                }
                ++indegree[dependent];
            }
        }

        return indegree;
    }

    /// Formats a set of rule dependencies for explain(): comma-joined names of the registered rules,
    /// followed by "N unregistered rule(s)" if the set references any type not registered in this manager.
    [[nodiscard]] std::string formatEdges(const std::set<std::type_index>& types) const
    {
        std::string names = fmt::to_string(fmt::join(
            types | std::views::filter([this](std::type_index type) { return rules.contains(type); })
                | std::views::transform([this](std::type_index type) { return rules.at(type).getName(); }),
            ", "));

        const auto unregistered = std::ranges::count_if(types, [this](std::type_index type) { return !rules.contains(type); });
        if (unregistered == 0)
        {
            return names;
        }
        return fmt::format("{}{}{} unregistered rule{}", names, names.empty() ? "" : ", ", unregistered, unregistered > 1 ? "s" : "");
    }
};
}
