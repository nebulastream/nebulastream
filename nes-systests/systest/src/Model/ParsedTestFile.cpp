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

#include <Model/ParsedTestFile.hpp>

#include <algorithm>
#include <unordered_set>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::vector<SystestQueryId> getQueryNumbersOf(const TestStatement& statement)
{
    return std::visit(
        Overloaded{
            [](const CreateStatement&) { return std::vector<SystestQueryId>{}; },
            [](const SelectStatement& query) { return std::vector{query.id}; },
            [](const DifferentialStatement& block) { return std::vector{block.firstId, block.secondId}; },
            [](const ExplainStatement& explain) { return std::vector{explain.id}; }},
        statement);
}

ConfigurationOverride getOverridesOf(const TestStatement& statement)
{
    return std::visit<ConfigurationOverride>(
        Overloaded{
            [](const CreateStatement&)
            {
                PRECONDITION(false, "a CREATE has no settings of its own, every partition repeats it");
                return ConfigurationOverride{};
            },
            [](const SelectStatement& query) { return query.overrides; },
            [](const DifferentialStatement& block) { return block.overrides; },
            [](const ExplainStatement&) { return ConfigurationOverride{}; }},
        statement);
}

void retainSelectedStatements(std::vector<TestStatement>& statements, const std::unordered_set<SystestQueryId>& selected)
{
    if (selected.empty())
    {
        return;
    }
    std::erase_if(
        statements,
        [&](const TestStatement& statement)
        {
            const auto numbers = getQueryNumbersOf(statement);
            return not numbers.empty() and std::ranges::none_of(numbers, [&](const auto& number) { return selected.contains(number); });
        });
}

bool hasTestCases(const std::vector<TestStatement>& statements)
{
    return std::ranges::any_of(
        statements, [](const TestStatement& statement) { return not std::holds_alternative<CreateStatement>(statement); });
}

}
