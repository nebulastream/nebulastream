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

#include <Parser/TestFilePartition.hpp>

#include <algorithm>
#include <ranges>
#include <variant>
#include <vector>

#include <Model/ConfigurationOverride.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{
namespace
{

/// Returns the configuration a statement asks for, or null when the statement belongs to every part.
/// A visit rather than a chain of tests, so a new kind of statement has to answer this before it compiles.
const ConfigurationOverride* overridesOf(const TestStatement& statement)
{
    /// An EXPLAIN asks for no configuration, so it joins the part whose queries ask for none either.
    /// Returning null would drop it from every part.
    static const ConfigurationOverride None;
    return std::visit(
        Overloaded{/// Every part repeats the CREATE statements, so a CREATE belongs to no part in particular.
                   [](const CreateStatement&) -> const ConfigurationOverride* { return nullptr; },
                   [](const SelectStatement& query) { return &query.overrides; },
                   [](const DifferentialStatement& block) { return &block.overrides; },
                   [](const ExplainStatement&) { return &None; }},
        statement);
}

/// Returns the CREATE statements of a test file, which every part starts from.
std::vector<TestStatement> createStatementsOf(const ParsedTestFile& testFile)
{
    return testFile.statements | std::views::filter([](const auto& each) { return std::holds_alternative<CreateStatement>(each); })
        | std::ranges::to<std::vector<TestStatement>>();
}

}

std::vector<TestFilePart> partitionByOverrides(const ParsedTestFile& testFile)
{
    const auto partFor = [](std::vector<TestFilePart>& parts, const ConfigurationOverride& overrides)
    { return std::ranges::find_if(parts, [&](const TestFilePart& each) { return each.overrides == overrides; }); };

    std::vector<TestFilePart> parts;
    for (const auto& statement : testFile.statements)
    {
        const auto* overrides = overridesOf(statement);
        if (overrides == nullptr or partFor(parts, *overrides) != parts.end())
        {
            continue;
        }
        parts.emplace_back(*overrides, ParsedTestFile{.path = testFile.path, .statements = createStatementsOf(testFile)});
    }
    if (parts.empty())
    {
        return {TestFilePart{.overrides = {}, .file = testFile}};
    }

    for (const auto& statement : testFile.statements)
    {
        if (const auto* overrides = overridesOf(statement))
        {
            partFor(parts, *overrides)->file.statements.push_back(statement);
        }
    }
    return parts;
}

}
