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

#include <TestFilePartition.hpp>

#include <algorithm>
#include <ranges>
#include <utility>
#include <variant>
#include <vector>

#include <Model/ConfigurationOverride.hpp>
#include <Model/TestFile.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{
namespace
{

/// Returns the settings a statement asks for, or null when the statement belongs to every part.
/// A visit rather than a chain of tests, so a new kind of statement has to answer this before it compiles.
const ConfigurationOverride* settingsOf(const Systest::Statement& statement)
{
    /// An EXPLAIN asks for no settings, so it joins the part whose queries ask for none either.
    /// Returning null would drop it from every part.
    static const ConfigurationOverride none;
    return std::visit(
        Overloaded{/// Every part repeats the CREATE statements, so a CREATE belongs to no part in particular.
                   [](const Systest::CreateStatement&) -> const ConfigurationOverride* { return nullptr; },
                   [](const Systest::QueryStatement& query) { return &query.settings; },
                   [](const Systest::DifferentialStatement& block) { return &block.settings; },
                   [](const Systest::ExplainStatement&) { return &none; }},
        statement);
}

/// Returns the CREATE statements of a test file, which every part starts from.
std::vector<Systest::Statement> createStatementsOf(const TestFile& testFile)
{
    return testFile.statements | std::views::filter([](const auto& each) { return std::holds_alternative<Systest::CreateStatement>(each); })
        | std::ranges::to<std::vector<Systest::Statement>>();
}

}

std::vector<TestFilePart> partitionBySettings(const TestFile& testFile)
{
    const auto partFor = [](std::vector<TestFilePart>& parts, const ConfigurationOverride& settings)
    { return std::ranges::find_if(parts, [&](const TestFilePart& each) { return each.settings == settings; }); };

    std::vector<TestFilePart> parts;
    for (const auto& statement : testFile.statements)
    {
        const auto* settings = settingsOf(statement);
        if (settings == nullptr or partFor(parts, *settings) != parts.end())
        {
            continue;
        }
        parts.emplace_back(*settings, TestFile{.path = testFile.path, .statements = createStatementsOf(testFile)});
    }
    if (parts.empty())
    {
        return {TestFilePart{.settings = {}, .file = testFile}};
    }

    for (const auto& statement : testFile.statements)
    {
        if (const auto* settings = settingsOf(statement))
        {
            partFor(parts, *settings)->file.statements.push_back(statement);
        }
    }
    return parts;
}

}
