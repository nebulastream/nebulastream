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
#include <optional>
#include <ranges>
#include <variant>
#include <vector>

#include <Model/ConfigurationOverride.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{

std::vector<TestFilePart> partitionByOverrides(const ParsedTestFile& testFile)
{
    /// Each part runs under its own key, and that key prefixes every qualified name.
    /// CREATE statements are repeated into every part, each declaring its sources using the prefix its queries reference.
    const auto creates = testFile.statements
        | std::views::filter([](const auto& statement) { return std::holds_alternative<CreateStatement>(statement); })
        | std::ranges::to<std::vector<TestStatement>>();

    std::vector<TestFilePart> partitions;
    for (const auto& statement : testFile.statements)
    {
        /// Every part repeats the CREATE statements, so a CREATE belongs to no part in particular.
        if (const auto overrides = std::visit<std::optional<ConfigurationOverride>>(
                Overloaded{
                    [](const CreateStatement&) { return std::nullopt; },
                    [](const SelectStatement& query) { return query.overrides; },
                    [](const DifferentialStatement& block) { return block.overrides; },
                    /// An EXPLAIN does not use overrides thus far, so it joins the default partition.
                    [](const ExplainStatement&) { return ConfigurationOverride{}; }},
                statement))
        {
            auto partition = std::ranges::find(partitions, *overrides, &TestFilePart::overrides);
            if (partition == partitions.end())
            {
                partition = partitions.emplace(partitions.end(), *overrides, ParsedTestFile{.path = testFile.path, .statements = creates});
            }
            partition->file.statements.push_back(statement);
        }
    }
    if (partitions.empty())
    {
        return {TestFilePart{.overrides = {}, .file = testFile}};
    }
    return partitions;
}

}
