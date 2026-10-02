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
#include <functional>
#include <ranges>
#include <variant>
#include <vector>

#include <Model/ParsedTestFile.hpp>

namespace NES
{

std::vector<TestFilePartition> partitionByOverrides(const ParsedTestFile& testFile)
{
    /// Each partition repeats the CREATEs, since its key prefixes their names.
    const auto isCreate = [](const TestStatement& statement) { return std::holds_alternative<CreateStatement>(statement); };
    const auto creates = testFile.statements | std::views::filter(isCreate) | std::ranges::to<std::vector<TestStatement>>();

    std::vector<TestFilePartition> partitions;
    for (const auto& testCase : testFile.statements | std::views::filter(std::not_fn(isCreate)))
    {
        const auto overrides = getOverridesOf(testCase);
        const auto found = std::ranges::find(partitions, overrides, &TestFilePartition::overrides);
        TestFilePartition& partition = found == partitions.end()
            ? partitions.emplace_back(overrides, ParsedTestFile{.path = testFile.path, .statements = creates})
            : *found;
        partition.file.statements.push_back(testCase);
    }
    if (partitions.empty())
    {
        return {TestFilePartition{.overrides = {}, .file = testFile}};
    }
    return partitions;
}

}
