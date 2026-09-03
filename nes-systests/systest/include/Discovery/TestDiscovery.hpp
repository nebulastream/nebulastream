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

#include <filesystem>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <Config/Config.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES
{

/// A test file's name: its path relative the discovery root without extension.
/// E.g., `operator/join/JoinNull` becomes `nes-systests/operator/join/JoinNull.test`.
/// The name labels the file in the report and in the result file path.
/// Strong types, so a name or a group is not confused with a path or with each other.
using TestName = NESStrongStringType<struct TestName_, "INVALID">;
using TestGroup = NESStrongStringType<struct TestGroup_, "INVALID">;

struct DiscoveredTestFile
{
    explicit DiscoveredTestFile(
        const std::filesystem::path& file,
        TestName testName,
        std::optional<std::unordered_set<SystestQueryId>> enabledQueries = std::nullopt);
    [[nodiscard]] std::string getLogFilePath() const;

    [[nodiscard]] TestName name() const { return testName; }

    std::filesystem::path file;
    TestName testName;
    /// The query numbers to run. Every query of the file runs when this is not set.
    std::optional<std::unordered_set<SystestQueryId>> enabledQueries;
    std::vector<TestGroup> groups;
};

std::ostream& operator<<(std::ostream& os, const std::vector<DiscoveredTestFile>& testFiles);

/// Reads the configuration and searches the given directories, returning one invocation's test files.
std::vector<DiscoveredTestFile> discoverTestFiles(const SystestConfiguration& config);

}
