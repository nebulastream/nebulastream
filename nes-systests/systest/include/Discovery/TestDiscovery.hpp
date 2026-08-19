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
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <Config/Config.hpp>
#include <Model/SystestQueryId.hpp>

namespace NES
{

using TestName = std::string;
using TestGroup = std::string;

/// One selected test file: its reported name, the groups declared in its header, and the query numbers this run is
/// restricted to. An empty set of query numbers runs every query of the file.
///
/// The name is the file's path below the discovery root without its extension, so two files that share a stem in
/// different folders stay apart. A name whose stem is unique across the run is shortened back to that stem once
/// discovery has finished, and a file named directly on the command line keeps its stem.
struct DiscoveredTestFile
{
    explicit DiscoveredTestFile(const std::filesystem::path& file);
    explicit DiscoveredTestFile(const std::filesystem::path& file, TestName testName);
    explicit DiscoveredTestFile(const std::filesystem::path& file, std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber);
    explicit DiscoveredTestFile(
        const std::filesystem::path& file, std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber, TestName testName);
    [[nodiscard]] std::string getLogFilePath() const;

    [[nodiscard]] TestName name() const { return testName; }

    std::filesystem::path file;
    TestName testName;
    std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber;
    std::vector<TestGroup> groups;
};

/// One invocation's test files.
using DiscoveredTestFiles = std::vector<DiscoveredTestFile>;

std::ostream& operator<<(std::ostream& os, const DiscoveredTestFiles& testFiles);

/// Selects the test files of one invocation.
/// The command line either gives a file directly, or the discovery root supplies every file that passes the group and disable filters.
DiscoveredTestFiles discoverTestFiles(const SystestConfiguration& config);

}
