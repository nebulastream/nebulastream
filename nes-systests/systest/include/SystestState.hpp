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
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <SystestConfiguration.hpp>
#include <SystestIdentifiers.hpp>

namespace NES::Systest
{

struct TestFile
{
    explicit TestFile(const std::filesystem::path& file);
    TestFile(
        const std::filesystem::path& file,
        std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber,
        std::vector<QueryNumberRange> queryNumberRanges = {});

    [[nodiscard]] std::string getLogFilePath() const;
    [[nodiscard]] bool hasQueryNumberSelection() const;
    [[nodiscard]] bool isQueryNumberSelected(SystestQueryId queryNumber) const;

    [[nodiscard]] TestName name() const { return file.stem().string(); }

    std::filesystem::path file;
    std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber;
    std::vector<QueryNumberRange> queryNumberRanges;
    std::vector<TestGroup> groups;
};

using TestFileMap = std::unordered_map<std::filesystem::path, TestFile>;

std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap);
std::vector<QueryNumberRange> parseTestQueryNumbers(std::string_view selection);
TestFileMap loadTestFileMap(const SystestConfiguration& config);

}
