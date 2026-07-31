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

#include <SystestState.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <SystestConfiguration.hpp>

namespace
{
template <typename Range, typename Projection>
std::unordered_set<std::string> toLowerSet(const Range& values, Projection projection)
{
    return values | std::views::transform(projection) | std::views::transform(NES::toLowerCase)
        | std::ranges::to<std::unordered_set<std::string>>();
}

struct DiscoveryFilters
{
    std::unordered_set<std::string> includedGroups;
    std::unordered_set<std::string> excludedGroups;
    std::unordered_set<std::string> explicitlyExcludedGroups;
    std::unordered_set<std::string> disabledTestFiles;
};

DiscoveryFilters createDiscoveryFilters(const NES::SystestConfiguration& config)
{
    auto includedGroups = toLowerSet(config.testGroups.getValues(), [](const auto& option) { return option.getValue(); });
    auto excludedGroups = toLowerSet(config.globalExcludedGroups, [](const auto& group) { return group; });
    for (const auto& includedGroup : includedGroups)
    {
        excludedGroups.erase(includedGroup);
    }

    auto explicitlyExcludedGroups = toLowerSet(config.excludeGroups.getValues(), [](const auto& option) { return option.getValue(); });
    excludedGroups.insert(explicitlyExcludedGroups.begin(), explicitlyExcludedGroups.end());

    return DiscoveryFilters{
        .includedGroups = std::move(includedGroups),
        .excludedGroups = std::move(excludedGroups),
        .explicitlyExcludedGroups = std::move(explicitlyExcludedGroups),
        .disabledTestFiles = toLowerSet(config.disabledTestFiles.getValues(), [](const auto& option) { return option.getValue(); })};
}

bool hasMatchingGroup(const NES::Systest::TestFile& testFile, const std::unordered_set<std::string>& groups)
{
    return std::ranges::any_of(testFile.groups, [&](const auto& group) { return groups.contains(NES::toLowerCase(group)); });
}

bool matchesDisabledTestFile(const NES::Systest::TestFile& testFile, const std::unordered_set<std::string>& disabledTestFiles)
{
    const auto lowerPath = NES::toLowerCase(testFile.file.string());
    const auto lowerFileName = NES::toLowerCase(testFile.file.filename().string());
    return std::ranges::any_of(
        disabledTestFiles,
        [&](const auto& disabledTestFile)
        {
            if (disabledTestFile == lowerFileName || disabledTestFile == lowerPath)
            {
                return true;
            }
            return (disabledTestFile.find('/') != std::string::npos || disabledTestFile.find('\\') != std::string::npos)
                && lowerPath.ends_with(disabledTestFile);
        });
}

std::optional<std::string> getIncludedGroupSkipReason(const NES::Systest::TestFile& testFile, const DiscoveryFilters& filters)
{
    if (filters.includedGroups.empty() || hasMatchingGroup(testFile, filters.includedGroups))
    {
        return std::nullopt;
    }
    return fmt::format("Skipping file://{} because it is not part of the {:} groups\n", testFile.getLogFilePath(), filters.includedGroups);
}

std::optional<std::string> getExcludedGroupSkipReason(const NES::Systest::TestFile& testFile, const DiscoveryFilters& filters)
{
    if (!hasMatchingGroup(testFile, filters.excludedGroups))
    {
        return std::nullopt;
    }

    const auto sourceSuffix = hasMatchingGroup(testFile, filters.explicitlyExcludedGroups) ? std::string{" (from --exclude-groups)"}
                                                                                           : std::string{" (from disable config file)"};
    return fmt::format(
        "Skipping file://{} because it is part of the {:} excluded groups{}\n",
        testFile.getLogFilePath(),
        filters.excludedGroups,
        sourceSuffix);
}

std::optional<std::string> getDisabledTestFileSkipReason(const NES::Systest::TestFile& testFile, const DiscoveryFilters& filters)
{
    if (!matchesDisabledTestFile(testFile, filters.disabledTestFiles))
    {
        return std::nullopt;
    }
    return fmt::format(
        "Skipping file://{} because it is configured in disabled_test_files in the disable config file\n", testFile.getLogFilePath());
}

std::optional<std::string> getSkipReason(const NES::Systest::TestFile& testFile, const DiscoveryFilters& filters)
{
    if (const auto skipReason = getIncludedGroupSkipReason(testFile, filters))
    {
        return skipReason;
    }
    if (const auto skipReason = getExcludedGroupSkipReason(testFile, filters))
    {
        return skipReason;
    }
    if (const auto skipReason = getDisabledTestFileSkipReason(testFile, filters))
    {
        return skipReason;
    }
    return std::nullopt;
}

std::vector<NES::Systest::TestGroup> readGroups(const NES::Systest::TestFile& testfile)
{
    std::vector<NES::Systest::TestGroup> groups;
    if (std::ifstream input(testfile.file); input.is_open())
    {
        std::string line;
        while (std::getline(input, line))
        {
            if (line.starts_with("# groups:"))
            {
                auto content = std::string_view(line).substr(9);
                const auto open = content.find('[');
                const auto close = content.find(']');
                const auto inner = content.substr(open + 1, close - open - 1);
                for (auto part : inner | std::views::split(',') | std::views::transform([](auto range) { return std::string_view(range); })
                         | std::views::transform([](auto value)
                                                 { return value | std::views::filter([](char c) { return !std::isspace(c); }); }))
                {
                    groups.emplace_back(std::ranges::to<std::string>(part));
                }
                break;
            }
        }
    }
    return groups;
}

struct TestGroupFiles
{
    std::string name;
    std::vector<std::filesystem::path> files;
};

std::vector<TestGroupFiles> collectTestGroups(const NES::Systest::TestFileMap& testMap)
{
    std::unordered_map<std::string, std::vector<std::filesystem::path>> groupFilesMap;
    for (const auto& testFile : testMap | std::views::values)
    {
        for (const auto& groupName : testFile.groups)
        {
            groupFilesMap[groupName].push_back(testFile.file);
        }
    }

    std::vector<TestGroupFiles> testGroups;
    testGroups.reserve(groupFilesMap.size());
    for (auto& [groupName, files] : groupFilesMap)
    {
        testGroups.push_back(TestGroupFiles{.name = std::move(groupName), .files = std::move(files)});
    }
    return testGroups;
}

NES::Systest::TestFileMap discoverTestsRecursively(const std::filesystem::path& path, const std::optional<std::string>& fileExtension)
{
    NES::Systest::TestFileMap testFiles;
    auto toLowerCopy = [](std::string value)
    {
        std::ranges::transform(
            value, value.begin(), [](const unsigned char character) { return static_cast<char>(std::tolower(character)); });
        return value;
    };
    const auto desiredExtension = fileExtension ? toLowerCopy(*fileExtension) : std::string{};

    for (const auto& entry : std::filesystem::recursive_directory_iterator(path, std::filesystem::directory_options::skip_permission_denied)
             | std::views::filter([](const auto& candidate) { return candidate.is_regular_file(); }))
    {
        if (!fileExtension || toLowerCopy(entry.path().extension().string()) == desiredExtension)
        {
            NES::Systest::TestFile testFile(entry.path());
            testFiles.emplace(testFile.file, std::move(testFile));
        }
    }
    return testFiles;
}
}

namespace NES::Systest
{

std::vector<QueryNumberRange> parseTestQueryNumbers(const std::string_view selection)
{
    const auto reject = [&]() -> void
    {
        throw InvalidConfigParameter(
            "Invalid test query selection '{}'. Expected a comma-separated list of positive query numbers or ascending ranges", selection);
    };
    const auto parseNumber = [&](const std::string_view value)
    {
        uint64_t number = 0;
        const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), number);
        if (value.empty() || error != std::errc{} || end != value.data() + value.size() || number == 0)
        {
            reject();
        }
        return number;
    };

    if (selection.empty())
    {
        reject();
    }
    std::vector<QueryNumberRange> result;
    size_t position = 0;
    while (position <= selection.size())
    {
        const auto comma = selection.find(',', position);
        const auto item = selection.substr(position, comma == std::string_view::npos ? selection.size() - position : comma - position);
        if (item.empty())
        {
            reject();
        }
        const auto dash = item.find('-');
        if (dash == std::string_view::npos)
        {
            const auto queryNumber = SystestQueryId{parseNumber(item)};
            result.push_back(QueryNumberRange{.first = queryNumber, .last = queryNumber});
        }
        else
        {
            if (item.find('-', dash + 1) != std::string_view::npos)
            {
                reject();
            }
            const auto first = SystestQueryId{parseNumber(item.substr(0, dash))};
            const auto last = SystestQueryId{parseNumber(item.substr(dash + 1))};
            if (first > last)
            {
                reject();
            }
            result.push_back(QueryNumberRange{.first = first, .last = last});
        }
        if (comma == std::string_view::npos)
        {
            break;
        }
        position = comma + 1;
    }
    return result;
}

TestFile::TestFile(const std::filesystem::path& file) : file(weakly_canonical(file)), groups(readGroups(*this))
{
}

TestFile::TestFile(
    const std::filesystem::path& file,
    std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber,
    std::vector<QueryNumberRange> queryNumberRanges)
    : file(weakly_canonical(file))
    , onlyEnableQueriesWithTestQueryNumber(std::move(onlyEnableQueriesWithTestQueryNumber))
    , queryNumberRanges(std::move(queryNumberRanges))
    , groups(readGroups(*this))
{
}

bool TestFile::hasQueryNumberSelection() const
{
    return !onlyEnableQueriesWithTestQueryNumber.empty() || !queryNumberRanges.empty();
}

bool TestFile::isQueryNumberSelected(const SystestQueryId queryNumber) const
{
    return onlyEnableQueriesWithTestQueryNumber.contains(queryNumber)
        || std::ranges::any_of(queryNumberRanges, [&](const QueryNumberRange& range) { return range.contains(queryNumber); });
}

TestFileMap loadTestFileMap(const SystestConfiguration& config)
{
    const auto filters = createDiscoveryFilters(config);

    if (!config.directlySpecifiedTestFiles.getValue().empty())
    {
        const auto directlySpecifiedTestFiles = config.directlySpecifiedTestFiles.getValue();
        if (config.testQueryNumbers.empty() && config.testQueryNumberRanges.empty())
        {
            TestFile testFile(directlySpecifiedTestFiles);
            if (matchesDisabledTestFile(testFile, filters.disabledTestFiles))
            {
                std::cout << fmt::format(
                    "Including file://{} because it was explicitly selected via --testLocation, overriding disabled_test_files\n",
                    testFile.getLogFilePath());
            }
            return TestFileMap{{testFile.file, std::move(testFile)}};
        }

        const auto testNumbers = std::ranges::to<std::unordered_set<SystestQueryId>>(
            config.testQueryNumbers.getValues()
            | std::views::transform([](const auto& option) { return SystestQueryId(option.getValue()); }));
        TestFile testFile(directlySpecifiedTestFiles, testNumbers, config.testQueryNumberRanges);
        if (matchesDisabledTestFile(testFile, filters.disabledTestFiles))
        {
            std::cout << fmt::format(
                "Including file://{} because it was explicitly selected via --testLocation, overriding disabled_test_files\n",
                testFile.getLogFilePath());
        }
        return TestFileMap{{testFile.file, std::move(testFile)}};
    }

    auto testMap = discoverTestsRecursively(config.testsDiscoverDir.getValue(), config.testFileExtension.getValue());
    std::erase_if(
        testMap,
        [&](const auto& nameAndFile)
        {
            if (const auto skipReason = getSkipReason(nameAndFile.second, filters))
            {
                std::cout << *skipReason;
                return true;
            }
            return false;
        });
    return testMap;
}

std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap)
{
    if (testMap.empty())
    {
        os << "No matching test files found\n";
        return os;
    }

    os << "Discovered Test Files:\n";
    for (const auto& [path, testFile] : testMap)
    {
        os << "\t" << path << "\tfile://" << testFile.file.c_str() << "\n";
    }

    const auto testGroups = collectTestGroups(testMap);
    if (!testGroups.empty())
    {
        os << "\nDiscovered Test Groups:\n";
        for (const auto& [name, files] : testGroups)
        {
            os << "\t" << name << "\n";
            for (const auto& filename : files)
            {
                os << "\t\tfile://" << filename.c_str() << "\n";
            }
        }
    }
    return os;
}

std::string TestFile::getLogFilePath() const
{
    if (const char* hostNebulaStreamRoot = std::getenv("HOST_NEBULASTREAM_ROOT"))
    {
        const auto commonFolder = std::filesystem::path(hostNebulaStreamRoot).filename();
        auto filePathIter = file.begin();
        if (const auto it = std::ranges::find(file, commonFolder); it != file.end())
        {
            filePathIter = std::next(it);
        }

        std::filesystem::path resultPath(hostNebulaStreamRoot);
        for (; filePathIter != file.end(); ++filePathIter)
        {
            resultPath /= *filePathIter;
        }
        return resultPath.string();
    }
    return file;
}

}
