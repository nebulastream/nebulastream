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

#include <Discovery/TestDiscovery.hpp>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Config/Config.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h> ///NOLINT: required by fmt
#include <SystestState.hpp>

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
    auto includedGroups = toLowerSet(config.testGroups, [](const auto& group) { return group; });
    auto excludedGroups = toLowerSet(config.globalExcludedGroups, [](const auto& group) { return group; });
    for (const auto& includedGroup : includedGroups)
    {
        excludedGroups.erase(includedGroup);
    }

    auto explicitlyExcludedGroups = toLowerSet(config.excludeGroups, [](const auto& group) { return group; });
    excludedGroups.insert(explicitlyExcludedGroups.begin(), explicitlyExcludedGroups.end());

    return DiscoveryFilters{
        .includedGroups = std::move(includedGroups),
        .excludedGroups = std::move(excludedGroups),
        .explicitlyExcludedGroups = std::move(explicitlyExcludedGroups),
        .disabledTestFiles = toLowerSet(config.disabledTestFiles, [](const auto& file) { return file; })};
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

NES::Systest::TestName testNameFromRelativePath(std::filesystem::path relativePath)
{
    relativePath.replace_extension();
    return relativePath.generic_string();
}

NES::Systest::TestName testNameStem(const NES::Systest::TestFile& testFile)
{
    return std::filesystem::path(testFile.name()).filename().string();
}

/// Use the filename stem when it is unique, retaining the relative path to disambiguate duplicate stems.
void shortenUniqueTestNames(NES::Systest::TestFileMap& testFiles)
{
    std::unordered_map<NES::Systest::TestName, size_t> testNameCounts;
    for (const auto& testFile : testFiles | std::views::values)
    {
        ++testNameCounts[testNameStem(testFile)];
    }

    for (auto& testFile : testFiles | std::views::values)
    {
        auto stem = testNameStem(testFile);
        if (testNameCounts.at(stem) == 1)
        {
            testFile.testName = std::move(stem);
        }
    }
}
}

namespace NES::Systest
{
namespace
{

TestFileMap discoverTestsRecursively(const std::filesystem::path& path, const std::optional<std::string>& fileExtension)
{
    TestFileMap testFiles;

    auto toLowerCopy = [](const std::string& str)
    {
        std::string lowerStr = str;
        std::ranges::transform(lowerStr, lowerStr.begin(), ::tolower);
        return lowerStr;
    };

    const auto desiredExtension = fileExtension.has_value() ? toLowerCopy(*fileExtension) : "";

    for (const auto& entry : std::filesystem::recursive_directory_iterator(path, std::filesystem::directory_options::skip_permission_denied)
             | std::views::filter([](const auto& entry) { return entry.is_regular_file(); }))
    {
        const std::string entryExt = toLowerCopy(entry.path().extension().string());
        if (!fileExtension || entryExt == desiredExtension)
        {
            auto sourceCatalog = SourceCatalog::create();
            TestFile testfile(
                entry.path(),
                testNameFromRelativePath(entry.path().lexically_relative(path)),
                copyPtr(sourceCatalog),
                std::make_shared<SinkCatalog>());
            testFiles.insert({testfile.file, std::move(testfile)});
        }
    }
    return testFiles;
}

struct TestGroupFiles
{
    std::string name;
    std::vector<std::filesystem::path> files;
};

std::vector<TestGroupFiles> collectTestGroups(const TestFileMap& testMap)
{
    std::unordered_map<std::string, std::vector<std::filesystem::path>> groupFilesMap;

    for (const auto& [testName, testFile] : testMap)
    {
        for (const auto& groupName : testFile.groups)
        {
            groupFilesMap[groupName].push_back(testFile.file);
        }
    }

    std::vector<TestGroupFiles> testGroups;
    testGroups.reserve(groupFilesMap.size());
    for (const auto& [groupName, files] : groupFilesMap)
    {
        testGroups.push_back(TestGroupFiles{.name = groupName, .files = files});
    }
    return testGroups;
}

}

TestFileMap loadTestFileMap(const SystestConfiguration& config)
{
    const auto filters = createDiscoveryFilters(config);

    if (not config.directlySpecifiedTestFiles.empty())
    {
        const auto& directlySpecifiedTestFiles = config.directlySpecifiedTestFiles;

        if (config.testQueryNumbers.empty())
        {
            auto sourceCatalog = SourceCatalog::create();
            auto testfile = TestFile(directlySpecifiedTestFiles, copyPtr(sourceCatalog), std::make_shared<SinkCatalog>());
            if (matchesDisabledTestFile(testfile, filters.disabledTestFiles))
            {
                std::cout << fmt::format(
                    "Including file://{} because it was explicitly selected via --testLocations, overriding disabled_test_files\n",
                    testfile.getLogFilePath());
            }
            return TestFileMap{{std::pair{testfile.file, std::move(testfile)}}};
        }

        const auto testNumbers = std::ranges::to<std::unordered_set<SystestQueryId>>(
            config.testQueryNumbers | std::views::transform([](const auto& queryNumber) { return SystestQueryId(queryNumber); }));
        auto sourceCatalog = SourceCatalog::create();
        const auto testfile = TestFile(directlySpecifiedTestFiles, testNumbers, copyPtr(sourceCatalog), std::make_shared<SinkCatalog>());
        if (matchesDisabledTestFile(testfile, filters.disabledTestFiles))
        {
            std::cout << fmt::format(
                "Including file://{} because it was explicitly selected via --testLocations, overriding disabled_test_files\n",
                testfile.getLogFilePath());
        }
        return TestFileMap{{testfile.file, testfile}};
    }

    TestFileMap testMap;
    for (const auto& discoverDir : config.testsDiscoverDirs)
    {
        auto tests = discoverTestsRecursively(discoverDir, config.testFileExtension);
        testMap.merge(tests);
    }

    std::erase_if(
        testMap,
        [&](const auto& nameAndFile)
        {
            const auto& [name, testFile] = nameAndFile;
            if (const auto skipReason = getSkipReason(testFile, filters))
            {
                std::cout << *skipReason;
                return true;
            }
            return false;
        });
    shortenUniqueTestNames(testMap);

    return testMap;
}

std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap)
{
    if (testMap.empty())
    {
        os << "No matching test files found\n";
    }
    else
    {
        os << "Discovered Test Files:\n";
        for (const auto& testFile : testMap)
        {
            os << "\t" << testFile.first << "\tfile://" << testFile.second.file.c_str() << "\n";
        }

        auto testGroups = collectTestGroups(testMap);
        if (not testGroups.empty())
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
    }
    return os;
}

}
