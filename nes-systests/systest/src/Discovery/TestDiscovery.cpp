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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h> ///NOLINT: required by fmt

#include <Config/Config.hpp>
#include <Model/SystestQueryId.hpp>
#include <Util/Strings.hpp>

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

bool hasMatchingGroup(const NES::DiscoveredTestFile& testFile, const std::unordered_set<std::string>& groups)
{
    return std::ranges::any_of(testFile.groups, [&](const auto& group) { return groups.contains(NES::toLowerCase(group)); });
}

bool matchesDisabledTestFile(const NES::DiscoveredTestFile& testFile, const std::unordered_set<std::string>& disabledTestFiles)
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

std::optional<std::string> getIncludedGroupSkipReason(const NES::DiscoveredTestFile& testFile, const DiscoveryFilters& filters)
{
    if (filters.includedGroups.empty() || hasMatchingGroup(testFile, filters.includedGroups))
    {
        return std::nullopt;
    }
    return fmt::format("Skipping file://{} because it is not part of the {:} groups\n", testFile.getLogFilePath(), filters.includedGroups);
}

std::optional<std::string> getExcludedGroupSkipReason(const NES::DiscoveredTestFile& testFile, const DiscoveryFilters& filters)
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

std::optional<std::string> getDisabledTestFileSkipReason(const NES::DiscoveredTestFile& testFile, const DiscoveryFilters& filters)
{
    if (!matchesDisabledTestFile(testFile, filters.disabledTestFiles))
    {
        return std::nullopt;
    }
    return fmt::format(
        "Skipping file://{} because it is configured in disabled_test_files in the disable config file\n", testFile.getLogFilePath());
}

std::optional<std::string> getSkipReason(const NES::DiscoveredTestFile& testFile, const DiscoveryFilters& filters)
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

/// Whether `path` lies at or below `root`, both as the run canonicalized them.
bool isBelow(const std::filesystem::path& path, const std::filesystem::path& root)
{
    const auto relative = std::filesystem::weakly_canonical(path).lexically_relative(std::filesystem::weakly_canonical(root));
    return not relative.empty() && *relative.begin() != "..";
}

NES::TestName testNameFromRelativePath(std::filesystem::path relativePath)
{
    relativePath.replace_extension();
    return relativePath.generic_string();
}

NES::TestName testNameStem(const NES::DiscoveredTestFile& testFile)
{
    return std::filesystem::path(testFile.name()).filename().string();
}

/// Use the filename stem when it is unique, retaining the relative path to disambiguate duplicate stems.
void shortenUniqueTestNames(NES::DiscoveredTestFiles& testFiles)
{
    std::unordered_map<NES::TestName, size_t> testNameCounts;
    for (const auto& testFile : testFiles)
    {
        ++testNameCounts[testNameStem(testFile)];
    }

    for (auto& testFile : testFiles)
    {
        auto stem = testNameStem(testFile);
        if (testNameCounts.at(stem) == 1)
        {
            testFile.testName = std::move(stem);
        }
    }
}

std::unordered_map<NES::TestName, size_t> countNames(const NES::DiscoveredTestFiles& testFiles)
{
    std::unordered_map<NES::TestName, size_t> counts;
    for (const auto& testFile : testFiles)
    {
        ++counts[testFile.testName];
    }
    return counts;
}

/// The directory that a name grows into when two files share one, which is the one above what the name already covers.
std::optional<std::string> nextDirectoryAbove(const NES::DiscoveredTestFile& testFile)
{
    const auto covered = std::filesystem::path{testFile.testName}.parent_path();
    auto directory = testFile.file.parent_path();
    for (auto component = covered.begin(); component != covered.end() && directory.has_parent_path(); ++component)
    {
        directory = directory.parent_path();
    }
    if (not directory.has_filename() || not directory.has_parent_path())
    {
        return std::nullopt;
    }
    return directory.filename().string();
}

/// Two files named against different directories can still end up with the same name, which the report and the result
/// file path cannot tell apart. Grow each of them by the directory above until they differ.
void separateDuplicateTestNames(NES::DiscoveredTestFiles& testFiles)
{
    for (auto counts = countNames(testFiles); std::ranges::any_of(counts, [](const auto& entry) { return entry.second > 1; });
         counts = countNames(testFiles))
    {
        bool grown = false;
        for (auto& testFile : testFiles)
        {
            if (counts.at(testFile.testName) > 1)
            {
                if (const auto directory = nextDirectoryAbove(testFile))
                {
                    testFile.testName = *directory + "/" + testFile.testName;
                    grown = true;
                }
            }
        }
        /// Nothing left to grow into, so the names stay as they are rather than looping forever.
        if (not grown)
        {
            return;
        }
    }
}
}

namespace NES
{

namespace
{
/// Names each file by its path relative to the key root, so a file keeps the same name however the run narrowed the search.
DiscoveredTestFiles discoverTestsRecursively(
    const std::filesystem::path& path, const std::filesystem::path& keyRoot, const std::optional<std::string>& fileExtension)
{
    DiscoveredTestFiles testFiles;

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
            testFiles.emplace_back(entry.path(), testNameFromRelativePath(entry.path().lexically_relative(keyRoot)));
        }
    }
    return testFiles;
}

std::vector<TestGroup> readGroups(const DiscoveredTestFile& testfile)
{
    constexpr auto groupsPrefix = std::string_view{"# groups:"};

    std::vector<TestGroup> groups;
    if (std::ifstream ifstream(testfile.file); ifstream.is_open())
    {
        std::string line;
        while (std::getline(ifstream, line))
        {
            if (line.starts_with(groupsPrefix))
            {
                auto content = std::string_view(line).substr(groupsPrefix.size());
                auto open = content.find('[');
                auto close = content.find(']');
                auto inner = content.substr(open + 1, close - open - 1);
                for (auto part : inner | std::views::split(',') | std::views::transform([](auto group) { return std::string_view(group); })
                         | std::views::transform(
                                     [](auto group)
                                     { return group | std::views::filter([](char character) { return !std::isspace(character); }); }))
                {
                    groups.emplace_back(std::ranges::to<std::string>(part));
                }
                break;
            }
        }
        ifstream.close();
    }
    return groups;
}

}

DiscoveredTestFile::DiscoveredTestFile(const std::filesystem::path& file) : DiscoveredTestFile(file, file.stem().string()) { };

DiscoveredTestFile::DiscoveredTestFile(const std::filesystem::path& file, TestName testName)
    : file(weakly_canonical(file)), testName(std::move(testName)), groups(readGroups(*this)) { };

DiscoveredTestFile::DiscoveredTestFile(
    const std::filesystem::path& file, std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber)
    : DiscoveredTestFile(file, std::move(onlyEnableQueriesWithTestQueryNumber), file.stem().string())
{
}

DiscoveredTestFile::DiscoveredTestFile(
    const std::filesystem::path& file, std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber, TestName testName)
    : file(weakly_canonical(file))
    , testName(std::move(testName))
    , onlyEnableQueriesWithTestQueryNumber(std::move(onlyEnableQueriesWithTestQueryNumber))
    , groups(readGroups(*this)) { };

namespace
{

struct TestGroupFiles
{
    std::string name;
    std::vector<std::filesystem::path> files;
};

std::vector<TestGroupFiles> collectTestGroups(const DiscoveredTestFiles& testFiles)
{
    std::unordered_map<std::string, std::vector<std::filesystem::path>> groupFilesMap;

    for (const auto& testFile : testFiles)
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

DiscoveredTestFiles discoverTestFiles(const SystestConfiguration& config)
{
    const auto filters = createDiscoveryFilters(config);

    if (not config.directlySpecifiedTestFiles.getValue().empty())
    {
        const auto directlySpecifiedTestFiles = config.directlySpecifiedTestFiles.getValue();

        if (config.testQueryNumbers.empty())
        {
            const auto testfile = DiscoveredTestFile(directlySpecifiedTestFiles);
            if (matchesDisabledTestFile(testfile, filters.disabledTestFiles))
            {
                std::cout << fmt::format(
                    "Including file://{} because it was explicitly selected via --testLocations, overriding disabled_test_files\n",
                    testfile.getLogFilePath());
            }
            return {testfile};
        }

        const auto testNumbers = std::ranges::to<std::unordered_set<SystestQueryId>>(
            config.testQueryNumbers.getValues()
            | std::views::transform([](const auto& option) { return SystestQueryId(option.getValue()); }));
        const auto testfile = DiscoveredTestFile(directlySpecifiedTestFiles, testNumbers);
        if (matchesDisabledTestFile(testfile, filters.disabledTestFiles))
        {
            std::cout << fmt::format(
                "Including file://{} because it was explicitly selected via --testLocations, overriding disabled_test_files\n",
                testfile.getLogFilePath());
        }
        return {testfile};
    }

    const std::filesystem::path discoverRoot{config.testDiscoverRoot.getValue()};
    auto searchRoots = config.testDiscoverDirs.getValues()
        | std::views::transform([](const auto& option) { return std::filesystem::path{option.getValue()}; })
        | std::ranges::to<std::vector<std::filesystem::path>>();
    if (searchRoots.empty())
    {
        searchRoots.push_back(discoverRoot);
    }
    DiscoveredTestFiles testFiles;
    /// Two search directories may nest, and a file below both would otherwise be discovered and run twice.
    std::unordered_set<std::string> seen;
    for (const auto& searchRoot : searchRoots)
    {
        /// A directory below the root narrows the search, and its files keep the name a full run would give them.
        /// A directory outside the root has no such name, so its files are named against the directory itself rather
        /// than through a path that walks up out of the root.
        const auto& keyRoot = isBelow(searchRoot, discoverRoot) ? discoverRoot : searchRoot;
        for (auto& found : discoverTestsRecursively(searchRoot, keyRoot, config.testFileExtension.getValue()))
        {
            if (seen.insert(found.file.string()).second)
            {
                testFiles.push_back(std::move(found));
            }
        }
    }
    std::erase_if(
        testFiles,
        [&](const DiscoveredTestFile& testFile)
        {
            if (const auto skipReason = getSkipReason(testFile, filters))
            {
                std::cout << *skipReason;
                return true;
            }
            return false;
        });
    shortenUniqueTestNames(testFiles);
    separateDuplicateTestNames(testFiles);

    return testFiles;
}

std::ostream& operator<<(std::ostream& os, const DiscoveredTestFiles& testFiles)
{
    if (testFiles.empty())
    {
        os << "No matching test files found\n";
    }
    else
    {
        os << "Discovered Test Files:\n";
        for (const auto& testFile : testFiles)
        {
            os << "\tfile://" << testFile.file.c_str() << "\n";
        }

        if (auto testGroups = collectTestGroups(testFiles); not testGroups.empty())
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

std::string DiscoveredTestFile::getLogFilePath() const
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe) The environment is read while the run is still single threaded.
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

    return std::filesystem::path(file);
}
}
