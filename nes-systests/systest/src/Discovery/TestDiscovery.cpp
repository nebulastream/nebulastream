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
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h> ///NOLINT: required by fmt

#include <Config/Config.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

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
    return std::ranges::any_of(testFile.groups, [&](const auto& group) { return groups.contains(NES::toLowerCase(group.getRawValue())); });
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

/// Whether `path` is located at or below `root`.
bool isBelow(const std::filesystem::path& path, const std::filesystem::path& root)
{
    const auto relative = std::filesystem::weakly_canonical(path).lexically_relative(std::filesystem::weakly_canonical(root));
    return not relative.empty() && *relative.begin() != "..";
}

std::filesystem::path normalizedDirectory(const std::filesystem::path& directory)
{
    auto normalized = std::filesystem::weakly_canonical(directory);
    return normalized.has_filename() ? normalized : normalized.parent_path();
}

NES::TestName testNameFromRelativePath(std::filesystem::path relativePath)
{
    relativePath.replace_extension();
    return NES::TestName{relativePath.generic_string()};
}

}

namespace NES
{

namespace
{
std::vector<std::filesystem::path>
findTestFilesBelow(const std::filesystem::path& searchRoot, const std::optional<std::string>& fileExtension)
{
    std::vector<std::filesystem::path> files;

    auto toLowerCopy = [](const std::string& str)
    {
        std::string lowerStr = str;
        std::ranges::transform(lowerStr, lowerStr.begin(), ::tolower);
        return lowerStr;
    };

    const auto desiredExtension = fileExtension.has_value() ? toLowerCopy(*fileExtension) : "";

    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(searchRoot, std::filesystem::directory_options::skip_permission_denied)
             | std::views::filter([](const auto& path_) { return path_.is_regular_file(); }))
    {
        if (const std::string entryExt = toLowerCopy(entry.path().extension().string()); !fileExtension || entryExt == desiredExtension)
        {
            files.push_back(std::filesystem::weakly_canonical(entry.path()));
        }
    }
    return files;
}

TestName
nameFor(const std::filesystem::path& file, const std::filesystem::path& discoverRoot, std::span<const std::filesystem::path> searchRoots)
{
    if (isBelow(file, discoverRoot))
    {
        return testNameFromRelativePath(file.lexically_relative(discoverRoot));
    }
    auto namingRoot = file.parent_path();
    for (const auto& searchRoot : searchRoots)
    {
        if (isBelow(file, searchRoot) and isBelow(namingRoot, searchRoot))
        {
            namingRoot = searchRoot;
        }
    }
    return testNameFromRelativePath(file.lexically_relative(namingRoot.parent_path()));
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
                const auto content = std::string_view(line).substr(groupsPrefix.size());
                const auto open = content.find('[');
                const auto close = content.find(']');
                /// A wrong header group declaration is an error, as the file is otherwise excluded from runs without noticing.
                /// Text after the closing bracket is allowed, as some files put an issue reference there.
                if (open == std::string_view::npos || close == std::string_view::npos || close < open)
                {
                    throw TestException(
                        "malformed groups line in file://{}: '{}', expected '# groups: [a, b]'", testfile.file.string(), line);
                }
                for (auto inner = content.substr(open + 1, close - open - 1);
                     auto part : inner | std::views::split(',') | std::views::transform([](auto group) { return std::string_view(group); })
                         | std::views::transform(
                                     [](auto group)
                                     { return group | std::views::filter([](char character) { return !std::isspace(character); }); }))
                {
                    auto group = std::ranges::to<std::string>(part);
                    if (group.empty())
                    {
                        throw TestException("empty group name in file://{}: '{}'", testfile.file.string(), line);
                    }
                    groups.emplace_back(std::move(group));
                }
                break;
            }
        }
        ifstream.close();
    }
    return groups;
}

}

DiscoveredTestFile::DiscoveredTestFile(
    const std::filesystem::path& file, TestName testName, std::optional<std::unordered_set<SystestQueryId>> enabledQueries)
    : file(weakly_canonical(file)), testName(std::move(testName)), enabledQueries(std::move(enabledQueries)), groups(readGroups(*this)) { };

namespace
{

struct TestGroupFiles
{
    std::string name;
    std::vector<std::filesystem::path> files;
};

std::vector<TestGroupFiles> collectTestGroups(const std::vector<DiscoveredTestFile>& testFiles)
{
    std::unordered_map<std::string, std::vector<std::filesystem::path>> groupFilesMap;

    for (const auto& testFile : testFiles)
    {
        for (const auto& groupName : testFile.groups)
        {
            groupFilesMap[groupName.getRawValue()].push_back(testFile.file);
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

std::vector<DiscoveredTestFile> discoverTestFiles(const SystestConfiguration& config)
{
    const auto filters = createDiscoveryFilters(config);
    const auto discoverRoot = normalizedDirectory(config.testDiscoverRoot.getValue());

    if (not config.directlySpecifiedTestFiles.getValue().empty())
    {
        std::optional<std::unordered_set<SystestQueryId>> enabledQueries;
        if (not config.testQueryNumbers.empty())
        {
            enabledQueries = std::ranges::to<std::unordered_set<SystestQueryId>>(
                config.testQueryNumbers.getValues()
                | std::views::transform([](const auto& option) { return SystestQueryId(option.getValue()); }));
        }
        const auto file = std::filesystem::weakly_canonical(config.directlySpecifiedTestFiles.getValue());
        const DiscoveredTestFile testfile{file, nameFor(file, discoverRoot, {}), std::move(enabledQueries)};
        if (matchesDisabledTestFile(testfile, filters.disabledTestFiles))
        {
            std::cout << fmt::format(
                "Including file://{} because it was explicitly selected via --testLocations, overriding disabled_test_files\n",
                testfile.getLogFilePath());
        }
        return {testfile};
    }

    /// A relative directory would not match the absolute root, and a trailing separator would make its parent the
    /// directory itself, so both are normalised away first.
    auto searchRoots = config.testDiscoverDirs.getValues()
        | std::views::transform([](const auto& option) { return normalizedDirectory(option.getValue()); })
        | std::ranges::to<std::vector<std::filesystem::path>>();
    if (searchRoots.empty())
    {
        searchRoots.push_back(discoverRoot);
    }
    std::vector<std::filesystem::path> files;
    /// Two search directories may nest, and a file below both would otherwise be discovered and run twice.
    std::unordered_set<std::string> seen;
    for (const auto& searchRoot : searchRoots)
    {
        for (auto& file : findTestFilesBelow(searchRoot, config.testFileExtension.getValue()))
        {
            if (seen.insert(file.string()).second)
            {
                files.push_back(std::move(file));
            }
        }
    }
    std::vector<DiscoveredTestFile> testFiles;
    testFiles.reserve(files.size());
    for (const auto& file : files)
    {
        testFiles.emplace_back(file, nameFor(file, discoverRoot, searchRoots));
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
    /// Names below the root are distinct by construction. Two directories outside the root can still yield one name, and
    /// two files under one name would share a result file, so the run stops here with both paths rather than later with
    /// a registration error.
    std::unordered_map<TestName, std::filesystem::path> fileByName;
    for (const auto& testFile : testFiles)
    {
        if (const auto [existing, inserted] = fileByName.emplace(testFile.testName, testFile.file); not inserted)
        {
            throw TestException(
                "the test files file://{} and file://{} would both run under the name '{}'; pass a discovery root that contains both",
                existing->second.string(),
                testFile.file.string(),
                testFile.testName);
        }
    }

    return testFiles;
}

std::ostream& operator<<(std::ostream& os, const std::vector<DiscoveredTestFile>& testFiles)
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
