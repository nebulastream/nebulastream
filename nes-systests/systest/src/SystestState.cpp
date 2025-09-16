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
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h> ///NOLINT: required by fmt

#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Strings.hpp>

namespace NES::Systest
{

std::filesystem::path
SystestQueryContext::resultFile(const std::filesystem::path& workingDir, std::string_view testName, const SystestQueryId queryIdInTestFile)
{
    const auto resultDir = workingDir / "results";
    if (not is_directory(resultDir))
    {
        create_directory(resultDir);
        std::cout << "Created working directory: file://" << resultDir.string() << "\n";
    }

    return resultDir / std::filesystem::path(fmt::format("{}_{}.csv", testName, queryIdInTestFile));
}

std::filesystem::path
SystestQueryContext::sourceFile(const std::filesystem::path& workingDir, std::string_view testName, const uint64_t sourceId)
{
    const auto sourceDir = workingDir / "sources";
    if (not is_directory(sourceDir))
    {
        create_directory(sourceDir);
        std::cout << "Created working directory: file://" << sourceDir.string() << "\n";
    }

    return sourceDir / std::filesystem::path(fmt::format("{}_{}.csv", testName, sourceId));
}

std::filesystem::path SystestQueryContext::resultFile() const
{
    return resultFile(workingDir, testName, queryIdInFile);
}

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
            TestFile testfile(entry.path(), std::make_unique<SourceCatalog>(), std::make_unique<SinkCatalog>());
            testFiles.insert({testfile.file, std::move(testfile)});
        }
    }
    return testFiles;
}

std::vector<TestGroup> readGroups(const TestFile& testfile)
{
    std::vector<TestGroup> groups;
    if (std::ifstream ifstream(testfile.file); ifstream.is_open())
    {
        std::string line;
        while (std::getline(ifstream, line))
        {
            if (line.starts_with("# groups:"))
            {
                std::string groupsStr = line.substr(9);
                groupsStr.erase(std::ranges::remove(groupsStr, '[').begin(), groupsStr.end());
                groupsStr.erase(std::ranges::remove(groupsStr, ']').begin(), groupsStr.end());
                std::istringstream iss(groupsStr);
                std::string group;
                while (std::getline(iss, group, ','))
                {
                    std::erase_if(group, ::isspace);
                    groups.emplace_back(group);
                }
                break;
            }
        }
        ifstream.close();
    }
    return groups;
}

TestFile::TestFile(
    const std::filesystem::path& file, std::shared_ptr<SourceCatalog> sourceCatalog, std::shared_ptr<SinkCatalog> sinkCatalog)
    : file(weakly_canonical(file)), groups(readGroups(*this)), sourceCatalog(std::move(sourceCatalog)), sinkCatalog(std::move(sinkCatalog))
{
}

TestFile::TestFile(
    const std::filesystem::path& file,
    std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber,
    std::shared_ptr<SourceCatalog> sourceCatalog,
    std::shared_ptr<SinkCatalog> sinkCatalog)
    : file(weakly_canonical(file))
    , onlyEnableQueriesWithTestQueryNumber(std::move(onlyEnableQueriesWithTestQueryNumber))
    , groups(readGroups(*this))
    , sourceCatalog(std::move(sourceCatalog))
    , sinkCatalog(std::move(sinkCatalog))
{
}

struct TestGroupFiles
{
    std::string name;
    std::vector<std::filesystem::path> files;
};

std::vector<TestGroupFiles> collectTestGroups(const TestFileMap& testMap)
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
    for (const auto& [groupName, files] : groupFilesMap)
    {
        testGroups.push_back(TestGroupFiles{.name = groupName, .files = files});
    }
    return testGroups;
}

TestFileMap loadTestFileMap(const SystestConfiguration& config)
{
    if (not config.directlySpecifiedTestFiles.getValue().empty()) /// load specifc test file
    {
        auto directlySpecifiedTestFiles = config.directlySpecifiedTestFiles.getValue();

        if (config.testQueryNumbers.empty()) /// case: load all tests
        {
            auto testfile = TestFile(directlySpecifiedTestFiles, std::make_shared<SourceCatalog>(), std::make_shared<SinkCatalog>());
            return TestFileMap{{testfile.file, std::move(testfile)}};
        }
        /// case: load a concrete set of tests
        auto scalarTestNumbers = config.testQueryNumbers.getValues();
        const auto testNumbers = std::ranges::to<std::unordered_set<SystestQueryId>>(
            scalarTestNumbers | std::views::transform([](const auto& option) { return SystestQueryId(option.getValue()); }));

        auto testfile
            = TestFile(directlySpecifiedTestFiles, testNumbers, std::make_unique<SourceCatalog>(), std::make_unique<SinkCatalog>());
        return TestFileMap{{testfile.file, std::move(testfile)}};
    }

    auto testsDiscoverDir = config.testsDiscoverDir.getValue();
    auto testFileExtension = config.testFileExtension.getValue();
    auto testMap = discoverTestsRecursively(testsDiscoverDir, testFileExtension);

    auto toLowerSet = [](const auto& vec)
    {
        return vec | std::views::transform([](const auto& scalarOption) { return scalarOption.getValue(); })
            | std::views::transform(Util::toLowerCase) | std::ranges::to<std::unordered_set<std::string>>();
    };
    auto includedGroupsVector = config.testGroups.getValues();
    auto excludedGroupsVector = config.excludeGroups.getValues();
    const auto includedGroups = toLowerSet(config.testGroups.getValues());
    const auto excludedGroups = toLowerSet(config.excludeGroups.getValues());

    std::erase_if(
        testMap,
        [&](const auto& nameAndFile)
        {
            const auto& [name, testFile] = nameAndFile;
            if (!includedGroups.empty())
            {
                if (std::ranges::none_of(
                        testFile.groups, [&](const auto& group) { return includedGroups.contains(Util::toLowerCase(group)); }))
                {
                    std::cout << fmt::format(
                        "Skipping file://{} because it is not part of the {:} groups\n", testFile.getLogFilePath(), includedGroups);
                    return true;
                }
            }
            if (std::ranges::any_of(testFile.groups, [&](const auto& group) { return excludedGroups.contains(Util::toLowerCase(group)); }))
            {
                std::cout << fmt::format(
                    "Skipping file://{} because it is part of the {:} excluded groups\n", testFile.getLogFilePath(), excludedGroups);
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

std::string TestFile::getLogFilePath() const
{
    if (const char* hostNebulaStreamRoot = std::getenv("HOST_NEBULASTREAM_ROOT"))
    {
        /// Set the correct logging path when using docker
        /// To do this, we need to combine the path to the host's nebula-stream root with the path to the file.
        /// We assume that the last part of hostNebulaStreamRoot is the common folder name.
        auto commonFolder = std::filesystem::path(hostNebulaStreamRoot).filename();

        /// Find the position of the common folder in the file path
        auto filePathIter = file.begin();
        if (const auto it = std::ranges::find(file, commonFolder); it != file.end())
        {
            filePathIter = std::next(it);
        }

        /// Combining the path to the host's nebula-stream root with the path to the file
        std::filesystem::path resultPath(hostNebulaStreamRoot);
        for (; filePathIter != file.end(); ++filePathIter)
        {
            resultPath /= *filePathIter;
        }

        return resultPath.string();
    }

    /// Set the correct logging path without docker
    return std::filesystem::path(file);
}
}
