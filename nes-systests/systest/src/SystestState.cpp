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
#include <array>
#include <chrono>
#include <cstdint>
#include <expected>
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
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h> ///NOLINT: required by fmt
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestRunner.hpp>

namespace NES::Systest
{

std::filesystem::path
Query::resultFile(const std::filesystem::path& workingDir, std::string_view testName, const uint64_t queryIdInTestFile)
{
    auto resultDir = workingDir / "results";
    if (not is_directory(resultDir))
    {
        create_directory(resultDir);
        std::cout << "Created working directory: file://" << resultDir.string() << "\n";
    }

    return resultDir / std::filesystem::path(fmt::format("{}_{}.csv", testName, queryIdInTestFile));
}

std::filesystem::path Query::sourceFile(const std::filesystem::path& workingDir, std::string_view testName, const uint64_t sourceId)
{
    auto sourceDir = workingDir / "sources";
    if (not is_directory(sourceDir))
    {
        create_directory(sourceDir);
        std::cout << "Created working directory: file://" << sourceDir.string() << "\n";
    }

    return sourceDir / std::filesystem::path(fmt::format("{}_{}.csv", testName, sourceId));
}

Query::Query(
    TestName name,
    std::string queryDefinition,
    std::filesystem::path sqlLogicTestFile,
    std::expected<LogicalPlan, Exception> queryPlan,
    const uint64_t queryIdInFile,
    std::filesystem::path workingDir,
    std::unordered_map<std::string, std::pair<std::filesystem::path, uint64_t>> sourceNamesToFilepathAndCount,
    SystestParser::Schema sinkSchema,
    std::optional<ExpectedError> expectedError)
    : name(std::move(name))
    , queryDefinition(std::move(queryDefinition))
    , sqlLogicTestFile(std::move(sqlLogicTestFile))
    , queryPlan(std::move(queryPlan))
    , queryIdInFile(queryIdInFile)
    , workingDir(std::move(workingDir))
    , sourceNamesToFilepathAndCount(std::move(sourceNamesToFilepathAndCount))
    , expectedSinkSchema(std::move(sinkSchema))
    , expectedError(expectedError)
{
}

std::filesystem::path Query::resultFile() const
{
    return resultFile(workingDir, name, queryIdInFile);
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

    std::string desiredExtension = fileExtension.has_value() ? toLowerCopy(*fileExtension) : "";

    for (const auto& entry : std::filesystem::recursive_directory_iterator(path, std::filesystem::directory_options::skip_permission_denied)
             | std::views::filter([](auto entry) { return entry.is_regular_file(); }))
    {
        std::string entryExt = toLowerCopy(entry.path().extension().string());

        if (!fileExtension || entryExt == desiredExtension)
        {
            TestFile testfile(entry.path());
            testFiles.insert({entry.path().string(), testfile});
        }
    }
    return testFiles;
}

void loadQueriesFromTestFile(TestFile& testfile, const std::filesystem::path& workingDir, const std::filesystem::path& testDataDir)
{
    auto loadedPlans = loadFromSLTFile(testfile.file, workingDir, testfile.name(), testDataDir);
    uint64_t queryIdInFile = 0;
    std::unordered_set<uint64_t> foundQueries;

    for (const auto& [decomposedPlan, queryDefinition, sinkSchema, sourceNamesToFilepath, expectedError] : loadedPlans)
    {
        if (not testfile.onlyEnableQueriesWithTestQueryNumber.empty())
        {
            for (const auto& testNumber : testfile.onlyEnableQueriesWithTestQueryNumber
                     | std::views::filter([&queryIdInFile](auto testNumber) { return testNumber == queryIdInFile + 1; }))
            {
                foundQueries.insert(queryIdInFile + 1);
                testfile.queries.emplace_back(
                    testfile.name(),
                    queryDefinition,
                    testfile.file,
                    decomposedPlan,
                    queryIdInFile,
                    workingDir,
                    sourceNamesToFilepath,
                    sinkSchema,
                    expectedError);
            }
        }
        else
        {
            testfile.queries.emplace_back(
                testfile.name(),
                queryDefinition,
                testfile.file,
                decomposedPlan,
                queryIdInFile,
                workingDir,
                sourceNamesToFilepath,
                sinkSchema,
                expectedError);
        }
        ++queryIdInFile;
    }

    /// After processing all queries, warn if any specified query number was not found
    for (auto testNumber : testfile.onlyEnableQueriesWithTestQueryNumber)
    {
        if (not foundQueries.contains(testNumber))
        {
            std::cerr << "Warning: Query number " << testNumber << " specified via command line argument but not found in file://"
                      << testfile.file.string() << "\n";
        }
    }
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

TestFile::TestFile(std::filesystem::path file) : file(weakly_canonical(file)), groups(readGroups(*this)) { };

TestFile::TestFile(std::filesystem::path file, std::vector<uint64_t> onlyEnableQueriesWithTestQueryNumber)
    : file(weakly_canonical(file))
    , onlyEnableQueriesWithTestQueryNumber(std::move(onlyEnableQueriesWithTestQueryNumber))
    , groups(readGroups(*this)) { };

std::vector<Query> loadQueries(TestFileMap& testmap, const std::filesystem::path& workingDir, const std::filesystem::path& testDataDir)
{
    std::vector<Query> queries;
    uint64_t loadedFiles = 0;
    for (auto& [testname, testfile] : testmap)
    {
        std::cout << "Loading queries from test file: file://" << testfile.getLogFilePath() << '\n' << std::flush;
        try
        {
            loadQueriesFromTestFile(testfile, workingDir, testDataDir);
            for (auto& query : testfile.queries)
            {
                queries.emplace_back(std::move(query));
            }
            ++loadedFiles;
        }
        catch (const Exception& exception)
        {
            tryLogCurrentException();
            std::cerr << fmt::format("Loading test file://{} failed: {}\n", testfile.getLogFilePath(), exception.what());
        }
    }
    std::cout << "Loaded test files: " << loadedFiles << "/" << testmap.size() << '\n' << std::flush;
    if (loadedFiles != testmap.size())
    {
        std::cerr << "Could not load all test files. Terminating.\n" << std::flush;
        std::exit(1);
    }
    return queries;
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


TestFileMap loadTestFileMap(const Configuration::SystestConfiguration& config)
{
    if (not config.directlySpecifiedTestFiles.getValue().empty()) /// load specifc test file
    {
        auto directlySpecifiedTestFiles = config.directlySpecifiedTestFiles.getValue();

        if (config.testQueryNumbers.empty()) /// case: load all tests
        {
            TestFile testfile = TestFile(directlySpecifiedTestFiles);
            return TestFileMap{{testfile.name(), testfile}};
        }
        else
        { /// case: load a concrete set of tests
            auto scalarTestNumbers = config.testQueryNumbers.getValues();
            std::vector<uint64_t> testNumbers;
            testNumbers.reserve(scalarTestNumbers.size());
            for (const auto& scalarOption : scalarTestNumbers)
            {
                testNumbers.push_back(scalarOption.getValue());
            }

            TestFile testfile = TestFile(directlySpecifiedTestFiles, testNumbers);
            return TestFileMap{{testfile.name(), testfile}};
        }
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

std::chrono::duration<double> RunningQuery::getElapsedTime() const
{
    INVARIANT(not querySummary.runs.empty(), "Query summaries should not be empty!");
    INVARIANT(queryId != INVALID_QUERY_ID, "QueryId should not be invalid");

    const auto lastRun = querySummary.runs.back();
    INVARIANT(lastRun.stop.has_value() && lastRun.running.has_value(), "Query {} has no querySummary timestamps!", queryId);
    return std::chrono::duration_cast<std::chrono::duration<double>>(lastRun.stop.value() - lastRun.running.value());
}

std::string RunningQuery::getThroughput() const
{
    INVARIANT(not querySummary.runs.empty(), "Query summaries should not be empty!");
    INVARIANT(queryId != INVALID_QUERY_ID, "QueryId should not be invalid");

    const auto lastRun = querySummary.runs.back();
    INVARIANT(lastRun.stop.has_value() && lastRun.running.has_value(), "Query {} has no querySummary timestamps!", queryId);
    if (not bytesProcessed.has_value() or not tuplesProcessed.has_value())
    {
        return "";
    }

    /// Calculating the throughput in bytes per second and tuples per second
    const std::chrono::duration<double> duration = lastRun.stop.value() - lastRun.running.value();
    const auto bytesPerSecond = static_cast<double>(bytesProcessed.value()) / duration.count();
    const auto tuplesPerSecond = static_cast<double>(tuplesProcessed.value()) / duration.count();

    auto formatUnits = [](double throughput)
    {
        /// Format throughput in SI units, e.g. 1.234 MB/s instead of 1234000 B/s
        const std::array<std::string, 5> units = {"", "k", "M", "G", "T"};
        uint64_t unitIndex = 0;
        constexpr auto nextUnit = 1000;
        while (throughput >= nextUnit && unitIndex < units.size() - 1)
        {
            throughput /= nextUnit;
            unitIndex++;
        }
        return fmt::format("{:.3f} {}", throughput, units[unitIndex]);
    };
    return fmt::format("{}B/s / {}Tup/s", formatUnits(bytesPerSecond), formatUnits(tuplesPerSecond));
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
