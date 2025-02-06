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

#include <fstream>
#include <iostream>
#include <ostream>
#include <ranges>
#include <vector>
#include <SystestRunner.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

TestFileMap discoverTestsRecursively(const std::filesystem::path& path, const std::optional<std::string>& fileExtension)
{
    TestFileMap testFiles;

    auto toLowerCopy = [](const std::string& str)
    {
        std::string lowerStr = str;
        std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(), ::tolower);
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

void loadQueriesFromTestFile(TestFile& testfile, const std::filesystem::path& workingDir)
{
    auto loadedPlans = loadFromSLTFile(testfile.file, workingDir, testfile.name());
    uint64_t queryIdInFile = 0;
    for (const auto& [decomposedPlan, queryDefinition, sinkSchema] : loadedPlans)
    {
        if (not testfile.onlyEnableQueriesWithTestQueryNumber.empty())
        {
            for (const auto& testNumber : testfile.onlyEnableQueriesWithTestQueryNumber
                     | std::views::filter([&queryIdInFile](auto testNumber) { return testNumber == queryIdInFile + 1; }))
            {
                testfile.queries.emplace_back(
                    testfile.name(), queryDefinition, testfile.file, decomposedPlan, queryIdInFile, workingDir, sinkSchema);
            }
        }
        else
        {
            testfile.queries.emplace_back(
                testfile.name(), queryDefinition, testfile.file, decomposedPlan, queryIdInFile, workingDir, sinkSchema);
        }
        ++queryIdInFile;
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

TestFile::TestFile(std::filesystem::path file) : file(weakly_canonical(file)), groups(readGroups(*this)) {};

TestFile::TestFile(std::filesystem::path file, std::vector<uint64_t> onlyEnableQueriesWithTestQueryNumber)
    : file(weakly_canonical(file))
    , onlyEnableQueriesWithTestQueryNumber(std::move(onlyEnableQueriesWithTestQueryNumber))
    , groups(readGroups(*this)) {};

std::vector<Query> loadQueries(TestFileMap& testmap, const std::filesystem::path& workingDir)
{
    std::vector<Query> queries;
    for (auto& [testname, testfile] : testmap)
    {
        std::cout << "Loading queries from test file: file://" << testfile.file.string() << '\n' << std::flush;
        loadQueriesFromTestFile(testfile, workingDir);
        for (auto& query : testfile.queries)
        {
            queries.emplace_back(std::move(query));
        }
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
        testGroups.push_back(TestGroupFiles{groupName, files});
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

    if (not config.testGroup.getValue().empty()) /// load specific test group
    {
        auto testsDiscoverDir = config.testsDiscoverDir.getValue();
        auto testFileExtension = config.testFileExtension.getValue();
        auto testMap = discoverTestsRecursively(testsDiscoverDir, testFileExtension);

        auto groupname = config.testGroup.getValue();

        TestFileMap resultMap;

        for (const auto& [testname, testfile] : testMap)
        {
            for (const auto& groups : testfile.groups | std::views::filter([&groupname](auto& groups) { return groups == groupname; }))
            {
                resultMap.emplace(testname, testfile);
            }
        }
        return resultMap;
    }

    /// laod from test dir
    auto testsDiscoverDir = config.testsDiscoverDir.getValue();
    auto testFileExtension = config.testFileExtension.getValue();
    return discoverTestsRecursively(testsDiscoverDir, testFileExtension);
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
