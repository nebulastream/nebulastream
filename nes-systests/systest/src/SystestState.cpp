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

namespace NES::Systest
{

TestFileMap discoverTestsRecursively(const std::filesystem::path& path, const std::optional<std::string>& fileExtension)
{
    TestFileMap testFiles;

    auto toLower = [](const std::string& str)
    {
        std::string lowerStr = str;
        std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(), ::tolower);
        return lowerStr;
    };

    std::string desiredExtension = fileExtension.has_value() ? toLower(*fileExtension) : "";

    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(path, std::filesystem::directory_options::skip_permission_denied))
    {
        if (entry.is_regular_file())
        {
            std::string entryExt = toLower(entry.path().extension().string());

            if (!fileExtension || entryExt == desiredExtension)
            {
                TestFile testfile(entry.path());
                testFiles.insert({entry.path().string(), testfile});
            }
        }
    }
    return testFiles;
}

void loadQueriesFromTestFile(TestFile& testfile, const std::filesystem::path& resultDir)
{
    auto loadedPlans = loadFromSLTFile(testfile.file, resultDir, testfile.name());
    uint64_t queryNrInFile = 0;
    for (const auto& plan : loadedPlans)
    {
        if (not testfile.onlyEnableQueriesWithId.empty())
        {
            for (const auto& testNumber : testfile.onlyEnableQueriesWithId)
            {
                if (testNumber == queryNrInFile + 1)
                {
                    testfile.queries.emplace_back(testfile.name(), testfile.file, plan, queryNrInFile);
                }
            }
        }
        else
        {
            testfile.queries.emplace_back(testfile.name(), testfile.file, plan, queryNrInFile);
        }
        queryNrInFile++;
    }
}

std::vector<TestGroup> TestFile::readGroups()
{
    std::vector<TestGroup> groups;
    std::ifstream ifstream(this->file);
    std::string line;
    if (ifstream.is_open())
    {
        while (std::getline(ifstream, line))
        {
            if (line.starts_with("# groups:"))
            {
                std::string groupsStr = line.substr(9);
                groupsStr.erase(std::remove(groupsStr.begin(), groupsStr.end(), '['), groupsStr.end());
                groupsStr.erase(std::remove(groupsStr.begin(), groupsStr.end(), ']'), groupsStr.end());
                std::istringstream iss(groupsStr);
                std::string group;
                while (std::getline(iss, group, ','))
                {
                    group.erase(std::remove_if(group.begin(), group.end(), ::isspace), group.end());
                    groups.emplace_back(group);
                }
                break;
            }
        }
        ifstream.close();
    }
    return groups;
}

std::vector<Query> loadQueries(TestFileMap&& testmap, const std::filesystem::path& resultDir)
{
    std::vector<Query> queries;

    for (auto& [testname, testfile] : testmap)
    {
        loadQueriesFromTestFile(testfile, resultDir);
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
    for (const auto& [groupName, files] : groupFilesMap)
    {
        testGroups.push_back(TestGroupFiles{groupName, files});
    }
    return testGroups;
}


TestFileMap loadTestFileMap(const Configuration::SystestConfiguration& config)
{
    if (not config.directlySpecifiedTestsFiles.getValue().empty()) /// load specifc test file
    {
        auto directlySpecifiedTestsFiles = config.directlySpecifiedTestsFiles.getValue();

        if (config.testNumbers.empty()) /// case: load all tests
        {
            TestFile testfile = TestFile(directlySpecifiedTestsFiles);
            return TestFileMap{{testfile.name(), testfile}};
        }
        else
        { /// case: load a concrete set of tests
            auto scalarTestNumbers = config.testNumbers.getValues();
            std::vector<uint64_t> testNumbers;
            testNumbers.reserve(scalarTestNumbers.size());
            for (const auto& scalarOption : scalarTestNumbers)
            {
                testNumbers.push_back(scalarOption.getValue());
            }

            TestFile testfile = TestFile(directlySpecifiedTestsFiles, testNumbers);
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
            for (const auto& groups : testfile.groups)
            {
                if (groups == groupname)
                {
                    resultMap.emplace(testname, testfile);
                }
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
            for (const auto& testGroup : testGroups)
            {
                os << "\t" << testGroup.name << "\n";
                for (const auto& filename : testGroup.files)
                {
                    os << "\t\tfile://" << filename.c_str() << "\n";
                }
            }
        }
    }
    return os;
}

}
