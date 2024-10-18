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

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <filesystem>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <SystestConfiguration.hpp>

namespace NES::Systest
{

using TestName = std::string;
using TestGroup = std::string;

struct Query
{
    Query(std::string name) : name(std::move(name)) {};

    const std::string name;

private:
    DecomposedQueryPlanPtr queryPlan;
    const std::filesystem::path cacheFile;
    uint64_t queryId;
};

class TestFile
{
public:
    TestFile(std::filesystem::path file) : file(std::move(file)), groups(readGroups()), queries(readQueries()) {};

    std::string name(){
        return file.stem().string();
    }

    const std::filesystem::path file;
    const std::vector<TestGroup> groups;
    const std::vector<Query> queries;

private:
    std::vector<Query> readQueries();
    std::vector<TestGroup> readGroups();
};

using TestFileMap = std::unordered_map<TestName, TestFile>;

TestFileMap loadTestFileMap(const Configuration::SystestConfiguration& config)
{
    TestFileMap testFiles;
    if (not config.directlySpecifiedTestsFiles.getValue().empty()) /// load specifc test file
    {
        auto testPath = config.directlySpecifiedTestsFiles.getValue();
        testFiles.emplace_back(testFile(testPath));
    }
    else if (not config.testGroup.getValue().empty()) /// load specific test group
    {
        auto testsDiscoverDir = config.testsDiscoverDir.getValue();
        auto testFileExtension = config.testFileExtension.getValue();
        auto groupname = config.testGroup.getValue();
        for (const auto& entry : std::filesystem::recursive_directory_iterator(std::filesystem::path(testsDiscoverDir)))
        {
            auto testname = entry.path().stem().string();
            if (entry.is_regular_file() && entry.path().extension() == testFileExtension && isInGroup(entry, groupname))
            {
                TestGroup group = {groupname, {testname, {entry.path()}}};
                TestFile file = {testname, canonical(entry.path()), {},{group}};
                testFiles.push_back(file);
            }
        }
    }
    else /// laod from test dir
    {
        auto testsDiscoverDir = config.testsDiscoverDir.getValue();
        auto testFileExtension = config.testFileExtension.getValue();
        for (const auto& entry : std::filesystem::recursive_directory_iterator(std::filesystem::path(testsDiscoverDir)))
        {
            if (entry.is_regular_file() && entry.path().extension() == testFileExtension)
            {
                testFiles.push_back({entry.path().stem().string(), canonical(entry.path()), {}, {}});
            }
        }
    }
    return testFiles;
}

std::vector<TestGroup> getGroups(const TestFileMap& testMap){
    std::unordered_map<TestGroup, std::vector<std::filesystem::path>> groups{};
    for (const auto& testFile : testMap)
    {
        for(const auto& group : testFile.second.groups)
        {
            groups.insert(group);
        }
    }
    return std::vector<std::string>(groups.begin(), groups.end());
}

void print(const TestFileMap& testMap)
{
    if (testMap.empty())
    {
        std::cout << "No matching test files found\n";
    }
    else
    {
        std::cout << "Discovered Test Files:\n";
        for (const auto& testFile : testMap)
        {
            std::cout << "\t" << testFile.first << "\tfile://" << testFile.second.file << "\n";
        }

        auto testGroups = getGroups(testMap);
        if (not testGroups.empty())
        {
            std::cout << "\nDiscovered Test Groups:\n";
            for (const auto& testGroup : testGroups)
            {
                std::cout << "\t" << testGroup. << "\n";
                for (const auto& filename : testGroup.files)
                {
                    std::cout << "\t\tfile://" << filename << "\n";
                }
            }
        }
    }
}

}