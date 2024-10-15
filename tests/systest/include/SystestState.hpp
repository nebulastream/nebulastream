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
    std::string name;
    DecomposedQueryPlanPtr queryPlan;
    const std::filesystem::path cacheFile;
    uint64_t queryId;
};

/// Representation of a system test file
class TestFile
{
public:
    TestFile(std::filesystem::path file) : file(file), groups(readGroups()), queries(readQueries()) {};

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

TestFileMap loadTestFileMap(const Configuration::SystestConfiguration& config);

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