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
#include <filesystem>

namespace NES::Systest
{

std::vector<TestGroup> TestFile::readGroups()
{
    std::vector<TestGroup> groups;

    for (const auto& testFile : testFiles)
    {
        std::ifstream file(testFile.path);
        std::string line;

        if (file.is_open())
        {
            while (std::getline(file, line))
            {
                if (line.starts_with("# groups:"))
                {
                    std::string groupsStr = line.substr(9);
                    groupsStr.erase(std::remove(groupsStr.begin(), groupsStr.end(), '['), groupsStr.end());
                    groupsStr.erase(std::remove(groupsStr.begin(), groupsStr.end(), ']'), groupsStr.end());

                    std::istringstream iss(groupsStr);
                    std::string groupName;

                    while (std::getline(iss, groupName, ','))
                    {
                        groupName.erase(std::remove_if(groupName.begin(), groupName.end(), ::isspace), groupName.end());

                        auto it = std::find_if(groups.begin(), groups.end(),
                                               [&](const TestGroup& g) { return g.name == groupName; });

                        if (it != groups.end())
                        {
                            it->files.push_back({testFile.path});
                        }
                        else
                        {
                            groups.push_back({groupName, {testFile.path}});
                        }
                    }
                    break;
                }
            }
            file.close();
        }
    }

    return groups;
}

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

}