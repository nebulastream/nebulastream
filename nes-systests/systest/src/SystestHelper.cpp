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

#include <SystestHelper.hpp>
#include <regex>

namespace NES::Systest
{

bool isInGroup(const std::filesystem::path& testFile, const std::string& expectedGroup)
{
    std::ifstream file(testFile);
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
                std::string group;
                while (std::getline(iss, group, ','))
                {
                    group.erase(std::remove_if(group.begin(), group.end(), ::isspace), group.end());
                    if (expectedGroup == group)
                    {
                        return true;
                    }
                }
                break;
            }
        }
        file.close();
    }
    return false;
}

std::pair<std::vector<std::filesystem::path>, std::vector<uint64_t>> getMatchingTestFiles(const std::string& testname, const Configuration::SystestConfiguration& config)
{
    std::pair<std::vector<std::filesystem::path>, std::vector<uint64_t>> matchingFiles;
    std::regex pattern(testname + R"_(_(\d+)\.pb)_");
    for (const auto& entry : std::filesystem::directory_iterator(config.cacheDir.getValue()))
    {
        std::smatch match;
        std::string filename = entry.path().filename().string();

        if (entry.is_regular_file() && std::regex_match(filename, match, pattern))
        {
            uint64_t digit = std::stoull(match[1].str());
            if (not config.testNumbers.empty()) /// if specified, only run specific test numbers
            {
                for (const auto& testNumber : config.testNumbers.getValues())
                {
                    if (testNumber.getValue() == digit + 1)
                    {
                        matchingFiles.first.emplace_back(entry.path());
                        matchingFiles.second.emplace_back(digit);
                    }
                }
            }
            else
            {
                matchingFiles.first.emplace_back(entry.path());
                matchingFiles.second.emplace_back(digit);
            }
        }
    }
    return matchingFiles;
}

void removeFilesInDirectory(const std::filesystem::path& dirPath, const std::string& extension)
{
    if (is_directory(dirPath))
    {
        for (const auto& entry : std::filesystem::directory_iterator(dirPath))
        {
            if (entry.is_regular_file() && entry.path().extension() == extension)
            {
                std::filesystem::remove(entry.path());
            }
        }
    }
    else
    {
        std::cerr << "The specified path is not a directory.\n";
    }
}

void removeFilesInDirectory(const std::filesystem::path& dirPath)
{
    if (is_directory(dirPath))
    {
        for (const auto& entry : std::filesystem::directory_iterator(dirPath))
        {
            if (entry.is_regular_file())
            {
                std::filesystem::remove(entry.path());
            }
        }
    }
    else
    {
        std::cerr << "The specified path is not a directory.\n";
    }
}

}