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
#include <filesystem>
#include <vector>
#include <SystestConfiguration.hpp>
#include <SystestRunner.hpp>

namespace NES::Systest
{
//bool isInGroup(const std::filesystem::path& testFile, const std::string& expectedGroup); // discover test files
std::vector<TestFile> discoverTestFiles(const Configuration::SystestConfiguration& config); // collectQueriesToTest, print test list, load test files
std::pair<std::vector<std::filesystem::path>, std::vector<uint64_t>> getMatchingTestFiles(const std::string& testname, const Configuration::SystestConfiguration& config); // get cache files
//void clearCacheDir(const Configuration::SystestConfiguration& config); // load cache files
//void printTestList(const Configuration::SystestConfiguration& config); // load config
//void removeAllFilesInDirectory(const std::filesystem::path& dirPath); // before running tests
}