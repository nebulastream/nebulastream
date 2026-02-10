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

#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace NES
{

struct PhysicalSourceConfig
{
    std::string logical;
    std::string type;
    std::unordered_map<std::string, std::string> parserConfig;
    std::unordered_map<std::string, std::string> sourceConfig;
};

class SourceDataProvider
{
public:
    static PhysicalSourceConfig provideFileDataSource(
        PhysicalSourceConfig initialPhysicalSourceConfig,
        std::shared_ptr<std::vector<std::jthread>> serverThreads,
        std::filesystem::path testFilePath);
    static PhysicalSourceConfig provideInlineDataSource(
        PhysicalSourceConfig initialPhysicalSourceConfig,
        std::vector<std::string> tuples,
        std::shared_ptr<std::vector<std::jthread>> serverThreads,
        std::filesystem::path testFilePath);
};

}
