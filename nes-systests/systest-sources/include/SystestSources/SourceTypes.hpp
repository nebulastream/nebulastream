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

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace NES
{

static constexpr std::string_view SYSTEST_FILE_PATH_PARAMETER = "filePath";
struct SystestPhysicalSource
{
    std::string logical;
    std::unordered_map<std::string, std::string> parserConfig;
    std::unordered_map<std::string, std::string> sourceConfig;
};

enum class TestDataIngestionType : uint8_t
{
    INLINE,
    FILE
};

// Todo: make class and make sure it is initialized correctly and destructed
struct SystestAttachSource
{
    std::string configurationPath;
    std::string logicalSourceName;
    std::string sourceType;
    TestDataIngestionType testDataIngestionType;
    std::optional<std::vector<std::string>> tuples;
    std::optional<std::filesystem::path> fileDataPath;
    std::shared_ptr<std::vector<std::jthread>> serverThreads;

    bool operator==(const SystestAttachSource& other) const = default;
};
}
