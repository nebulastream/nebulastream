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
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <DataTypes/DataType.hpp>


namespace NES
{

static constexpr std::string_view SYSTEST_FILE_PATH_PARAMETER = "filePath";

struct InlineGeneratorConfiguration
{
    std::vector<std::string> fieldSchema;
    std::unordered_map<std::string, std::string> options;
};

enum class TestDataIngestionType : uint8_t
{
    INLINE,
    FILE,
    GENERATOR
};

struct SystestAttachSource
{
    std::string sourceType;
    std::filesystem::path sourceConfigurationPath;
    std::string inputFormatterType;
    std::filesystem::path inputFormatterConfigurationPath;
    std::string logicalSourceName;
    TestDataIngestionType testDataIngestionType;
    std::optional<std::vector<std::string>> tuples;
    std::optional<std::filesystem::path> fileDataPath;
    std::shared_ptr<std::vector<std::jthread>> serverThreads;
    std::optional<InlineGeneratorConfiguration> inlineGeneratorConfiguration;
};

}

namespace NES::SystestSourceYAMLBinder
{

struct SchemaField
{
    SchemaField(std::string name, const std::string& typeName);
    SchemaField(std::string name, DataType type);
    SchemaField() = default;

    std::string name;
    DataType type;
};

struct LogicalSource
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct PhysicalSource
{
    std::string logical;
    std::unordered_map<std::string, std::string> parserConfig;
    std::unordered_map<std::string, std::string> sourceConfig;
};

struct Sink
{
    std::string name;
    std::string type;
    std::unordered_map<std::string, std::string> config;
};

struct QueryConfig
{
    std::string query;
    std::unordered_map<std::string, Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
};

}
