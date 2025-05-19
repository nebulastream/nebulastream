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
#include <istream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <SystestSources/SourceTypes.hpp>

namespace NES
{
class LogicalPlan;
}

namespace NES::CLI
{

struct SchemaField
{
    SchemaField(std::string name, const std::string& typeName);
    SchemaField(std::string name, NES::DataType type);
    SchemaField() = default;

    std::string name;
    NES::DataType type;
};

struct Sink
{
    std::string name;
    std::string type;
    std::unordered_map<std::string, std::string> config;
};

struct LogicalSource
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct QueryConfig
{
    std::string query;
    std::unordered_map<std::string, Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<SystestPhysicalSource> physical;
};

std::unique_ptr<LogicalPlan> loadFromYAMLFile(const std::filesystem::path& file);
SystestPhysicalSource loadFromYAMLSource(const std::filesystem::path& file);
SystestPhysicalSource loadSystestPhysicalSourceFromYAML(
    std::string logicalSourceName, const std::filesystem::path& sourceFilePath, const std::filesystem::path& inputFormatterFilePath);
std::unique_ptr<LogicalPlan> loadFrom(std::istream& inputStream);
std::unique_ptr<LogicalPlan> createFullySpecifiedQueryPlan(const QueryConfig& config);
}
