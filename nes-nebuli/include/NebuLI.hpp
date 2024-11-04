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

#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES::CLI
{

struct SchemaField
{
    std::string name;
    BasicType type;
};

struct LogicalSource
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct PhysicalSource
{
    std::string logical;
    std::map<std::string, std::string> config;
};

struct QueryConfig
{
    std::string query;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
};

DecomposedQueryPlanPtr loadFromYAMLFile(const std::filesystem::path& file);
DecomposedQueryPlanPtr loadFrom(std::istream& inputStream);
DecomposedQueryPlanPtr createFullySpecifiedQueryPlan(const QueryConfig& config);
}
