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

#include <istream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp> /// NOLINT(misc-include-cleaner)
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <experimental/propagate_const>

namespace NES::CLI
{

/// In NES::CLI SchemaField, Sink, LogicalSource, PhysicalSource and QueryConfig are used as target for the YAML parser.
/// These types should not be used anywhere else in NES; instead we use the bound and validated types, such as NES::LogicalSource and NES::SourceDescriptor.
struct SchemaField
{
    SchemaField(std::string name, const std::string& typeName);
    SchemaField(std::string name, DataType type);
    SchemaField() = default;

    std::string name;
    DataType type;
};

struct Sink
{
    std::string name;
    std::vector<SchemaField> schema;
    std::string type;
    std::unordered_map<std::string, std::string> config;
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

struct QueryConfig
{
    std::string query;
    std::vector<Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
};

/// Validated and bound content of a YAML file, the members are not specific to the yaml-binder anymore but our "normal" types.
/// If something goes wrong, for example, a source is declared twice, the binder will throw an exception.
struct BoundQueryConfig
{
    LogicalPlan plan;
    /// This should be changed to bound sinks once there is a sink catalog
    std::unordered_map<std::string, std::shared_ptr<Sinks::SinkDescriptor>> sinks;
    std::vector<NES::LogicalSource> logicalSources;
    std::vector<SourceDescriptor> sourceDescriptors;
};

class YAMLBinder
{
    std::experimental::propagate_const<std::shared_ptr<SourceCatalog>> sourceCatalog;
    std::experimental::propagate_const<std::shared_ptr<SinkCatalog>> sinkCatalog;

public:
    explicit YAMLBinder(std::shared_ptr<SourceCatalog> sourceCatalog, std::shared_ptr<SinkCatalog> sinkCatalog)
        : sourceCatalog(std::move(sourceCatalog)), sinkCatalog(std::move(sinkCatalog))
    {
    }
    LogicalPlan parseAndBind(std::istream& inputStream);

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    [[nodiscard]] Schema bindSchema(const std::vector<SchemaField>& attributeFields) const;
    std::vector<NES::LogicalSource> bindRegisterLogicalSources(const std::vector<LogicalSource>& unboundSources);
    std::vector<SourceDescriptor> bindRegisterPhysicalSources(const std::vector<PhysicalSource>& unboundSources);
    std::vector<Sinks::SinkDescriptor> bindRegisterSinks(const std::vector<Sink>& unboundSinks);
};

}
