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

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>
#include <WorkerCatalog.hpp>

#include <yaml-cpp/yaml.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>

namespace NES::CLI
{

inline DataType stringToFieldType(const std::string& fieldNodeType)
{
    return DataTypeProvider::provideDataType(fieldNodeType);
}

/// In NES::CLI SchemaField, Sink, LogicalSource, PhysicalSource and QueryConfig are used as target for the YAML parser.
/// These types should not be used anywhere else in NES; instead we use the bound and validated types, such as NES::LogicalSource and NES::SourceDescriptor.
struct SchemaField
{
    SchemaField(std::string name, const std::string& dataTypeName) : SchemaField(std::move(name), stringToFieldType(dataTypeName)) { }

    SchemaField(std::string name, DataType dataType) : name(std::move(name)), dataType(std::move(dataType)) { }

    SchemaField() = default;

    std::string name;
    DataType dataType;
};

struct Sink
{
    std::string name;
    std::string type;
    std::vector<SchemaField> schema;
    HostAddr host;
    std::unordered_map<std::string, std::string> config;
};

struct PhysicalSource
{
    std::string logicalSource;
    std::string type;
    HostAddr host;
    std::unordered_map<std::string, std::string> parserConfig;
    std::unordered_map<std::string, std::string> sourceConfig;
};

struct LogicalSource
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct WorkerConfig
{
    HostAddr host;
    GrpcAddr grpc;
    size_t capacity;
    std::vector<HostAddr> downstreamNodes;
};

struct ClusterConfig
{
    std::vector<WorkerConfig> nodes;
};

struct QueryConfig
{
    std::string query;
    std::vector<Sink> sinks;
    std::vector<LogicalSource> logicalSources;
    std::vector<PhysicalSource> physicalSources;
    std::vector<WorkerConfig> workerNodes;
};
}

namespace YAML
{

template <>
struct convert<NES::CLI::WorkerConfig>
{
    static bool decode(const YAML::Node& node, NES::CLI::WorkerConfig& workerConfig)
    {
        workerConfig.host = node["host"].as<NES::HostAddr>();
        workerConfig.grpc = node["grpc"].as<NES::GrpcAddr>();
        workerConfig.capacity = node["capacity"].as<size_t>();
        workerConfig.downstreamNodes = node["downstreamNodes"].as<std::vector<NES::HostAddr>>(std::vector<NES::HostAddr>{});
        return true;
    }
};

template <>
struct convert<NES::CLI::SchemaField>
{
    static bool decode(const YAML::Node& node, NES::CLI::SchemaField& schemaField)
    {
        schemaField.name = node["name"].as<std::string>();
        schemaField.dataType = NES::CLI::stringToFieldType(node["dataType"].as<std::string>());
        return true;
    }
};

template <>
struct convert<NES::CLI::Sink>
{
    static bool decode(const YAML::Node& node, NES::CLI::Sink& sink)
    {
        sink.name = node["name"].as<std::string>();
        sink.type = node["type"].as<std::string>();
        sink.schema = node["schema"].as<std::vector<NES::CLI::SchemaField>>();
        sink.host = node["host"].as<NES::HostAddr>();
        sink.config = node["config"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::LogicalSource>
{
    static bool decode(const YAML::Node& node, NES::CLI::LogicalSource& logicalSource)
    {
        logicalSource.name = node["name"].as<std::string>();
        logicalSource.schema = node["schema"].as<std::vector<NES::CLI::SchemaField>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::PhysicalSource>
{
    static bool decode(const YAML::Node& node, NES::CLI::PhysicalSource& physicalSource)
    {
        physicalSource.logicalSource = node["logicalSource"].as<std::string>();
        physicalSource.type = node["type"].as<std::string>();
        physicalSource.host = node["host"].as<NES::HostAddr>();
        physicalSource.parserConfig = node["parserConfig"].as<std::unordered_map<std::string, std::string>>();
        physicalSource.sourceConfig = node["sourceConfig"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::ClusterConfig>
{
    static bool decode(const YAML::Node& node, NES::CLI::ClusterConfig& clusterConfig)
    {
        clusterConfig.nodes = node["workers"].as<std::vector<NES::CLI::WorkerConfig>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const YAML::Node& node, NES::CLI::QueryConfig& config)
    {
        config.query = node["query"].as<std::string>();
        config.sinks = node["sinks"].as<std::vector<NES::CLI::Sink>>(std::vector<NES::CLI::Sink>{});
        config.logicalSources = node["logicalSources"].as<std::vector<NES::CLI::LogicalSource>>(std::vector<NES::CLI::LogicalSource>{});
        config.physicalSources = node["physicalSources"].as<std::vector<NES::CLI::PhysicalSource>>(std::vector<NES::CLI::PhysicalSource>{});
        config.workerNodes = node["workerNodes"].as<std::vector<NES::CLI::WorkerConfig>>(std::vector<NES::CLI::WorkerConfig>{});
        return true;
    }
};

}
