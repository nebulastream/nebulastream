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

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>


namespace NES::CLI
{

using GrpcAddr = std::string;
using HostAddr = std::string;
using ChannelId = std::string;

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
    HostAddr host;
    std::unordered_map<std::string, std::string> config;
};

struct PhysicalSource
{
    std::string logicalSource;
    HostAddr host;
    std::unordered_map<std::string, std::string> parserConfig;
    std::unordered_map<std::string, std::string> sourceConfig;
};

struct LogicalSource
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct NodeConfig
{
    HostAddr host;
    GrpcAddr grpc;
    size_t capacity;
    std::vector<HostAddr> downstreamNodes;
};

struct QueryConfig
{
    std::string query;
    Sink sink;
    std::vector<LogicalSource> logicalSources;
    std::vector<PhysicalSource> physicalSources;
    std::vector<NodeConfig> workerNodes;
};
}
