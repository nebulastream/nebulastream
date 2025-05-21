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
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <Common/DataTypes/DataType.hpp>

namespace NES::CLI
{

struct SchemaField
{
    SchemaField(std::string name, std::string typeName);
    SchemaField(std::string name, std::shared_ptr<DataType> type);
    SchemaField() = default;

    std::string name;
    std::shared_ptr<DataType> type;
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
}

namespace NES::Distributed::Config
{

using ConnectionIdentifier = std::string;
using ChannelIdentifier = std::string;

struct Links
{
    std::vector<ConnectionIdentifier> upstreams;
    std::vector<ConnectionIdentifier> downstreams;
};

struct Node
{
    std::string grpc;
    ConnectionIdentifier connection;
    std::vector<CLI::Sink> sinks;
    std::vector<CLI::PhysicalSource> physical;
    size_t capacity;
    Links links;
};

struct Topology
{
    std::vector<Node> nodes;
    std::vector<CLI::LogicalSource> logical;
    static Topology from(CLI::QueryConfig config, ConnectionIdentifier singleNodeServer);
};
}
