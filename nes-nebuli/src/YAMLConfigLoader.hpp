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
#include <yaml-cpp/yaml.h>
#include <Configuration.hpp>
#include <Common/DataTypes/DataType.hpp>


namespace NES::CLI
{
std::shared_ptr<DataType> stringToFieldType(const std::string& fieldNodeType);
}

namespace YAML
{

template <>
struct convert<NES::Distributed::Config::Links>
{
    static bool decode(const Node& node, NES::Distributed::Config::Links& rhs)
    {
        rhs.downstreams = node["downstreams"].as<std::vector<NES::Distributed::Config::ConnectionIdentifier>>(
            std::vector<NES::Distributed::Config::ConnectionIdentifier>{});
        rhs.upstreams = node["upstreams"].as<std::vector<NES::Distributed::Config::ConnectionIdentifier>>(
            std::vector<NES::Distributed::Config::ConnectionIdentifier>{});
        return true;
    }
};

template <>
struct convert<NES::Distributed::Config::Node>
{
    static bool decode(const Node& node, NES::Distributed::Config::Node& rhs)
    {
        rhs.links = node["links"].as<NES::Distributed::Config::Links>(NES::Distributed::Config::Links{});
        rhs.connection = node["connection"].as<NES::Distributed::Config::ConnectionIdentifier>();
        rhs.physical = node["physical"].as<std::vector<NES::CLI::PhysicalSource>>(std::vector<NES::CLI::PhysicalSource>{});
        rhs.grpc = node["grpc"].as<std::string>();
        rhs.sinks = node["sinks"].as<std::vector<NES::CLI::Sink>>(std::vector<NES::CLI::Sink>{});
        rhs.capacity = node["capacity"].as<size_t>();
        return true;
    }
};

template <>
struct convert<NES::Distributed::Config::Topology>
{
    static bool decode(const YAML::Node& node, NES::Distributed::Config::Topology& rhs)
    {
        rhs.logical = node["logical"].as<std::vector<NES::CLI::LogicalSource>>();
        rhs.nodes = node["nodes"].as<std::vector<NES::Distributed::Config::Node>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::SchemaField>
{
    static bool decode(const Node& node, NES::CLI::SchemaField& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.type = NES::CLI::stringToFieldType(node["type"].as<std::string>());
        return true;
    }
};
template <>
struct convert<NES::CLI::Sink>
{
    static bool decode(const Node& node, NES::CLI::Sink& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.type = node["type"].as<std::string>();
        rhs.config = node["config"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};
template <>
struct convert<NES::CLI::LogicalSource>
{
    static bool decode(const Node& node, NES::CLI::LogicalSource& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.schema = node["schema"].as<std::vector<NES::CLI::SchemaField>>();
        return true;
    }
};
template <>
struct convert<NES::CLI::PhysicalSource>
{
    static bool decode(const Node& node, NES::CLI::PhysicalSource& rhs)
    {
        rhs.logical = node["logical"].as<std::string>();
        rhs.parserConfig = node["parserConfig"].as<std::unordered_map<std::string, std::string>>();
        rhs.sourceConfig = node["sourceConfig"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};
template <>
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const Node& node, NES::CLI::QueryConfig& rhs)
    {
        rhs.sinks = node["sink"].as<std::vector<NES::CLI::Sink>>(std::vector<NES::CLI::Sink>{});
        rhs.logical = node["logical"].as<std::vector<NES::CLI::LogicalSource>>(std::vector<NES::CLI::LogicalSource>{});
        rhs.physical = node["physical"].as<std::vector<NES::CLI::PhysicalSource>>(std::vector<NES::CLI::PhysicalSource>{});
        rhs.query = node["query"].as<std::string>();
        return true;
    }
};

}
