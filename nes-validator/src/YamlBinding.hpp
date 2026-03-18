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

#include <string>
#include <unordered_map>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <yaml-cpp/yaml.h>

namespace NES::Validator
{

/// Types used as YAML deserialization targets for topology configuration.
/// Ported from nes-frontend/apps/cli/CLIStarter.cpp (NES::CLI namespace).

struct SchemaField
{
    std::string name;
    DataType type;
};

struct LogicalSourceConfig
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct PhysicalSourceConfig
{
    std::string logical;
    std::string host;
    std::string type;
    std::unordered_map<std::string, std::string> parserConfig;
    std::unordered_map<std::string, std::string> sourceConfig;
};

struct SinkConfig
{
    std::string name;
    std::vector<SchemaField> schema;
    std::string type;
    std::unordered_map<std::string, std::string> config;
    std::unordered_map<std::string, std::string> parserConfig;
};

struct TopologyConfig
{
    std::vector<std::string> query;
    std::vector<SinkConfig> sinks;
    std::vector<LogicalSourceConfig> logical;
    std::vector<PhysicalSourceConfig> physical;
};

} // namespace NES::Validator

/// yaml-cpp convert<> specializations for topology types.
namespace YAML
{

template <>
struct convert<NES::Validator::SchemaField>
{
    static bool decode(const Node& node, NES::Validator::SchemaField& rhs);
};

template <>
struct convert<NES::Validator::SinkConfig>
{
    static bool decode(const Node& node, NES::Validator::SinkConfig& rhs);
};

template <>
struct convert<NES::Validator::LogicalSourceConfig>
{
    static bool decode(const Node& node, NES::Validator::LogicalSourceConfig& rhs);
};

template <>
struct convert<NES::Validator::PhysicalSourceConfig>
{
    static bool decode(const Node& node, NES::Validator::PhysicalSourceConfig& rhs);
};

template <>
struct convert<NES::Validator::TopologyConfig>
{
    static bool decode(const Node& node, NES::Validator::TopologyConfig& rhs);
};

} // namespace YAML
