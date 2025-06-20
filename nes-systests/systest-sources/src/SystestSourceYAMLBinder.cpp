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

#include <SystestSources/SystestSourceYAMLBinder.hpp>

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h> /// NOLINT(misc-include-cleaner)
#include <ErrorHandling.hpp>


namespace NES::SystestSourceYAMLBinder
{
struct SchemaField;
}
namespace
{
NES::DataType stringToFieldType(const std::string& fieldNodeType)
{
    try
    {
        return NES::DataTypeProvider::provideDataType(fieldNodeType);
    }
    catch (std::runtime_error& e)
    {
        throw NES::SLTWrongSchema("Found invalid logical source configuration. {} is not a proper datatype.", fieldNodeType);
    }
}
}

namespace YAML
{

template <>
struct convert<NES::SystestSourceYAMLBinder::SchemaField>
{
    static bool decode(const Node& node, NES::SystestSourceYAMLBinder::SchemaField& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.type = stringToFieldType(node["type"].as<std::string>());
        return true;
    }
};
template <>
struct convert<NES::SystestSourceYAMLBinder::Sink>
{
    static bool decode(const Node& node, NES::SystestSourceYAMLBinder::Sink& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.type = node["type"].as<std::string>();
        rhs.config = node["config"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};
template <>
struct convert<NES::SystestSourceYAMLBinder::LogicalSource>
{
    static bool decode(const Node& node, NES::SystestSourceYAMLBinder::LogicalSource& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.schema = node["schema"].as<std::vector<NES::SystestSourceYAMLBinder::SchemaField>>();
        return true;
    }
};
template <>
struct convert<NES::SystestSourceYAMLBinder::PhysicalSource>
{
    static bool decode(const Node& node, NES::SystestSourceYAMLBinder::PhysicalSource& rhs)
    {
        rhs.logical = node["logical"].as<std::string>();
        rhs.parserConfig = node["parserConfig"].as<std::unordered_map<std::string, std::string>>();
        rhs.sourceConfig = node["sourceConfig"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};
template <>
struct convert<NES::SystestSourceYAMLBinder::QueryConfig>
{
    static bool decode(const Node& node, NES::SystestSourceYAMLBinder::QueryConfig& rhs)
    {
        const auto sink = node["sink"].as<NES::SystestSourceYAMLBinder::Sink>();
        rhs.sinks.emplace(sink.name, sink);
        rhs.logical = node["logical"].as<std::vector<NES::SystestSourceYAMLBinder::LogicalSource>>();
        rhs.physical = node["physical"].as<std::vector<NES::SystestSourceYAMLBinder::PhysicalSource>>();
        rhs.query = node["query"].as<std::string>();
        return true;
    }
};

}

namespace NES::SystestSourceYAMLBinder
{

SchemaField::SchemaField(std::string name, const std::string& typeName) : SchemaField(std::move(name), stringToFieldType(typeName))
{
}

SchemaField::SchemaField(std::string name, DataType type) : name(std::move(name)), type(std::move(type))
{
}

PhysicalSource loadSystestPhysicalSourceFromYAML(
    std::string logicalSourceName, const std::filesystem::path& sourceFilePath, const std::filesystem::path& inputFormatterFilePath)
{
    std::ifstream sourceConfigFile(sourceFilePath);
    if (not sourceConfigFile)
    {
        throw InvalidConfigParameter("Couldn't load source config from: {}", sourceFilePath.string());
    }
    std::ifstream inputFormatterConfigFile(inputFormatterFilePath);
    if (not inputFormatterConfigFile)
    {
        throw InvalidConfigParameter("Couldn't load input formatter config from: {}", inputFormatterFilePath.string());
    }

    try
    {
        PhysicalSource systestPhysicalSource{};
        systestPhysicalSource.logical = std::move(logicalSourceName);
        systestPhysicalSource.sourceConfig = YAML::Load(sourceConfigFile).as<std::unordered_map<std::string, std::string>>();
        auto typeNode = systestPhysicalSource.sourceConfig.extract("type");
        if (typeNode.empty())
        {
            throw InvalidConfigParameter("No source type specified in source config: {}");
        }
        systestPhysicalSource.type = typeNode.mapped();
        systestPhysicalSource.parserConfig = YAML::Load(inputFormatterConfigFile).as<std::unordered_map<std::string, std::string>>();
        return systestPhysicalSource;
    }
    catch (const YAML::ParserException& pex)
    {
        throw QueryDescriptionNotParsable("{}", pex.what());
    }
}
}
