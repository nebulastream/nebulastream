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

#include <YAML/YAMLBinder.hpp>

#include <istream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h> /// NOLINT(misc-include-cleaner)
#include <ErrorHandling.hpp>

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
struct convert<NES::CLI::SchemaField>
{
    static bool decode(const Node& node, NES::CLI::SchemaField& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.type = stringToFieldType(node["type"].as<std::string>());
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
        rhs.schema = node["schema"].as<std::vector<NES::CLI::SchemaField>>();
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
        rhs.type = node["type"].as<std::string>();
        rhs.parserConfig = node["parser_config"].as<std::unordered_map<std::string, std::string>>();
        rhs.sourceConfig = node["source_config"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};


template <>
struct convert<NES::CLI::Model>
{
    static bool decode(const Node& node, NES::CLI::Model& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.inputs
            = node["inputs"].as<std::vector<std::string>>() | std::views::transform(stringToFieldType) | std::ranges::to<std::vector>();
        rhs.outputs = node["outputs"].as<std::vector<NES::CLI::SchemaField>>();
        rhs.path = node["path"].as<std::string>();
        return true;
    }
};

template <>
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const Node& node, NES::CLI::QueryConfig& rhs)
    {
        rhs.sinks = node["sinks"].as<std::vector<NES::CLI::Sink>>();
        rhs.logical = node["logical"].as<std::vector<NES::CLI::LogicalSource>>();
        rhs.physical = node["physical"].as<std::vector<NES::CLI::PhysicalSource>>();
        rhs.query = node["query"].as<std::string>();
        if (node["models"].IsDefined())
        {
            rhs.models = node["models"].as<std::vector<NES::CLI::Model>>();
        }
        return true;
    }
};
}

namespace NES::CLI
{


CLI::SchemaField::SchemaField(std::string name, const std::string& typeName) : SchemaField(std::move(name), stringToFieldType(typeName))
{
}

CLI::SchemaField::SchemaField(std::string name, DataType type) : name(std::move(name)), type(std::move(type))
{
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Schema YAMLBinder::bindSchema(const std::vector<SchemaField>& attributeFields) const
{
    Schema schema;
    for (const auto& [name, type] : attributeFields)
    {
        schema.addField(name, type);
    }
    return schema;
}

std::vector<NES::LogicalSource> YAMLBinder::bindRegisterLogicalSources(const std::vector<LogicalSource>& unboundSources)
{
    std::vector<NES::LogicalSource> boundSources{};
    /// Add logical sources to the SourceCatalog to prepare adding physical sources to each logical source.
    for (const auto& [logicalSourceName, schemaFields] : unboundSources)
    {
        auto schema = bindSchema(schemaFields);
        NES_INFO("Adding logical source: {}", logicalSourceName);
        if (const auto registeredSource = sourceCatalog->addLogicalSource(logicalSourceName, schema); registeredSource.has_value())
        {
            boundSources.push_back(registeredSource.value());
        }
        else
        {
            NES_ERROR("Could not register logical source \"{}\" because it already exists", logicalSourceName);
        }
    }
    return boundSources;
}

std::vector<SourceDescriptor> CLI::YAMLBinder::bindRegisterPhysicalSources(const std::vector<PhysicalSource>& unboundSources)
{
    std::vector<SourceDescriptor> boundSources{};
    /// Add physical sources to corresponding logical sources.
    for (auto [logicalSourceName, sourceType, parserConfig, sourceConfig] : unboundSources)
    {
        auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
        if (not logicalSource.has_value())
        {
            throw UnknownSourceName("{}", logicalSourceName);
        }
        NES_DEBUG("Source type is: {}", sourceType);

        const auto sourceDescriptorOpt = sourceCatalog->addPhysicalSource(logicalSource.value(), sourceType, sourceConfig, parserConfig);
        if (not sourceDescriptorOpt.has_value())
        {
            throw UnknownSourceName("{}", logicalSource.value().getLogicalSourceName());
        }
        boundSources.push_back(sourceDescriptorOpt.value());
    }
    return boundSources;
}

std::vector<SinkDescriptor> CLI::YAMLBinder::bindRegisterSinks(const std::vector<Sink>& unboundSinks)
{
    std::vector<SinkDescriptor> boundSinks{};
    for (const auto& [sinkName, schemaFields, sinkType, sinkConfig] : unboundSinks)
    {
        auto schema = bindSchema(schemaFields);
        NES_DEBUG("Adding sink: {} of type {}", sinkName, sinkType);
        if (auto sinkDescriptor = sinkCatalog->addSinkDescriptor(sinkName, schema, sinkType, sinkConfig); sinkDescriptor.has_value())
        {
            boundSinks.push_back(sinkDescriptor.value());
        }
    }
    return boundSinks;
}

std::vector<Nebuli::Inference::ModelDescriptor> CLI::YAMLBinder::bindRegisterModels(const std::vector<Model>& unboundModels)
{
    auto boundModels = unboundModels
        | std::views::transform(
                           [](const auto& model)
                           {
                               Schema schema{};
                               for (auto [name, type] : model.outputs)
                               {
                                   schema.addField(name, type);
                               }
                               return Nebuli::Inference::ModelDescriptor{
                               model.name, model.path, model.inputs, schema, model.batchSize};
                           })
        | std::ranges::to<std::vector>();
    std::ranges::for_each(boundModels, [&](const auto& model) { modelCatalog->registerModel(model); });
    return boundModels;
}

LogicalPlan CLI::YAMLBinder::parseAndBind(std::istream& inputStream)
{
    try
    {
        auto [queryString, unboundSinks, unboundLogicalSources, unboundPhysicalSources, unboundModels] = YAML::Load(inputStream).as<QueryConfig>();
        bindRegisterLogicalSources(unboundLogicalSources);
        bindRegisterPhysicalSources(unboundPhysicalSources);
        bindRegisterSinks(unboundSinks);
        bindRegisterModels(unboundModels);
        auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryString);
        return plan;
    }
    catch (const YAML::ParserException& pex)
    {
        throw QueryDescriptionNotParsable("{}", pex.what());
    }
}
}
