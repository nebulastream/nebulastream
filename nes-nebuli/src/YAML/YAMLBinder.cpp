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
#include <memory>
#include <ranges>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/ConfigurationsNames.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
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
        const auto sink = node["sink"].as<NES::CLI::Sink>();
        rhs.sinks.emplace(sink.name, sink);
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


SchemaField::SchemaField(std::string name, const std::string& typeName) : SchemaField(std::move(name), stringToFieldType(typeName))
{
}

SchemaField::SchemaField(std::string name, DataType type) : name(std::move(name)), type(std::move(type))
{
}

std::vector<NES::LogicalSource> YAMLBinder::bindRegisterLogicalSources(const std::vector<LogicalSource>& unboundSources)
{
    std::vector<NES::LogicalSource> boundSources{};
    /// Add logical sources to the SourceCatalog to prepare adding physical sources to each logical source.
    for (const auto& [logicalSourceName, schemaFields] : unboundSources)
    {
        auto schema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
        NES_INFO("Adding logical source: {}", logicalSourceName);
        for (const auto& [name, type] : schemaFields)
        {
            schema.addField(name, type);
        }
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

std::vector<SourceDescriptor> YAMLBinder::bindRegisterPhysicalSources(const std::vector<PhysicalSource>& unboundSources)
{
    std::vector<SourceDescriptor> boundSources{};
    /// Add physical sources to corresponding logical sources.
    for (auto [logicalSourceName, parserConfig, sourceConfig] : unboundSources)
    {
        auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
        if (not logicalSource.has_value())
        {
            throw UnknownSource("{}", logicalSourceName);
        }
        PRECONDITION(
            sourceConfig.contains(std::string{Configurations::SOURCE_TYPE_CONFIG}),
            "Missing `Configurations::SOURCE_TYPE_CONFIG` in source configuration");
        auto sourceType = sourceConfig.at(std::string{Configurations::SOURCE_TYPE_CONFIG});
        NES_DEBUG("Source type is: {}", sourceType);

        auto buffersInLocalPool = SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL;

        if (const auto configuredNumSourceLocalBuffers = sourceConfig.find(std::string{Configurations::NUMBER_OF_BUFFERS_IN_LOCAL_POOL});
            configuredNumSourceLocalBuffers != sourceConfig.end())
        {
            if (const auto customBuffersInLocalPool = Util::from_chars<int>(configuredNumSourceLocalBuffers->second))
            {
                buffersInLocalPool = customBuffersInLocalPool.value();
            }
        }
        const auto validInputFormatterConfig = ParserConfig::create(parserConfig);
        auto validSourceConfig = Sources::SourceValidationProvider::provide(sourceType, std::move(sourceConfig));
        const auto sourceDescriptorOpt = sourceCatalog->addPhysicalSource(
            logicalSource.value(),
            INITIAL<WorkerId>,
            sourceType,
            buffersInLocalPool,
            std::move(validSourceConfig),
            validInputFormatterConfig);
        if (not sourceDescriptorOpt.has_value())
        {
            throw UnknownSource("{}", logicalSource.value().getLogicalSourceName());
        }
        boundSources.push_back(sourceDescriptorOpt.value());
    }
    return boundSources;
}

std::vector<Nebuli::Inference::ModelDescriptor> YAMLBinder::bindRegisterModels(const std::vector<Model>& unboundModels)
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
                               return Nebuli::Inference::ModelDescriptor{model.name, model.path, model.inputs, schema};
                           })
        | std::ranges::to<std::vector>();
    std::ranges::for_each(boundModels, [&](const auto& model) { modelCatalog->registerModel(model); });
    return boundModels;
}

BoundQueryConfig YAMLBinder::parseAndBind(std::istream& inputStream)
{
    BoundQueryConfig config{};
    auto& [plan, sinks, logicalSources, physicalSources, models] = config;
    try
    {
        auto [queryString, unboundSinks, unboundLogicalSources, unboundPhysicalSources, unboundModels]
            = YAML::Load(inputStream).as<QueryConfig>();
        plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryString);
        const auto sinkOperators = plan.rootOperators;
        if (sinkOperators.size() != 1)
        {
            throw QueryInvalid(
                "NebulaStream currently only supports a single sink per query, but the query contains: {}", sinkOperators.size());
        }
        auto sinkOperator = sinkOperators.at(0).tryGet<SinkLogicalOperator>();
        INVARIANT(sinkOperator.has_value(), "Root operator in plan was not sink");

        if (const auto foundSink = unboundSinks.find(sinkOperator->sinkName); foundSink != unboundSinks.end())
        {
            auto validatedSinkConfig = Sinks::SinkDescriptor::validateAndFormatConfig(foundSink->second.type, foundSink->second.config);
            auto sinkDescriptor = std::make_shared<Sinks::SinkDescriptor>(foundSink->second.type, std::move(validatedSinkConfig), false);
            sinkOperator->sinkDescriptor = sinkDescriptor;
            sinks = std::unordered_map{std::make_pair(foundSink->second.name, sinkDescriptor)};
        }
        else
        {
            throw UnknownSinkType(
                "Sinkname {} not specified in the configuration {}",
                sinkOperator->sinkName,
                fmt::join(std::ranges::views::keys(sinks), ","));
        }
        plan.rootOperators.at(0) = *sinkOperator;
        logicalSources = bindRegisterLogicalSources(unboundLogicalSources);
        physicalSources = bindRegisterPhysicalSources(unboundPhysicalSources);
        models = bindRegisterModels(unboundModels);
        return config;
    }
    catch (const YAML::ParserException& pex)
    {
        throw QueryDescriptionNotParsable("{}", pex.what());
    }
}
}
