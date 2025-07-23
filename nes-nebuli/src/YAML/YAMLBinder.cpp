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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <istream>
#include <memory>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <yaml-cpp/yaml.h> /// NOLINT(misc-include-cleaner)

#include <Configurations/ConfigurationsNames.hpp>
#include <Distributed/NetworkTopology.hpp>
#include <Distributed/NodeCatalog.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <QueryConfig.hpp>

namespace YAML
{

template <>
struct convert<NES::CLI::NodeConfig>
{
    static bool decode(const YAML::Node& node, NES::CLI::NodeConfig& workerConfig)
    {
        workerConfig.host = node["host"].as<NES::CLI::HostAddr>();
        workerConfig.grpc = node["grpc"].as<NES::CLI::GrpcAddr>();
        workerConfig.capacity = node["capacity"].as<size_t>();
        workerConfig.downstreamNodes = node["downstreamNodes"].as<std::vector<NES::CLI::HostAddr>>(std::vector<NES::CLI::HostAddr>{});
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
        sink.host = node["host"].as<NES::CLI::HostAddr>();
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
        physicalSource.host = node["host"].as<NES::CLI::HostAddr>();
        physicalSource.parserConfig = node["parserConfig"].as<std::unordered_map<std::string, std::string>>();
        physicalSource.sourceConfig = node["sourceConfig"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};
template <>
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const YAML::Node& node, NES::CLI::QueryConfig& config)
    {
        config.query = node["query"].as<std::string>();
        config.sink = node["sink"].as<NES::CLI::Sink>(NES::CLI::Sink{});
        config.logicalSources = node["logicalSources"].as<std::vector<NES::CLI::LogicalSource>>(std::vector<NES::CLI::LogicalSource>{});
        config.physicalSources = node["physicalSources"].as<std::vector<NES::CLI::PhysicalSource>>(std::vector<NES::CLI::PhysicalSource>{});
        config.workerNodes = node["workerNodes"].as<std::vector<NES::CLI::NodeConfig>>(std::vector<NES::CLI::NodeConfig>{});
        return true;
    }
};
}

namespace NES::CLI
{
YAMLBinder::YAMLBinder(const QueryConfig& config)
    : plan(AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(config.query))
    , queryConfig{config}
    , topology{TopologyGraph::from(
          std::views::transform(config.workerNodes, [](const auto& conf) { return std::make_pair(conf.host, conf.downstreamNodes); })
          | std::ranges::to<std::vector>())}
    , nodeCatalog{NodeCatalog::from(config.workerNodes)}
    , sourceCatalog{std::make_shared<SourceCatalog>()}
{
}

void YAMLBinder::bindRegisterLogicalSources(const std::vector<LogicalSource>& unboundSources)
{
    /// Add logical sources to the SourceCatalog to prepare adding physical sources to each logical source.
    for (const auto& [logicalSourceName, schemaFields] : unboundSources)
    {
        auto schema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
        NES_INFO("Adding logical source: {}", logicalSourceName);
        for (const auto& [name, type] : schemaFields)
        {
            schema.addField(name, type);
        }
        if (not sourceCatalog->addLogicalSource(logicalSourceName, schema).has_value())
        {
            NES_ERROR("Could not register logical source \"{}\" because it already exists", logicalSourceName);
        }
    }
}

void YAMLBinder::bindRegisterPhysicalSources(const std::vector<PhysicalSource>& unboundSources)
{
    /// Add physical sources to corresponding logical sources.
    for (auto [logicalSourceName, host, parserConfig, sourceConfig] : unboundSources)
    {
        auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
        if (not logicalSource.has_value())
        {
            throw UnknownSourceName("{}", logicalSourceName);
        }
        PRECONDITION(sourceConfig.contains(std::string{SOURCE_TYPE_CONFIG}), "Missing `SOURCE_TYPE_CONFIG` in source configuration");
        auto sourceType = sourceConfig.at(std::string{SOURCE_TYPE_CONFIG});
        NES_DEBUG("Source type is: {}", sourceType);

        auto buffersInLocalPool = SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL;

        if (const auto configuredNumSourceLocalBuffers = sourceConfig.find(std::string{NUMBER_OF_BUFFERS_IN_LOCAL_POOL});
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
            logicalSource.value(), host, sourceType, buffersInLocalPool, std::move(validSourceConfig), validInputFormatterConfig);
        if (not sourceDescriptorOpt.has_value())
        {
            throw UnknownSourceName("{}", logicalSource.value().getLogicalSourceName());
        }
    }
}

LogicalPlan YAMLBinder::bindValidateSink(const Sink& unboundSink) const
{
    const auto sinkOperators = plan.getRootOperators();
    if (sinkOperators.size() != 1)
    {
        throw QueryInvalid(
            "NebulaStream currently only supports a single sink per query, but the query contains: {}", sinkOperators.size());
    }
    auto sinkOperator = sinkOperators.front().tryGet<SinkLogicalOperator>();
    INVARIANT(sinkOperator.has_value(), "Root operator in plan was not sink");

    if (sinkOperator->sinkName == unboundSink.name)
    {
        auto validatedSinkConfig = Sinks::SinkDescriptor::validateAndFormatConfig(unboundSink.type, unboundSink.config);
        const auto sinkDescriptor
            = std::make_shared<Sinks::SinkDescriptor>(unboundSink.type, std::move(validatedSinkConfig), unboundSink.host, false);
        sinkOperator->sinkDescriptor = sinkDescriptor;
    }
    else
    {
        throw UnknownSinkType(
            "Sink name '{}' does not match the name specified in the configuration: '{}'", sinkOperator->sinkName, unboundSink.name, ",");
    }
    return plan.withRootOperators({*sinkOperator});
}

BoundLogicalPlan YAMLBinder::bind() &&
{
    /// Validate and set descriptors for sources and sinks, register into source catalog
    bindRegisterLogicalSources(queryConfig.logicalSources);
    bindRegisterPhysicalSources(queryConfig.physicalSources);
    auto planWithUpdatedSink = bindValidateSink(queryConfig.sink);
    return BoundLogicalPlan{
        .plan = std::move(planWithUpdatedSink),
        .topology = std::move(topology),
        .nodeCatalog = std::move(nodeCatalog),
        .sourceCatalog = sourceCatalog};
}

QueryConfig YAMLLoader::load(const std::string& inputArgument)
{
    const std::string source = inputArgument == "-" ? "stdin" : inputArgument;

    try
    {
        if (inputArgument == "-")
        {
            return loadFromStream(std::cin);
        }
        return loadFromFile(inputArgument);
    }
    catch (const YAML::ParserException& e)
    {
        throw QueryDescriptionNotReadable("YAML syntax error in '{}' at line {}: {}", source, e.mark.line + 1, e.what());
    }
    catch (const YAML::BadConversion& e)
    {
        throw QueryDescriptionNotReadable("YAML conversion error in '{}': {}", source, e.what());
    }
    catch (const YAML::Exception& e)
    {
        throw QueryDescriptionNotReadable("YAML error in '{}': {}", source, e.what());
    }
    catch (const std::ios_base::failure& e)
    {
        throw QueryDescriptionNotReadable("IO error reading '{}': {}", source, e.what());
    }
}

QueryConfig YAMLLoader::loadFromFile(const std::string& filePath)
{
    if (!std::filesystem::exists(filePath))
    {
        throw QueryDescriptionNotReadable("File '{}' does not exist", filePath);
    }

    std::ifstream file{filePath};
    if (!file.is_open())
    {
        throw QueryDescriptionNotReadable("Cannot open file '{}': {}", filePath, std::strerror(errno));
    }

    file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
    return loadFromStream(file);
}

QueryConfig YAMLLoader::loadFromStream(std::istream& stream)
{
    return YAML::Load(stream).as<QueryConfig>();
}
}
