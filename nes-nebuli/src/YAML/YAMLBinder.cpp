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

#include <istream>
#include <memory>
#include <ranges>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>


#include <Configurations/ConfigurationsNames.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <YAML/YAMLBinder.hpp>
#include <fmt/ranges.h>
#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h> /// NOLINT(misc-include-cleaner)

#include <Identifiers/NESStrongType.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>


namespace
{
std::shared_ptr<NES::DataType> stringToFieldType(const std::string& fieldNodeType)
{
    try
    {
        return NES::DataTypeProvider::provideDataType(fieldNodeType);
    }
    catch (std::runtime_error& e)
    {
        throw NES::SLTWrongSchema("Found Invalid Logical Source Configuration. {} is not a proper Schema Field Type.", fieldNodeType);
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
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const Node& node, NES::CLI::QueryConfig& rhs)
    {
        const auto sink = node["sink"].as<NES::CLI::Sink>();
        rhs.sinks.emplace(sink.name, sink);
        rhs.logical = node["logical"].as<std::vector<NES::CLI::LogicalSource>>();
        rhs.physical = node["physical"].as<std::vector<NES::CLI::PhysicalSource>>();
        rhs.query = node["query"].as<std::string>();
        return true;
    }
};

}

namespace NES::CLI
{


SchemaField::SchemaField(std::string name, const std::string& typeName) : SchemaField(std::move(name), stringToFieldType(typeName))
{
}

SchemaField::SchemaField(std::string name, std::shared_ptr<DataType> type) : name(std::move(name)), type(std::move(type))
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

std::vector<Sources::SourceDescriptor> YAMLBinder::bindRegisterPhysicalSources(const std::vector<PhysicalSource>& unboundSources)
{
    std::vector<Sources::SourceDescriptor> boundSources{};
    /// Add physical sources to corresponding logical sources.
    for (auto [logicalSourceName, parserConfig, sourceConfig] : unboundSources)
    {
        auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
        if (not logicalSource.has_value())
        {
            throw UnregisteredSource("{}", logicalSourceName);
        }
        PRECONDITION(
            sourceConfig.contains(std::string{Configurations::SOURCE_TYPE_CONFIG}),
            "Missing `Configurations::SOURCE_TYPE_CONFIG` in source configuration");
        auto sourceType = sourceConfig.at(std::string{Configurations::SOURCE_TYPE_CONFIG});
        NES_DEBUG("Source type is: {}", sourceType);

        auto buffersInLocalPool = Sources::SourceDescriptor::INVALID_BUFFERS_IN_LOCAL_POOL;

        if (const auto configuredNumSourceLocalBuffers = sourceConfig.find(std::string{Configurations::BUFFERS_IN_LOCAL_POOL});
            configuredNumSourceLocalBuffers != sourceConfig.end())
        {
            if (const auto customBuffersInLocalPool = Util::from_chars<int>(configuredNumSourceLocalBuffers->second))
            {
                buffersInLocalPool = customBuffersInLocalPool.value();
            }
        }
        const auto validParserConfig = Sources::ParserConfig::create(parserConfig);
        auto validSourceConfig = Sources::SourceValidationProvider::provide(sourceType, std::move(sourceConfig));
        const auto sourceDescriptorOpt = sourceCatalog->addPhysicalSource(
            logicalSource.value(), INITIAL<WorkerId>, sourceType, buffersInLocalPool, std::move(validSourceConfig), validParserConfig);
        if (not sourceDescriptorOpt.has_value())
        {
            throw UnregisteredSource("{}", logicalSource.value().getLogicalSourceName());
        }
        boundSources.push_back(sourceDescriptorOpt.value());
    }
    return boundSources;
}

BoundQueryConfig YAMLBinder::parseAndBind(std::istream& inputStream)
{
    BoundQueryConfig config{};
    auto& [plan, sinks, logicalSources, physicalSources] = config;
    try
    {
        auto [queryString, unboundSinks, unboundLogicalSources, unboundPhysicalSources] = YAML::Load(inputStream).as<QueryConfig>();
        plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryString);
        const auto sinkOperators = plan->getSinkOperators();
        PRECONDITION(
            sinkOperators.size() == 1,
            "NebulaStream currently only supports a single sink per query, but the query contains: {}",
            sinkOperators.size());
        PRECONDITION(not unboundSinks.empty(), "Expects at least one sink in the query config!");
        if (const auto foundSink = unboundSinks.find(sinkOperators.at(0)->sinkName); foundSink != unboundSinks.end())
        {
            auto validatedSinkConfig = Sinks::SinkDescriptor::validateAndFormatConfig(foundSink->second.type, foundSink->second.config);
            auto sinkDescriptor = std::make_shared<Sinks::SinkDescriptor>(foundSink->second.type, std::move(validatedSinkConfig), false);
            sinkOperators.at(0)->sinkDescriptor = sinkDescriptor;
            sinks = std::unordered_map{std::make_pair(foundSink->second.name, sinkDescriptor)};
        }
        else
        {
            throw UnknownSinkType(
                "Sinkname {} not specified in the configuration {}",
                sinkOperators.front()->sinkName,
                fmt::join(std::ranges::views::keys(sinks), ","));
        }
        logicalSources = bindRegisterLogicalSources(unboundLogicalSources);
        physicalSources = bindRegisterPhysicalSources(unboundPhysicalSources);
        return config;
    }
    catch (const YAML::ParserException& pex)
    {
        throw QueryDescriptionNotParsable("{}", pex.what());
    }
}

}
