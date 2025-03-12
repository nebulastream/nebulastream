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

#include <NebuLI.hpp>

#include <memory>
#include <ranges>
#include <string>
#include <utility>

#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <fmt/ranges.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include "Distributed/Placement.hpp"
#include "Distributed/Topology.hpp"

namespace NES::Catalogs
{
using SinkCatalog = std::unordered_map<std::string, std::pair<NES::CLI::Sink, Distributed::Topology::Node>>;
}
namespace NES::CLI
{


Sources::ParserConfig validateAndFormatParserConfig(const std::unordered_map<std::string, std::string>& parserConfig)
{
    auto validParserConfig = Sources::ParserConfig{};
    if (const auto parserType = parserConfig.find("type"); parserType != parserConfig.end())
    {
        validParserConfig.parserType = parserType->second;
    }
    else
    {
        throw InvalidConfigParameter("Parser configuration must contain: type");
    }
    if (const auto tupleDelimiter = parserConfig.find("tupleDelimiter"); tupleDelimiter != parserConfig.end())
    {
        validParserConfig.tupleDelimiter = tupleDelimiter->second;
    }
    else
    {
        NES_DEBUG("Parser configuration did not contain: tupleDelimiter, using default: \\n");
        validParserConfig.tupleDelimiter = "\n";
    }
    if (const auto fieldDelimiter = parserConfig.find("fieldDelimiter"); fieldDelimiter != parserConfig.end())
    {
        validParserConfig.fieldDelimiter = fieldDelimiter->second;
    }
    else
    {
        NES_DEBUG("Parser configuration did not contain: fieldDelimiter, using default: ,");
        validParserConfig.fieldDelimiter = ",";
    }
    return validParserConfig;
}

Sources::SourceDescriptor createSourceDescriptor(
    std::string logicalSourceName,
    std::shared_ptr<Schema> schema,
    const std::unordered_map<std::string, std::string>& parserConfig,
    std::unordered_map<std::string, std::string> sourceConfiguration)
{
    PRECONDITION(
        sourceConfiguration.contains(Configurations::SOURCE_TYPE_CONFIG),
        "Missing `Configurations::SOURCE_TYPE_CONFIG` in source configuration");
    auto sourceType = sourceConfiguration.at(Configurations::SOURCE_TYPE_CONFIG);
    NES_DEBUG("Source type is: {}", sourceType);

    auto validParserConfig = validateAndFormatParserConfig(parserConfig);
    auto validSourceConfig = Sources::SourceValidationProvider::provide(sourceType, std::move(sourceConfiguration));
    return Sources::SourceDescriptor(
        std::move(schema), std::move(logicalSourceName), "", sourceType, std::move(validParserConfig), std::move(validSourceConfig));
}

void validateAndSetSinkDescriptors(const QueryPlan& query, const Catalogs::SinkCatalog& sinks)
{
    PRECONDITION(
        query.getSinkOperators().size() == 1,
        "NebulaStream currently only supports a single sink per query, but the query contains: {}",
        query.getSinkOperators().size());
    PRECONDITION(not sinks.empty(), fmt::format("Expects at least one sink in the query config!"));
    if (const auto sink = sinks.find(query.getSinkOperators().at(0)->sinkName); sink != sinks.end())
    {
        auto validatedSinkConfig = *Sinks::SinkDescriptor::validateAndFormatConfig(sink->second.first.type, sink->second.first.config);
        query.getSinkOperators().at(0)->sinkDescriptor
            = std::make_shared<Sinks::SinkDescriptor>(sink->second.first.type, std::move(validatedSinkConfig), false);
    }
    else
    {
        throw UnknownSinkType(
            "Sinkname {} not specified in the configuration {}",
            query.getSinkOperators().front()->sinkName,
            fmt::join(std::views::keys(sinks), ","));
    }
}

std::tuple<
    Distributed::Topology,
    std::unordered_map<Distributed::Topology::Node, Distributed::PhysicalNodeConfig>,
    std::unordered_map<Distributed::Topology::Node, size_t>,
    Catalogs::SinkCatalog>
buildTopologyFromConfiguration(const NES::Distributed::Config::Topology& config, Catalogs::Source::SourceCatalog& sourceCatalog)
{
    Distributed::Topology topology;
    Catalogs::SinkCatalog sinks;
    std::unordered_map<Distributed::Topology::Node, Distributed::PhysicalNodeConfig> physicalTopology;
    std::unordered_map<Distributed::Topology::Node, size_t> capacities;
    auto nodesByConnection
        = std::views::transform(
              config.nodes,
              [&](const auto& nodeConfig) { return std::make_pair(nodeConfig.connection, std::make_pair(nodeConfig, topology.addNode())); })
        | ranges::to<std::unordered_map>();

    for (const auto& [nodeConnection, pair] : nodesByConnection)
    {
        auto [nodeConfig, node] = pair;
        /// Capacties
        capacities[node] = nodeConfig.capacity;
        physicalTopology[node] = Distributed::PhysicalNodeConfig{.connection = nodeConnection, .grpc = nodeConfig.grpc};

        /// Topology Links
        for (auto downstreamConnection : nodeConfig.links.downstreams)
        {
            topology.addDownstreams(node, nodesByConnection.at(downstreamConnection).second);
        }
        for (auto downstreamConnection : nodeConfig.links.upstreams)
        {
            topology.addUpstreams(node, nodesByConnection.at(downstreamConnection).second);
        }

        /// Register Sources
        for (auto [logicalSourceName, parserConfig, sourceConfig] : nodeConfig.physical)
        {
            auto sourceDescriptor = createSourceDescriptor(
                logicalSourceName, sourceCatalog.getSchemaForLogicalSource(logicalSourceName), parserConfig, std::move(sourceConfig));
            sourceCatalog.addPhysicalSource(
                logicalSourceName,
                Catalogs::Source::SourceCatalogEntry::create(
                    NES::PhysicalSource::create(std::move(sourceDescriptor)), sourceCatalog.getLogicalSource(logicalSourceName), node));
        }

        /// Register Sinks
        for (auto sink : nodeConfig.sinks)
        {
            sinks.try_emplace(sink.name, sink, node);
        }
    }

    return {topology, physicalTopology, capacities, sinks};
}

std::vector<std::shared_ptr<DecomposedQueryPlan>>
createFullySpecifiedQueryPlan(std::string_view queryText, const NES::Distributed::Config::Topology& topologyConfiguration)
{
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();

    /// Add logical sources to the SourceCatalog to prepare adding physical sources to each logical source.
    for (const auto& [logicalSourceName, schemaFields] : topologyConfiguration.logical)
    {
        auto schema = Schema::create();
        NES_INFO("Adding logical source: {}", logicalSourceName);
        for (const auto& [name, type] : schemaFields)
        {
            schema = schema->addField(name, type);
        }
        sourceCatalog->addLogicalSource(logicalSourceName, schema);
    }

    auto [topology, physicalTopology, capacity, sinks] = buildTopologyFromConfiguration(topologyConfiguration, *sourceCatalog);


    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog);
    auto logicalSourceExpansionRule = NES::Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    auto typeInference = Optimizer::TypeInferencePhase::create(sourceCatalog);
    auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
    auto queryRewritePhase = Optimizer::QueryRewritePhase::create();

    auto query = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryText);

    validateAndSetSinkDescriptors(*query, sinks);
    semanticQueryValidation->validate(query); /// performs the first type inference

    logicalSourceExpansionRule->apply(query);
    typeInference->performTypeInferenceQuery(query);

    originIdInferencePhase->execute(query);
    queryRewritePhase->execute(query);
    typeInference->performTypeInferenceQuery(query);

    if (topology.size() == 1)
    {
        return {std::make_shared<DecomposedQueryPlan>(query->getQueryId(), "", query->getRootOperators())};
    }

    /// Distributed
    NES::Distributed::BottomUpPlacement placement;
    placement.capacity = std::move(capacity);

    Distributed::pinSourcesAndSinks(
        *query,
        *sourceCatalog,
        std::views::transform(sinks, [](const auto& pair) { return std::make_pair(pair.first, pair.second.second); })
            | ranges::to<std::unordered_map>());
    placement.doPlacement(topology, *query);
    auto dag = NES::Distributed::decompose(topology, *query);
    return NES::Distributed::connect(dag, physicalTopology);
}

}
