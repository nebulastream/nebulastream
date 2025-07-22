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

#include <YAML/YamlBinder.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <istream>
#include <memory>
#include <ranges>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Distributed/NetworkTopology.hpp>
#include <Distributed/WorkerCatalog.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <QueryConfig.hpp>

namespace NES::CLI
{

YamlBinder::YamlBinder(const QueryConfig& config)
    : plan(AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(config.query))
    , queryConfig{config}
    , topology{TopologyGraph::from(
          std::views::transform(config.workerNodes, [](const auto& conf) { return std::make_pair(conf.host, conf.downstreamNodes); })
          | std::ranges::to<std::vector>())}
    , workerCatalog{WorkerCatalog::from(config.workerNodes)}
    , sourceCatalog{std::make_shared<SourceCatalog>()}
    , sinkCatalog{std::make_shared<SinkCatalog>()}
{
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Schema YamlBinder::bindSchema(const std::vector<SchemaField>& attributeFields) const
{
    auto schema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
    for (const auto& [name, type] : attributeFields)
    {
        schema.addField(name, type);
    }
    return schema;
}

void YamlBinder::bindRegisterLogicalSources(const std::vector<LogicalSource>& unboundSources)
{
    /// Add logical sources to the SourceCatalog to prepare adding physical sources to each logical source.
    for (const auto& [logicalSourceName, schemaFields] : unboundSources)
    {
        auto schema = bindSchema(schemaFields);
        NES_INFO("Adding logical source: {}", logicalSourceName);
        if (not sourceCatalog->addLogicalSource(logicalSourceName, schema).has_value())
        {
            NES_ERROR("Could not register logical source \"{}\" because it already exists", logicalSourceName);
        }
    }
}

void YamlBinder::bindRegisterPhysicalSources(const std::vector<PhysicalSource>& unboundSources)
{
    /// Add physical sources to corresponding logical sources.
    for (const auto& [logicalSourceName, sourceType, hostAddr, parserConfig, sourceConfig] : unboundSources)
    {
        auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
        if (not logicalSource.has_value())
        {
            throw UnknownSourceName("{}", logicalSourceName);
        }
        NES_DEBUG("Source type is: {}", sourceType);

        const auto validInputFormatterConfig = ParserConfig::create(parserConfig);
        const auto sourceDescriptorOpt
            = sourceCatalog->addPhysicalSource(logicalSource.value(), sourceType, hostAddr, sourceConfig, validInputFormatterConfig);
        if (not sourceDescriptorOpt.has_value())
        {
            throw InvalidSourceConfig();
        }
    }
}

void YamlBinder::bindRegisterSinks(const std::vector<Sink>& unboundSinks)
{
    for (const auto& [sinkName, sinkType, schemaFields, hostAddr, sinkConfig] : unboundSinks)
    {
        auto schema = bindSchema(schemaFields);
        NES_DEBUG("Adding sink: {} of type {}", sinkName, sinkType);
        if (auto sinkDescriptor = sinkCatalog->addSinkDescriptor(sinkName, schema, sinkType, hostAddr, sinkConfig);
            not sinkDescriptor.has_value())
        {
            throw InvalidSinkConfig();
        }
    }
}

BoundLogicalPlan YamlBinder::bind() &&
{
    /// Validate and set descriptors for sources and sinks, register into source catalog
    bindRegisterLogicalSources(queryConfig.logicalSources);
    bindRegisterPhysicalSources(queryConfig.physicalSources);
    bindRegisterSinks(queryConfig.sinks);
    return BoundLogicalPlan{
        .plan = std::move(plan),
        .topology = std::move(topology),
        .workerCatalog = std::move(workerCatalog),
        .sourceCatalog = std::move(sourceCatalog),
        .sinkCatalog = std::move(sinkCatalog)};
}
}
