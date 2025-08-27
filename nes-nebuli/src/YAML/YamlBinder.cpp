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
#include <istream>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <QueryConfig.hpp>
#include <QueryPlanning.hpp>
#include <WorkerCatalog.hpp>

namespace NES::CLI
{

YamlBinder::YamlBinder(const QueryConfig& config)
    : queryConfig{config}
    , workerCatalog{std::make_unique<WorkerCatalog>()}
    , sourceCatalog{std::make_unique<SourceCatalog>()}
    , sinkCatalog{std::make_unique<SinkCatalog>()}
{
}

void YamlBinder::bindRegisterWorkers(const std::vector<WorkerConfig>& unboundWorkers)
{
    for (const auto& [host, grpc, capacity, downstream] : unboundWorkers)
    {
        NES_INFO("Adding worker: {}", host);
        if (not workerCatalog->addWorker(host, grpc, capacity, downstream))
        {
            NES_ERROR("Failed to add worker {}, because it already exists", host);
        }
    }
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

std::pair<PlanStage::BoundLogicalPlan, QueryPlanningContext> YamlBinder::bind(LogicalPlan&& plan) &&
{
    /// Validate and set descriptors for sources and sinks, register into source catalog
    bindRegisterWorkers(queryConfig.workerNodes);
    bindRegisterLogicalSources(queryConfig.logicalSources);
    bindRegisterPhysicalSources(queryConfig.physicalSources);
    bindRegisterSinks(queryConfig.sinks);
    auto boundPlan = PlanStage::BoundLogicalPlan{std::move(plan)};
    auto ctx = QueryPlanningContext{
        .id = boundPlan.plan.getQueryId(),
        .sqlString = boundPlan.plan.getOriginalSql(),
        .sourceCatalog = std::move(sourceCatalog),
        .sinkCatalog = std::move(sinkCatalog),
        .workerCatalog = std::move(workerCatalog)};

    return {std::move(boundPlan), std::move(ctx)};
}

}
