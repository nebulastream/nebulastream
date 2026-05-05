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

#include <Validator/DumpPipeline.hpp>
#include "YamlBinding.hpp"

#include <exception>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <WorkerCatalog.hpp>
#include <fmt/format.h>

namespace NES::Validator
{

std::string explainTopology(const std::string& yamlString)
{
    try
    {
        auto config = YAML::Load(yamlString).as<TopologyConfig>();

        // 1. Populate source catalog
        auto sourceCatalog = std::make_shared<SourceCatalog>();
        for (const auto& logicalSource : config.logical)
        {
            Schema schema;
            for (const auto& field : logicalSource.schema)
            {
                schema.addField(field.name, field.type);
            }
            if (!sourceCatalog->addLogicalSource(logicalSource.name, schema))
            {
                return "Error: Duplicate logical source: " + logicalSource.name;
            }
        }
        for (const auto& physicalSource : config.physical)
        {
            auto logicalOpt = sourceCatalog->getLogicalSource(physicalSource.logical);
            if (!logicalOpt)
            {
                return "Error: Unknown logical source: " + physicalSource.logical;
            }
            auto result = sourceCatalog->addPhysicalSource(
                *logicalOpt, physicalSource.type, Host(physicalSource.host), physicalSource.sourceConfig, physicalSource.parserConfig);
            if (!result)
            {
                return "Error: " + std::string(result.error().what());
            }
        }

        // 2. Populate sink catalog
        auto sinkCatalog = std::make_shared<SinkCatalog>();
        for (const auto& sinkConfig : config.sinks)
        {
            Schema schema;
            for (const auto& field : sinkConfig.schema)
            {
                schema.addField(field.name, field.type);
            }
            if (!sinkCatalog->addSinkDescriptor(
                    sinkConfig.name, schema, sinkConfig.type, Host(sinkConfig.host), sinkConfig.config, sinkConfig.parserConfig))
            {
                return "Error: Invalid sink configuration: " + sinkConfig.name;
            }
        }

        // 3. Populate worker catalog from topology
        auto workerCatalog = std::make_shared<WorkerCatalog>();
        for (const auto& worker : config.workers)
        {
            auto downstream = worker.downstream | std::views::transform([](const auto& d) { return Host(d); })
                | std::ranges::to<std::vector>();
            auto capacity = worker.maxOperators.has_value() ? Capacity(CapacityKind::Limited{*worker.maxOperators})
                                                           : Capacity(CapacityKind::Unlimited{});
            workerCatalog->addWorker(Host(worker.host), worker.dataAddress, capacity, downstream);
        }

        // 4. Run full optimizer (same as nes-cli dump). On main QueryOptimizer::optimize
        // runs SemanticAnalyzer internally, so we do NOT call it separately — that
        // would re-run OriginIdInferenceRule and trip its single-assignment invariant.
        // Web build cannot run model inference; an empty ModelCatalog is enough to
        // satisfy the optimizer signature — InferModelResolutionRule has no work
        // without registered models.
        auto modelCatalog = std::make_shared<ModelCatalog>();
        auto queryOptimizer = QueryOptimizer(QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog);

        std::stringstream output;
        for (const auto& query : config.query)
        {
            auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
            fmt::println(output, "Query:\n{}", query);
            fmt::println(output, "Initial Logical Plan:\n{}", plan);

            auto distributedPlan = queryOptimizer.optimize(plan);

            fmt::println(output, "Optimized Global Plan:\n{}", distributedPlan.getGlobalPlan());

            fmt::println(output, "Decomposed Plans:");
            for (const auto& [worker, plans] : distributedPlan)
            {
                fmt::println(output, "{} plans on {}:", plans.size(), worker);
                for (size_t i = 0; i < plans.size(); ++i)
                {
                    fmt::println(output, "{}:\n{}\n", i, plans[i]);
                }
            }
        }

        return output.str();
    }
    catch (const std::exception& e)
    {
        return std::string("Error: ") + e.what();
    }
}

} // namespace NES::Validator
