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

#include <Validator/TopologyValidator.hpp>
#include "YamlBinding.hpp"

#include <exception>
#include <memory>
#include <ranges>
#include <string>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <ModelCatalog.hpp>
#include <Phases/SemanticAnalyzer.hpp>
#include <QueryOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <WorkerCatalog.hpp>

namespace NES::Validator
{

std::string validateTopology(const std::string& yamlString)
{
    try
    {
        // 1. Parse YAML into TopologyConfig
        auto config = YAML::Load(yamlString).as<TopologyConfig>();

        // 2. Populate source catalog from parsed config
        auto sourceCatalog = std::make_shared<SourceCatalog>();

        for (const auto& logicalSource : config.logical)
        {
            Schema schema;
            for (const auto& field : logicalSource.schema)
            {
                schema.addField(field.name, field.type);
            }
            auto addResult = sourceCatalog->addLogicalSource(logicalSource.name, schema);
            if (!addResult)
            {
                return "Duplicate logical source: " + logicalSource.name;
            }
        }

        for (const auto& physicalSource : config.physical)
        {
            auto logicalOpt = sourceCatalog->getLogicalSource(physicalSource.logical);
            if (!logicalOpt)
            {
                return "Unknown logical source: " + physicalSource.logical;
            }
            auto result = sourceCatalog->addPhysicalSource(
                *logicalOpt, physicalSource.type, Host(physicalSource.host), physicalSource.sourceConfig, physicalSource.parserConfig);
            if (!result)
            {
                return result.error().what();
            }
        }

        // 3. Populate sink catalog from parsed config
        auto sinkCatalog = std::make_shared<SinkCatalog>();

        for (const auto& sinkConfig : config.sinks)
        {
            Schema schema;
            for (const auto& field : sinkConfig.schema)
            {
                schema.addField(field.name, field.type);
            }
            auto addResult = sinkCatalog->addSinkDescriptor(
                sinkConfig.name, schema, sinkConfig.type, Host(sinkConfig.host), sinkConfig.config, sinkConfig.parserConfig);
            if (!addResult)
            {
                return "Invalid sink configuration: " + sinkConfig.name;
            }
        }

        // 4. Populate worker catalog from topology
        auto workerCatalog = std::make_shared<WorkerCatalog>();
        for (const auto& worker : config.workers)
        {
            auto downstream = worker.downstream | std::views::transform([](const auto& d) { return Host(d); })
                | std::ranges::to<std::vector>();
            auto capacity = worker.maxOperators.has_value() ? Capacity(CapacityKind::Limited{*worker.maxOperators})
                                                           : Capacity(CapacityKind::Unlimited{});
            workerCatalog->addWorker(Host(worker.host), worker.data, capacity, downstream);
        }

        // 5. Parse and validate SQL queries through semantic analysis + placement
        // Web build cannot run model inference; pass an empty ModelCatalog to satisfy the
        // optimizer signature.
        auto modelCatalog = std::make_shared<ModelCatalog>();
        auto semanticAnalyzer = SemanticAnalyzer(sourceCatalog, sinkCatalog, modelCatalog);
        auto queryOptimizer = QueryOptimizer(QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog);

        for (const auto& query : config.query)
        {
            auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
            auto analysedPlan = semanticAnalyzer.analyse(plan);
            [[maybe_unused]] auto distributedPlan = queryOptimizer.optimize(analysedPlan);
        }

        return ""; // success
    }
    catch (const std::exception& e)
    {
        return e.what();
    }
}

} // namespace NES::Validator
