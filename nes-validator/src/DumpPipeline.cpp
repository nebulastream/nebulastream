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

#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <LegacyOptimizer.hpp>
#include <Util/Ranges.hpp>
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
                schema.addField(field.name, field.type.type, field.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
            }
            auto addResult = sourceCatalog->addLogicalSource(logicalSource.name, schema);
            if (!addResult)
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
                schema.addField(field.name, field.type.type, field.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
            }
            auto addResult = sinkCatalog->addSinkDescriptor(sinkConfig.name, schema, sinkConfig.type, Host(sinkConfig.host), sinkConfig.config);
            if (!addResult)
            {
                return "Error: Invalid sink configuration for '" + sinkConfig.name + "'";
            }
        }

        // 3. Populate worker catalog
        auto workerCatalog = std::make_shared<WorkerCatalog>();
        for (const auto& worker : config.workers)
        {
            auto downstream = worker.downstream
                | std::views::transform([](const auto& host) { return Host(host); })
                | std::ranges::to<std::vector>();
            Capacity capacity = worker.maxOperators.has_value()
                ? Capacity(CapacityKind::Limited{worker.maxOperators.value()})
                : Capacity(CapacityKind::Unlimited{});

            SingleNodeWorkerConfiguration workerConfig;
            if (!worker.config.empty())
            {
                workerConfig.overwriteConfigWithCommandLineInput(worker.config);
            }
            workerCatalog->addWorker(Host(worker.host), worker.data, capacity, downstream, std::move(workerConfig));
        }

        // 4. Run optimizer on each query and format output like ExplainQueryStatement
        auto optimizer = std::make_shared<LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
        std::stringstream output;

        for (const auto& query : config.query)
        {
            auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
            fmt::println(output, "Query:\n{}", plan.getOriginalSql());
            fmt::println(output, "Initial Logical Plan:\n{}", plan);

            const auto distributedPlan = optimizer->optimize(plan);

            fmt::println(output, "Optimized Global Plan:\n{}", distributedPlan.getGlobalPlan());

            fmt::println(output, "Decomposed Plans:");
            for (const auto& [worker, plans] : distributedPlan)
            {
                fmt::println(output, "{} plans on {}:", plans.size(), worker);
                for (const auto& [index, localPlan] : plans | views::enumerate)
                {
                    fmt::println(output, "{}:\n{}\n", index, localPlan);
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
