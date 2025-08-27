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
#pragma once

#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <NetworkTopology.hpp>
#include <WorkerCatalog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <QueryConfig.hpp>
#include <QueryPlanning.hpp>

namespace NES::CLI
{

class YamlBinder
{
    const QueryConfig& queryConfig;

    UniquePtr<WorkerCatalog> workerCatalog;
    UniquePtr<SourceCatalog> sourceCatalog;
    UniquePtr<SinkCatalog> sinkCatalog;

    explicit YamlBinder(const QueryConfig& config);

public:
    static YamlBinder from(const QueryConfig& config) { return YamlBinder{config}; }

    std::pair<PlanStage::BoundLogicalPlan, QueryPlanningContext> bind(LogicalPlan&& plan) &&;

private:
    void bindRegisterWorkers(const std::vector<WorkerConfig>& unboundWorkers);
    void bindRegisterLogicalSources(const std::vector<LogicalSource>& unboundSources);
    void bindRegisterPhysicalSources(const std::vector<PhysicalSource>& unboundSources);
    void bindRegisterSinks(const std::vector<Sink>& unboundSinks);
    [[nodiscard]] Schema bindSchema(const std::vector<SchemaField>& attributeFields) const;
};

}
