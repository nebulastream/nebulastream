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

#include <vector>

#include <DataTypes/Schema.hpp>
#include <Distributed/NetworkTopology.hpp>
#include <Distributed/WorkerCatalog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <QueryConfig.hpp>

namespace NES
{
/// Validated and bound content of a YAML file, the members are not specific to the yaml-binder anymore but our "normal" types.
/// If something goes wrong, for example, a source is declared twice, the binder will throw an exception.
struct BoundLogicalPlan
{
    LogicalPlan plan;
    TopologyGraph topology;
    WorkerCatalog workerCatalog;
    SharedPtr<SourceCatalog> sourceCatalog;
    SharedPtr<SinkCatalog> sinkCatalog;
};
}

namespace NES::CLI
{

class YamlBinder
{
    LogicalPlan plan;
    QueryConfig queryConfig;

    TopologyGraph topology;
    WorkerCatalog workerCatalog;
    SharedPtr<SourceCatalog> sourceCatalog;
    SharedPtr<SinkCatalog> sinkCatalog;

    explicit YamlBinder(const QueryConfig& config);

public:
    static YamlBinder from(const QueryConfig& config) { return YamlBinder{config}; }

    BoundLogicalPlan bind() &&;

private:
    void bindRegisterLogicalSources(const std::vector<LogicalSource>& unboundSources);
    void bindRegisterPhysicalSources(const std::vector<PhysicalSource>& unboundSources);
    void bindRegisterSinks(const std::vector<Sink>& unboundSinks);
    Schema bindSchema(const std::vector<SchemaField>& attributeFields) const;
};

}
