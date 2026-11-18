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

#include <DistributedTestCluster.hpp>

#include <cstddef>
#include <memory>
#include <ranges>
#include <string>
#include <vector>
#include <Configurations/Util.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
namespace
{
Schema<UnqualifiedUnboundField, Ordered> createSchema(const std::vector<std::string>& fields)
{
    return fields
        | std::views::transform([](const auto& field) { return UnqualifiedUnboundField{Identifier::parse(field), DataType::Type::UINT64}; })
        | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
}

std::vector<Host> toHosts(const std::vector<std::string>& hosts)
{
    return hosts | std::views::transform([](const auto& host) { return Host(host); }) | std::ranges::to<std::vector>();
}
}

DistributedTestCluster::DistributedTestCluster()
    : workerCatalog(std::make_shared<WorkerCatalog>())
    , sourceCatalog(std::make_shared<SourceCatalog>())
    , sinkCatalog(std::make_shared<SinkCatalog>())
    , modelCatalog(std::make_shared<ModelCatalog>())
{
}

void DistributedTestCluster::addWorker(const std::string& host, const std::vector<std::string>& downstream)
{
    if (not workerCatalog->addWorker(Host(host), host, CapacityKind::Unlimited{}, toHosts(downstream)))
    {
        throw TestException("Duplicate worker {}", host);
    }
}

void DistributedTestCluster::addWorker(const std::string& host, const std::vector<std::string>& downstream, const size_t capacity)
{
    if (not workerCatalog->addWorker(Host(host), host, CapacityKind::Limited{capacity}, toHosts(downstream)))
    {
        throw TestException("Duplicate worker {}", host);
    }
}

void DistributedTestCluster::addSource(const std::string& name, const std::vector<std::string>& fields, const std::string& host)
{
    const auto logicalSource = sourceCatalog->addLogicalSource(Identifier::parse(name), createSchema(fields));
    if (not logicalSource)
    {
        throw TestException("Could not add logical source {}", name);
    }
    const auto physicalSource = sourceCatalog->addPhysicalSource(
        *logicalSource,
        Identifier::parse("File"),
        Host(host),
        {{Identifier::parse("file_path"), "does_not_exist"}},
        {{Identifier::parse("type"), "CSV"}});
    if (not physicalSource)
    {
        throw TestException("Could not add physical source {}: {}", name, physicalSource.error().what());
    }
}

void DistributedTestCluster::addSink(const std::string& name, const std::vector<std::string>& fields, const std::string& host)
{
    const auto sink
        = sinkCatalog->addSinkDescriptor(Identifier::parse(name), createSchema(fields), Identifier::parse("VOID"), Host(host), {}, {});
    if (not sink)
    {
        throw TestException("Could not add sink {}: {}", name, sink.error().what());
    }
}

DistributedLogicalPlan DistributedTestCluster::optimize(const std::string& sql) const
{
    return optimize(sql, defaultConfiguration<QueryOptimizerConfiguration>().joinStrategy);
}

DistributedLogicalPlan DistributedTestCluster::optimize(const std::string& sql, const StreamJoinStrategy joinStrategy) const
{
    auto configuration = defaultConfiguration<QueryOptimizerConfiguration>();
    configuration.joinStrategy = joinStrategy;
    const QueryOptimizer optimizer(configuration, copyPtr(sourceCatalog), copyPtr(sinkCatalog), copyPtr(workerCatalog), modelCatalog);
    return optimizer.optimize(AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql));
}

}
