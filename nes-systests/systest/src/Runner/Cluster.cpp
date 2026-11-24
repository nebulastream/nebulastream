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

#include <Runner/Cluster.hpp>

#include <algorithm>
#include <iterator>
#include <string>
#include <vector>

#include <Config/Topology.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <nes-coordinator-bridge/coordinator.h>
#include <rust/cxx.h>
#include <BridgeConfig.hpp>
#include <BridgeError.hpp>

#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Rewriter/Constants.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

rust::Box<Bridge::Coordinator> startCoordinator(const Cluster::Config& config)
{
    const auto optimizer = Bridge::literalsToJson(config.optimizerConfig);
    try
    {
        /// The catalog is kept in memory and lives for this run only, so no database path is given.
        return Bridge::start_coordinator(
            Bridge::StateBackendKind::Memory, rust::Str{}, config.mode, rust::Str{optimizer.data(), optimizer.size()});
    }
    catch (const rust::Error& error)
    {
        throw TestException("could not start the coordinator: {}", error.what());
    }
}

std::string createWorkerStatement(const TopologyWorker& worker)
{
    std::vector options{fmt::format("{} AS {}", Sql::stringLiteral(worker.dataAddress), Sql::DataAddress)};
    if (worker.maxOperators.has_value())
    {
        options.push_back(fmt::format("{} AS {}", *worker.maxOperators, Sql::MaxOperators));
    }
    for (const auto& downstream : worker.downstream)
    {
        options.push_back(fmt::format("{} AS {}", Sql::stringLiteral(downstream.view()), Sql::Downstream));
    }
    std::ranges::transform(worker.config, std::back_inserter(options), Bridge::literalToSql);
    return Sql::createWorker(worker.host.view(), options);
}

Schema<LiteralConfigValue, Ordered> literalsOf(const ConfigurationOverride& overrides)
{
    std::vector<std::string> arguments;
    for (const auto& [key, value] : overrides)
    {
        arguments.push_back(fmt::format("--{}={}", key, value));
    }
    return parseCommandLineConfig(arguments);
}

/// An embedded worker starts with the settings of the topology, the command line and the test files, in rising precedence.
/// A remote worker is already running, so the cluster registers it without settings.
void registerWorkers(const Bridge::Coordinator& coordinator, const Cluster::Config& config)
{
    const auto embedded = config.mode == Bridge::WorkerMode::Embedded;
    const auto testFileConfig = embedded ? literalsOf(config.overrides) : Schema<LiteralConfigValue, Ordered>{};
    for (const auto& worker : config.topology.workers)
    {
        auto configured = worker;
        configured.config = embedded ? mergeConfigLayers({{.name = "topology", .literals = worker.config},
                                                          {.name = "run configuration", .literals = config.workerConfig},
                                                          {.name = "test file", .literals = testFileConfig}})
                                           .literals
                                     : Schema<LiteralConfigValue, Ordered>{};
        const auto statement = createWorkerStatement(configured);
        Bridge::throwIfError(coordinator.submit(rust::Str{statement.data(), statement.size()}, Bridge::WaitMode::None, 0, false).error);
    }
}

}

Placement placementIn(const ClusterConfiguration& topology)
{
    if (topology.workers.empty() or topology.allowSourcePlacement.empty() or topology.allowSinkPlacement.empty())
    {
        throw TestException("the topology needs at least one worker, and one host each that allows sources and sinks");
    }
    /// The first allowed host, not one chosen to spread the work.
    /// The lists say where a placement is permitted, not which hosts the data can reach: `two-nodes-localhost.yaml`
    /// allows a sink on either node while only one is downstream of the source.
    return Placement{.sources = topology.allowSourcePlacement.front(), .sinks = topology.allowSinkPlacement.front()};
}

Cluster::Cluster(const Config& config) : coordinator{startCoordinator(config)}, mode{config.mode}
{
    registerWorkers(*coordinator, config);
}

}
