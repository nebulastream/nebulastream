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

#include <string>
#include <utility>
#include <vector>

#include <coordinator/lib.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rust/cxx.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Rewriter/Constants.hpp>
#include <Runner/WorkerRegistration.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

Cluster::Cluster(const EmbeddedCoordinator& coordinator, const Settings& settings)
    : coordinator{coordinator}
    , mode{settings.mode}
    , defaultHost{std::string{coordinator.default_host()}}
    , baseSettings{settings.workerSettings}
{
    /// In remote mode the coordinator starts no worker of its own and answers no default host, so the topology has to
    /// declare the workers and where they are.
    /// Reporting that here beats emitting DDL with an empty host and failing once every statement has been placed nowhere.
    if (mode == WorkerMode::Remote and settings.topology.workers.empty())
    {
        throw TestException("a remote run has no workers of its own, so it needs a topology: pass --clusterConfig");
    }

    /// In the embedded mode the coordinator answers the address it places a statement with no `HOST` clause on, and the
    /// run registers its first worker there.
    /// An empty answer leaves the run with no address to register that worker at.
    INVARIANT(mode == WorkerMode::Remote or not defaultHost.view().empty(), "the coordinator answers no default host");

    /// A topology declares every worker the run has, so registering the built-in one as well claims its data address twice.
    /// Without a topology the built-in one is all there is, at the address an omitted `HOST` clause resolves to.
    /// The coordinator can then place fragments on it, and a statement without a host reaches a worker that exists.
    if (settings.topology.workers.empty())
    {
        registerWorker(ConfigurationOverride{});
        return;
    }
    declared = registerTopology(settings.topology, defaultHost);
}

Placement Cluster::registerTopology(const ClusterConfiguration& topology, const Host& fallback) const
{
    /// Each worker the topology declares becomes one catalog row.
    /// In the embedded mode the coordinator starts a worker in this process for each of them, and memcom moves the data
    /// between them through memory rather than the network.
    for (const auto& worker : topology.workers)
    {
        const auto statement = registerTopologyWorkerStatement(worker);
        coordinator.get().submit_sql(rust::Str{statement.data(), statement.size()}, false);
    }

    /// The first allowed host, not one chosen to spread the work.
    /// An allowlist says where a placement is permitted and says nothing about which hosts the data can reach.
    /// `two-nodes-localhost.yaml` allows a sink on either node while only one is downstream of the source, and picking
    /// the other one fails to place the query at all.
    return Placement{
        .sources = topology.allowSourcePlacement.empty() ? fallback : topology.allowSourcePlacement.front(),
        .sinks = topology.allowSinkPlacement.empty() ? fallback : topology.allowSinkPlacement.front()};
}

std::optional<Placement> Cluster::placementFor(const ConfigurationOverride& settings)
{
    if (declared.has_value())
    {
        /// The topology fixes the set of workers and the harness started each one, so the run cannot hand a file settings of its own.
        /// Answering nothing has the caller skip those queries, which is better than registering a worker the topology does not
        /// describe and leaving the query with nowhere to be placed.
        if (not settings.overrideParameters.empty())
        {
            return std::nullopt;
        }
        return *declared;
    }

    /// One worker for both sides, because the query has to reach its sink from its source and nothing connects two
    /// workers the run invented for itself.
    if (const auto known = workersBySettings.find(settings); known != workersBySettings.end())
    {
        return Placement{.sources = known->second, .sinks = known->second};
    }
    const auto& registered = registerWorker(settings);
    return Placement{.sources = registered, .sinks = registered};
}

const Host& Cluster::registerWorker(const ConfigurationOverride& settings)
{
    const auto index = workersBySettings.size();
    Host address{workerHostAddress(defaultHost.view(), index)};

    /// The command line settings apply to every worker, and a test file's settings override them.
    /// The coordinator starts a worker from its catalog row, so the statement has to declare them.
    auto applied = baseSettings;
    for (const auto& [key, value] : settings.overrideParameters)
    {
        applied.insert_or_assign(key, value);
    }

    std::vector options{dataAddressOption(index)};
    for (const auto& [key, value] : applied)
    {
        options.push_back(fmt::format("{} AS {}", Sql::stringLiteral(value), key));
    }
    const auto statement = registerWorkerStatement(address.view(), fmt::to_string(fmt::join(options, ", ")));
    coordinator.get().submit_sql(rust::Str{statement.data(), statement.size()}, false);

    return workersBySettings.emplace(settings, std::move(address)).first->second;
}

}
