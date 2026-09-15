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

#include <string>
#include <unordered_map>

#include <nes-coordinator-bridge/coordinator.h>

#include <Config/Topology.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// The hosts on which the sources and the sinks of one test file are created.
struct Placement
{
    Host sources;
    Host sinks;
};

/// The system under test of one run: a coordinator with an in-memory catalog, and the workers registered with it.
/// Destroying the cluster shuts down the coordinator and with it the embedded workers.
///
/// A worker reads its configuration once, when it starts.
/// A test file with configuration overrides therefore cannot reuse the workers of other test files.
/// In an embedded run, each distinct set of overrides gets its own copy of the topology,
/// with a suffix in the host: `sink-node:8080` becomes `sink-node-1:8080`.
/// Test files with equal overrides share a copy.
///
/// In a remote run the workers are separate processes that are already running.
/// Their configuration cannot be changed, so only the workers of the topology file are registered,
/// and the runner skips test files with overrides.
class Cluster
{
public:
    struct Config
    {
        Bridge::WorkerMode mode = Bridge::WorkerMode::Embedded;

        ClusterConfiguration topology;

        /// From the command line. The worker config applies to every worker and is overridden per test file.
        Schema<LiteralConfigValue, Ordered> optimizerConfig;
        Schema<LiteralConfigValue, Ordered> workerConfig;
    };

    /// Throws if the coordinator cannot be started, or if the topology has no worker or an empty allow list.
    explicit Cluster(const Config& config);

    /// Not a pure lookup: the first call with a new set of overrides registers a copy of the workers at the coordinator.
    /// Sources and sinks go to the first host of the allow lists of the topology.
    [[nodiscard]] Placement placementFor(const ConfigurationOverride& overrides);

    [[nodiscard]] const Bridge::Coordinator& getCoordinator() const { return *coordinator; }

    /// False for a remote run. Remote workers take no overrides and cannot reach a data server bound in this process.
    [[nodiscard]] bool runsInThisProcess() const { return mode == Bridge::WorkerMode::Embedded; }

private:
    /// Registers one more copy of the topology. The first copy keeps the hosts of the topology file.
    [[nodiscard]] Placement registerWorkers(const ConfigurationOverride& overrides) const;
    void submit(const std::string& statement) const;

    rust::Box<Bridge::Coordinator> coordinator;
    Bridge::WorkerMode mode;
    ClusterConfiguration topology;
    Schema<LiteralConfigValue, Ordered> baseWorkerConfig;
    std::unordered_map<ConfigurationOverride, Placement> placements;
};

}
