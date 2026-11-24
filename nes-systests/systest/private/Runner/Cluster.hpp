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

/// Sources and sinks go to the first host of the allow lists of the topology.
/// Throws if the topology has no worker or an empty allow list.
[[nodiscard]] Placement choosePlacement(const ClusterConfiguration& topology);

/// The system under test of the test files with equal configuration overrides:
/// a coordinator with an in-memory catalog, and the workers registered with it.
/// Destroying the cluster shuts down the coordinator and with it the embedded workers.
///
/// A worker reads its configuration once, when it starts.
/// A test file with configuration overrides therefore cannot reuse the workers of other test files.
/// A run creates one cluster per distinct set of overrides and destroys it before it creates the next,
/// because each embedded worker allocates its whole memory budget when it starts.
///
/// In a remote run the workers are separate processes that are already running.
/// A run cannot change their configuration, so the cluster registers them without settings, and the runner skips test files with overrides.
class Cluster
{
public:
    struct Config
    {
        Bridge::WorkerMode mode = Bridge::WorkerMode::Embedded;

        ClusterConfiguration topology;

        /// From the command line.
        /// The worker config applies to every worker, and the settings of a test file override it.
        Schema<LiteralConfigValue, Ordered> optimizerConfig;
        Schema<LiteralConfigValue, Ordered> workerConfig;

        /// The settings of the test files that this cluster runs.
        ConfigurationOverride overrides;
    };

    /// Registers the workers of the topology at the coordinator.
    /// Throws if the coordinator does not start or rejects a worker.
    explicit Cluster(const Config& config);

    [[nodiscard]] const Bridge::Coordinator& getCoordinator() const { return *coordinator; }

    /// False for a remote run, whose workers take no overrides.
    [[nodiscard]] bool runsInThisProcess() const { return mode == Bridge::WorkerMode::Embedded; }

private:
    rust::Box<Bridge::Coordinator> coordinator;
    Bridge::WorkerMode mode;
};

}
