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

#include <functional>
#include <optional>
#include <string>
#include <unordered_map>

#include <coordinator/lib.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Runner/Topology.hpp>

namespace NES
{

/// Where a run puts one test file's sources and its sinks, which the rewriter needs before it emits SQL.
struct Placement
{
    Host sources;
    Host sinks;
};

/// The workers a run has, and where it places the sources and the sinks of each test file.
/// A topology declares the workers up front and fixes where a placement may go.
/// An invocation that gives no topology registers one worker per set of settings a test file asks for.
class Cluster
{
public:
    /// Where the workers run, what a topology declares about them, and the settings every one of them starts with.
    struct Settings
    {
        /// What the coordinator does with the worker a catalog row describes: it starts one in this process, or it
        /// sends commands over gRPC to one already running at that address.
        /// In the remote mode it starts no worker and answers no default host, so the run reads both the workers and
        /// their addresses from the topology and requires one.
        WorkerMode mode = WorkerMode::Embedded;

        /// The workers a topology declares, which this registers instead of the one at the address the coordinator answers with.
        /// An invocation that gives no topology leaves this empty and runs against that one worker.
        ClusterConfiguration topology;

        /// The settings the command line gave, which every worker starts with before a test file's settings apply.
        std::unordered_map<std::string, std::string> workerSettings;
    };

    /// Registers the workers the run starts with, and rejects an invocation that describes none.
    Cluster(const EmbeddedCoordinator& coordinator, const Settings& settings);

    /// Returns where a test file asking for these settings goes, and nothing when the run cannot give it those settings.
    /// Without a topology both sides go on one worker, registered the first time a set of settings appears, because a
    /// worker takes its settings at startup and two workers with no edge between them cannot run one query.
    /// With a topology they go on the hosts it allows, which may differ.
    [[nodiscard]] std::optional<Placement> placementFor(const ConfigurationOverride& settings);

    /// Whether the workers run in this process, which decides whether a test file may serve a source over a socket.
    /// A worker in another process resolves `localhost` to itself, so it cannot read from a server bound here.
    [[nodiscard]] bool runsInThisProcess() const { return mode == WorkerMode::Embedded; }

private:
    /// Registers a worker for these settings and returns its address.
    const Host& registerWorker(const ConfigurationOverride& settings);

    /// Registers every worker a topology declares, and returns where its sources and its sinks go.
    /// A topology that allows a placement nowhere puts that side on the fallback host, so a file missing a list still
    /// runs rather than emitting DDL with no host.
    [[nodiscard]] Placement registerTopology(const ClusterConfiguration& topology, const Host& fallback) const;

    /// The coordinator this submits each `CREATE WORKER` to.
    std::reference_wrapper<const EmbeddedCoordinator> coordinator;

    /// Where the workers run, which decides what this may assume about the filesystem they see.
    /// A run whose workers are elsewhere assumes they see the paths this process writes, which the harness mounts.
    /// Nothing here can check that, so a source file a worker cannot open fails on the worker rather than here.
    WorkerMode mode;

    /// The hosts a topology allows, unset when the invocation gave no topology.
    /// A topology fixes the set of workers, which rules out a worker per configuration override.
    std::optional<Placement> declared;

    /// The address the coordinator places a statement without a `HOST` clause on, which the workers below count up from.
    Host defaultHost;

    /// The settings the command line gave, which every worker starts with before a test file's settings apply.
    std::unordered_map<std::string, std::string> baseSettings;

    /// One worker per distinct set of settings, so test files asking for the same settings share a worker.
    std::unordered_map<ConfigurationOverride, Host> workersBySettings;
};

}
