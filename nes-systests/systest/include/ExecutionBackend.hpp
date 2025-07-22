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

#include <expected>
#include <string>
#include <unordered_map>

#include <Distributed/DistributedQueryId.hpp>
#include <Listeners/QueryLog.hpp>
#include <ErrorHandling.hpp>
#include <GRPCClient.hpp>
#include <QueryConfig.hpp>
#include <QueryPlanning.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES::Systest
{

/// Interface for interacting with the backend.
class ExecutionBackend
{
public:
    virtual ~ExecutionBackend() = default;
    virtual std::expected<Distributed::QueryId, std::vector<Exception>> registerQuery(const QueryPlanner::FinalizedLogicalPlan& plan) = 0;
    virtual void startQuery(const Distributed::QueryId& query) = 0;
    virtual QueryStatus status(const Distributed::QueryId& query) = 0;
};

/// Launches an in process NebulaStream worker.
class EmbeddedWorkerBackend final : public ExecutionBackend
{
    SingleNodeWorker worker;

public:
    std::expected<Distributed::QueryId, std::vector<Exception>> registerQuery(const QueryPlanner::FinalizedLogicalPlan& plan) override;
    void startQuery(const Distributed::QueryId& query) override;
    QueryStatus status(const Distributed::QueryId& query) override;

    explicit EmbeddedWorkerBackend(const SingleNodeWorkerConfiguration& configuration);
};

/// Communicates via gRPC to a cluster of workers.
class ClusterBackend final : public ExecutionBackend
{
    struct ClusterNode
    {
        GRPCClient client;
        CLI::WorkerConfig workerConfig;
    };
    using Cluster = std::unordered_map<CLI::GrpcAddr, ClusterNode>;
    Cluster cluster{};

public:
    std::expected<Distributed::QueryId, std::vector<Exception>> registerQuery(const QueryPlanner::FinalizedLogicalPlan& plan) override;
    void startQuery(const Distributed::QueryId& query) override;
    QueryStatus status(const Distributed::QueryId& query) override;

    explicit ClusterBackend(const std::string& clusterConfigFile);
};

}
