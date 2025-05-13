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
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestState.hpp>

#include <GRPCClient.hpp>

namespace NES::Systest
{

/// Interface for submitting queries to a NebulaStream Worker.
class QuerySubmitter
{
public:
    virtual ~QuerySubmitter() = default;
    virtual std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan) = 0;
    virtual void startQuery(QueryId query) = 0;
    virtual void stopQuery(QueryId query) = 0;
    virtual void unregisterQuery(QueryId query) = 0;
    virtual QuerySummary waitForQueryTermination(QueryId query) = 0;

    /// Blocks until atleast one query has finished (or potentially failed)
    virtual std::vector<QuerySummary> finishedQueries() = 0;
};

/// Launches an in process NebulaStream worker.
class LocalWorkerQuerySubmitter final : public QuerySubmitter
{
    std::unordered_set<QueryId> ids;
    SingleNodeWorker worker;

public:
    std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan) override;
    void startQuery(QueryId query) override;
    void stopQuery(QueryId query) override;
    void unregisterQuery(QueryId query) override;
    QuerySummary waitForQueryTermination(QueryId query) override;
    std::vector<QuerySummary> finishedQueries() override;
    explicit LocalWorkerQuerySubmitter(const Configuration::SingleNodeWorkerConfiguration& configuration);
};

/// Communicates via gRPC to a remote worker.
class RemoteWorkerQuerySubmitter final : public QuerySubmitter
{
    std::unordered_set<QueryId> ids;
    const GRPCClient client;

public:
    std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan) override;
    void startQuery(QueryId query) override;
    void stopQuery(QueryId query) override;
    void unregisterQuery(QueryId query) override;
    QuerySummary waitForQueryTermination(QueryId query) override;
    std::vector<QuerySummary> finishedQueries() override;
    explicit RemoteWorkerQuerySubmitter(const std::string& serverURI);
};
}
