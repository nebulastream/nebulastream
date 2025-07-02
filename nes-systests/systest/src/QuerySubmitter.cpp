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

#include <QuerySubmitter.hpp>

#include <chrono>
#include <optional>
#include <ranges>
#include <thread>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SingleNodeWorkerRPCService.pb.h>
#include <SystestState.hpp>

namespace NES::Systest
{

std::expected<QueryId, Exception> LocalWorkerQuerySubmitter::registerQuery(const LogicalPlan& plan)
{
    return worker.registerQuery(plan);
}
void LocalWorkerQuerySubmitter::startQuery(QueryId query)
{
    worker.startQuery(query);
    ids.emplace(query);
}
void LocalWorkerQuerySubmitter::stopQuery(QueryId query)
{
    worker.stopQuery(query, QueryTerminationType::Graceful);
}
void LocalWorkerQuerySubmitter::unregisterQuery(QueryId query)
{
    worker.unregisterQuery(query);
}

QuerySummary LocalWorkerQuerySubmitter::waitForQueryTermination(QueryId query)
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        const auto summary = worker.getQuerySummary(query);
        if (summary)
        {
            if (summary->currentStatus == QueryStatus::Stopped)
            {
                return *summary;
            }
        }
    }
}

std::vector<QuerySummary> LocalWorkerQuerySubmitter::finishedQueries()
{
    while (true)
    {
        std::vector<QuerySummary> results;
        for (auto id : ids)
        {
            if (auto summary = worker.getQuerySummary(id))
            {
                if (summary->currentStatus == QueryStatus::Failed || summary->currentStatus == QueryStatus::Stopped)
                {
                    results.emplace_back(std::move(*summary));
                }
            }
        }
        if (results.empty())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
            continue;
        }

        for (auto& result : results)
        {
            ids.erase(result.queryId);
        }

        return results;
    }
}
LocalWorkerQuerySubmitter::LocalWorkerQuerySubmitter(const Configuration::SingleNodeWorkerConfiguration& configuration)
    : worker(configuration)
{
}
std::expected<QueryId, Exception> RemoteWorkerQuerySubmitter::registerQuery(const LogicalPlan& plan)
{
    return QueryId(client.registerQuery(plan));
}
void RemoteWorkerQuerySubmitter::startQuery(const QueryId query)
{
    ids.insert(query);
    client.start(query);
}
void RemoteWorkerQuerySubmitter::stopQuery(const QueryId query)
{
    client.stop(query);
}
void RemoteWorkerQuerySubmitter::unregisterQuery(const QueryId query)
{
    client.unregister(query);
}
QuerySummary RemoteWorkerQuerySubmitter::waitForQueryTermination(const QueryId query)
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        const auto summary = client.status(query);
        if (summary.currentStatus == QueryStatus::Stopped)
        {
            return summary;
        }
    }
}
std::vector<QuerySummary> RemoteWorkerQuerySubmitter::finishedQueries()
{
    while (true)
    {
        std::vector<QuerySummary> results;
        for (auto id : ids)
        {
            auto summary = client.status(id);
            if (summary.currentStatus == QueryStatus::Failed || summary.currentStatus == QueryStatus::Stopped)
            {
                results.emplace_back(summary);
            }
        }
        if (results.empty())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
            continue;
        }

        for (auto& result : results)
        {
            ids.erase(result.queryId);
        }

        return results;
    }
}
RemoteWorkerQuerySubmitter::RemoteWorkerQuerySubmitter(const std::string& serverURI)
    : client(CreateChannel(serverURI, grpc::InsecureChannelCredentials()))
{
}
}
