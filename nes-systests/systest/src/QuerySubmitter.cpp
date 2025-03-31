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

namespace
{
NES::Runtime::QuerySummary deserialize(const QuerySummaryReply& reply)
{
    auto deserializeError = [](const QueryRunSummary& runSummery) -> std::optional<NES::Exception>
    {
        if (runSummery.has_error())
        {
            return NES::Exception(runSummery.error().message(), runSummery.error().code());
        }
        return {};
    };

    auto deserializeRun = [deserializeError](const QueryRunSummary& runSummery)
    {
        return NES::Runtime::QueryRunSummary{
            .start = std::chrono::system_clock::time_point(std::chrono::milliseconds(runSummery.startunixtimeinms())),
            .running = std::chrono::system_clock::time_point(std::chrono::milliseconds(runSummery.runningunixtimeinms())),
            .stop = std::chrono::system_clock::time_point(std::chrono::milliseconds(runSummery.stopunixtimeinms())),
            .error = deserializeError(runSummery)};
    };

    return NES::Runtime::QuerySummary{
        .queryId = NES::QueryId(reply.queryid()),
        .currentStatus = static_cast<NES::Runtime::Execution::QueryStatus>(reply.status()),
        .runs = std::views::transform(reply.runs(), deserializeRun) | std::ranges::to<std::vector>()};
}
}

NES::QueryId NES::Systest::LocalWorkerQuerySubmitter::registerQuery(const Query& query)
{
    return worker.registerQuery(query.queryPlan->copy());
}
void NES::Systest::LocalWorkerQuerySubmitter::startQuery(QueryId query)
{
    worker.startQuery(query);
    ids.emplace(query);
}
void NES::Systest::LocalWorkerQuerySubmitter::stopQuery(QueryId query)
{
    worker.stopQuery(query, Runtime::QueryTerminationType::Graceful);
}
void NES::Systest::LocalWorkerQuerySubmitter::unregisterQuery(QueryId query)
{
    worker.unregisterQuery(query);
}
std::vector<NES::Runtime::QuerySummary> NES::Systest::LocalWorkerQuerySubmitter::finishedQueries()
{
    while (true)
    {
        std::vector<Runtime::QuerySummary> results;
        for (auto id : ids)
        {
            if (auto summary = worker.getQuerySummary(id))
            {
                if (summary->currentStatus == Runtime::Execution::QueryStatus::Failed
                    || summary->currentStatus == Runtime::Execution::QueryStatus::Stopped)
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
NES::Systest::LocalWorkerQuerySubmitter::LocalWorkerQuerySubmitter(const Configuration::SingleNodeWorkerConfiguration& configuration)
    : worker(configuration)
{
}
NES::QueryId NES::Systest::RemoteWorkerQuerySubmitter::registerQuery(const Query& query)
{
    return QueryId(client.registerQuery(*query.queryPlan));
}
void NES::Systest::RemoteWorkerQuerySubmitter::startQuery(QueryId query)
{
    client.start(query.getRawValue());
}
void NES::Systest::RemoteWorkerQuerySubmitter::stopQuery(QueryId query)
{
    client.stop(query.getRawValue());
}
void NES::Systest::RemoteWorkerQuerySubmitter::unregisterQuery(QueryId query)
{
    client.unregister(query.getRawValue());
}
std::vector<NES::Runtime::QuerySummary> NES::Systest::RemoteWorkerQuerySubmitter::finishedQueries()
{
    while (true)
    {
        std::vector<Runtime::QuerySummary> results;
        for (auto id : ids)
        {
            auto summary = client.status(id.getRawValue());
            if (summary.status() == QueryStatus::Failed || summary.status() == QueryStatus::Stopped)
            {
                results.emplace_back(deserialize(summary));
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
NES::Systest::RemoteWorkerQuerySubmitter::RemoteWorkerQuerySubmitter(const std::string& serverURI)
    : client(CreateChannel(serverURI, grpc::InsecureChannelCredentials()))
{
}
