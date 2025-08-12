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

#include <algorithm>
#include <chrono>
#include <exception>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <argparse/argparse.hpp>
#include <google/protobuf/text_format.h>
#include <grpcpp/create_channel.h>
#include <from_current.hpp>

#include <Distributed/DistributedQueryId.hpp>
#include <Distributed/WorkerStatus.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <YAML/YAMLBinder.hpp>
#include <ErrorHandling.hpp>
#include <GRPCClient.hpp>
#include <GrpcService.hpp>
#include <QueryPlanning.hpp>
#include <Repl.hpp>

namespace NES
{
std::unordered_map<std::string, NES::SingleNodeWorker> instantiate(const TopologyGraph& graph, const NodeCatalog& nodes)
{
    std::unordered_map<std::string, NES::SingleNodeWorker> servers;
    for (const auto& connection : graph | std::views::keys)
    {
        SingleNodeWorkerConfiguration configuration;
        configuration.connection = connection;
        configuration.workerConfiguration.defaultQueryExecution.executionMode = ExecutionMode::INTERPRETER;
        configuration.workerConfiguration.queryEngine.numberOfWorkerThreads = 1;
        servers.emplace(nodes.getGrpcAddressFor(connection), NES::SingleNodeWorker(configuration));
    }
    return servers;
}
}

int main(const int argc, char** argv)
{
    CPPTRACE_TRY
    {
        /// Initialize logging
        NES::Logger::setupLogging("nebuli.log", NES::LogLevel::LOG_DEBUG);
        argparse::ArgumentParser parser("nebuli-embedded");
        parser.add_argument("path");

        parser.parse_args(argc, argv);

        const auto path = parser.get<std::string>("path");
        auto [decomposedPlan, topology, catalog] = NES::QueryPlanner::plan(path);
        auto workers = instantiate(topology, catalog);

        std::vector<NES::Distributed::QueryId::ConnectionQueryIdPair> queryFragments;

        std::vector<std::pair<std::string, NES::QueryId>> pendingQueries;

        for (const auto& [grpcAddr, plans] : decomposedPlan)
        {
            try
            {
                auto& worker = workers.at(grpcAddr);

                for (const auto& plan : plans)
                {
                    const auto queryId = worker.registerQuery(plan).value();
                    queryFragments.emplace_back(grpcAddr, queryId.getRawValue());
                    worker.startQuery(queryId);
                    pendingQueries.emplace_back(grpcAddr, queryId);
                }
            }
            catch (const std::exception& e)
            {
                NES_ERROR("Failed to register query on host {}: {}", grpcAddr, e.what());
                throw;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        int attempts = 0;
        while (!pendingQueries.empty())
        {
            if (attempts > 80)
            {
                std::cerr << "deadlock?" << std::endl;
                exit(1);
            } else
            {
                std::cout << "running" << std::endl;
            }
            std::vector<std::pair<std::string, NES::QueryId>> newPendingQueries;
            for (auto [grpc, query] : pendingQueries)
            {
                auto queryStatus = workers.at(grpc).getQuerySummary(query)->currentStatus;
                if (queryStatus == NES::QueryStatus::Running || queryStatus == NES::QueryStatus::Started)
                {
                    newPendingQueries.push_back({grpc, query});
                }
            }
            pendingQueries = std::move(newPendingQueries);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            attempts++;
        }

        std::cout << "All Queries done";
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
