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

#include <chrono>
#include <thread>
#include <QueryManager/QueryManagementUtils.hpp>
#include <QueryManager/QueryManager.hpp>
#include <QueryManager/QuerySupervisor.hpp>

namespace NES
{

void QuerySupervisor::begin()
{
    spawnRepairThread();
    spawnHeartbeatThreads();
}

void QuerySupervisor::spawnHeartbeatThreads()
{
    auto distributedQuery = syncDistributedQuery.wlock();
    for (auto& [worker, localQueries] : distributedQuery->getLocalQueries())
    {
        for (auto& localQuery : localQueries)
        {
            heartbeatThreads.emplace_back(
                [this, &localQuery](std::stop_token st)
                {
                    // TODO this is kinda bad practice because we let the local query objects escape the synchronization
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                    bool hasFailed = false;
                    while (!st.stop_requested())
                    {
                        bool injectFailure
                            = !hasFailed && std::rand() % 5000 == 1 && localQuery.getHost().getRawValue().starts_with("source");
                        if (injectFailure)
                        {
                            hasFailed = true;
                        }
                        /// periodically check the status of the local query
                        QueryManagementUtils::checkLocalQueryStatus(
                            localQuery, queryManager.backends.at(localQuery.getHost()), injectFailure);
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }
                });
        }
    }
}

void QuerySupervisor::spawnRepairThread()
{
    repairThread = std::jthread(
        [this](std::stop_token st)
        {
            while (!st.stop_requested())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                // TODO maybe the timing is not ideal for clustered failures,. as some heartbeats may lag behind

                if (st.stop_requested())
                {
                    // avoid going into another iteration after the query was stopped
                    return;
                }

                auto distributedQuery = syncDistributedQuery.tryWLock();
                if (!distributedQuery)
                {
                    continue;
                }


                for (auto [_, query] : distributedQuery->iterate())
                {
                    INVARIANT(
                        query.getManagementState() == RUNNING,
                        "a correct query supervisor should converge to running state after each iteration");
                }

                /// check if query has finished (either succesful or failed); if so, tear down and stop the supervisor threads
                if (distributedQuery->checkQueryCompletion())
                {
                    std::vector<std::jthread> stopThreads{};
                    for (auto [_, query] : distributedQuery->iterate())
                    {
                        stopThreads.emplace_back(QueryManagementUtils::stopLocalQuery(query, queryManager.backends.at(query.getHost())));
                    }

                    heartbeatThreads.clear();
                    return;
                }

                /// check if any queries are missing
                /// missing query = worker crashed and has restarted
                auto missingQueries = findMissingQueries(distributedQuery);
                if (missingQueries.empty())
                {
                    continue;
                }

                /// collect recovery set by going down the subtrees of missing queries
                std::unordered_set<LocalQuery*> queriesToRestart;
                for (auto query : missingQueries)
                {
                    collectQueriesToRestartDFS(distributedQuery, query, queriesToRestart);
                }

                restartQueries(queriesToRestart);
            }
        });
}

std::unordered_set<LocalQuery*> QuerySupervisor::findMissingQueries(DistributedQueryGuard& guard)
{
    std::unordered_set<LocalQuery*> missingQueries;

    for (auto [host, query] : guard->iterate())
    {
        if (query.checkMissing())
        {
            missingQueries.insert(&query);
        }
    }
    return missingQueries;
}

void QuerySupervisor::collectQueriesToRestartDFS(
    DistributedQueryGuard& guard, LocalQuery* root, std::unordered_set<LocalQuery*>& queriesToRestart)
{
    if (queriesToRestart.contains(root))
    {
        return;
    }
    queriesToRestart.insert(root);

    // TODO maybe improve host lookup
    auto dataAddr = Host(queryManager.backends.getWorkerCatalog()->getWorker(root->getHost()).value().dataAddress);
    if (!guard->getUpstreamQueries().contains(dataAddr))
    {
        return; // found source node
    }

    for (auto* upstreamQuery : guard->getUpstreamQueries().at(dataAddr))
    {
        collectQueriesToRestartDFS(guard, upstreamQuery, queriesToRestart);
    }
}

void QuerySupervisor::restartQueries(const std::unordered_set<LocalQuery*>& queries)
{
    std::vector<std::jthread> repairThreads;
    for (auto query : queries)
    {
        repairThreads.emplace_back(QueryManagementUtils::stopLocalQuery(*query, queryManager.backends.at(query->getHost())));
    }

    repairThreads.clear(); // join barrier. Waits for all queries to stop

    for (auto query : queries)
    {
        repairThreads.emplace_back(QueryManagementUtils::restartLocalQuery(*query, queryManager.backends.at(query->getHost())));
    }
    repairThreads.clear();
}

}