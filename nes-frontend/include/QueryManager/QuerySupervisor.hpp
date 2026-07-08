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
#include <DistributedQuery.hpp>

namespace NES
{

class QueryManager;

class QuerySupervisor
{
public:
    QuerySupervisor(QueryManager& queryManager, folly::Synchronized<DistributedQuery>& distributedQuery)
        : queryManager(queryManager), syncDistributedQuery(distributedQuery)
    {
    }

    void begin();

    ~QuerySupervisor()
    {
        repairThread.request_stop();
        repairThread.join();
        heartbeatThreads.clear();
    }

private:
    using DistributedQueryGuard = decltype(std::declval<folly::Synchronized<DistributedQuery>&>().tryWLock());

    void spawnRepairThread();
    void spawnHeartbeatThreads();
    std::unordered_set<LocalQuery*> collectQueriesToRestart(DistributedQueryGuard& guard);
    void collectQueriesToRestartDFS(DistributedQueryGuard& guard, LocalQuery* root, std::unordered_set<LocalQuery*>& queriesToRestart);
    void restartQueries(const std::unordered_set<LocalQuery*>& queries);
    bool checkQueryCompletion(DistributedQueryGuard& guard);
    std::unordered_set<LocalQuery*> findMissingQueries(DistributedQueryGuard& guard);

    QueryManager& queryManager;
    folly::Synchronized<DistributedQuery>& syncDistributedQuery;
    std::jthread repairThread;
    std::vector<std::jthread> heartbeatThreads;
};


}