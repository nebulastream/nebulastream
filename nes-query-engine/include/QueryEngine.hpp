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
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryEngineStatisticListener.hpp>

namespace NES
{
/// Forward declaration so that only the QueryEngine can be included
class QueryCatalog;
class ThreadPool;

class QueryEngine
{
public:
    explicit QueryEngine(
        const QueryEngineConfiguration& configuration,
        std::vector<std::shared_ptr<QueryEngineStatisticListener>> statListener,
        std::shared_ptr<AbstractQueryStatusListener> listener,
        std::shared_ptr<Memory::BufferManager> bm);
    void stop(QueryId queryId);
    void start(std::unique_ptr<ExecutableQueryPlan> executableQueryPlan);
    ~QueryEngine();

    /// Order of Member construction is top to bottom and order of destruction is reversed
    /// Starting the ThreadPool is the very **last** thing the query engine does and **stopping**
    /// the ThreadPool is the first thing that happens during destruction.
    /// This implies that there will be no ThreadPool and WorkQueue around to handle the termination of
    /// left over running queries. Dropping a RunningQueryPlan has to invoke a HardStop which must not emit
    /// further work into the TaskQueue.
    std::shared_ptr<Memory::BufferManager> bufferManager;
    std::shared_ptr<AbstractQueryStatusListener> statusListener;
    std::vector<std::shared_ptr<QueryEngineStatisticListener>> statisticListener;
    std::shared_ptr<QueryCatalog> queryCatalog;
    std::unique_ptr<ThreadPool> threadPool;
};

}
