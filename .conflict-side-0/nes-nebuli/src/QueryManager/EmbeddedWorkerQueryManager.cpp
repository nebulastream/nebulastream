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

#include <QueryManager/EmbeddedWorkerQueryManager.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{
EmbeddedWorkerQueryManager::EmbeddedWorkerQueryManager(const SingleNodeWorkerConfiguration& configuration) : worker(configuration)
{
}

std::expected<QueryId, Exception> EmbeddedWorkerQueryManager::registerQuery(const LogicalPlan& plan) noexcept
{
    return worker.registerQuery(plan);
}

std::expected<void, Exception> EmbeddedWorkerQueryManager::start(const QueryId queryId) noexcept
{
    return worker.startQuery(queryId);
}

std::expected<void, Exception> EmbeddedWorkerQueryManager::stop(const QueryId queryId) noexcept
{
    return worker.stopQuery(queryId, QueryTerminationType::Graceful);
}

std::expected<void, Exception> EmbeddedWorkerQueryManager::unregister(const QueryId queryId) noexcept
{
    return worker.unregisterQuery(queryId);
}

std::expected<LocalQueryStatus, Exception> EmbeddedWorkerQueryManager::status(QueryId queryId) const noexcept
{
    return worker.getQueryStatus(queryId);
}
}
