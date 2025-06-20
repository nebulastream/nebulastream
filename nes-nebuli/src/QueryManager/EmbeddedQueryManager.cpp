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

#include <QueryManager/EmbeddedQueryManager.hpp>

namespace NES
{
EmbeddedQueryManager::EmbeddedQueryManager(const Configuration::SingleNodeWorkerConfiguration& configuration) : worker(configuration)
{
}
std::expected<QueryId, Exception> EmbeddedQueryManager::registerQuery(const LogicalPlan& plan) noexcept
{
    return worker.registerQuery(plan);
}
std::expected<void, Exception> EmbeddedQueryManager::start(const QueryId queryId) noexcept
{
    return worker.startQuery(queryId);
}
std::expected<void, Exception> EmbeddedQueryManager::stop(const QueryId queryId) noexcept
{
    return worker.stopQuery(queryId, QueryTerminationType::Graceful);
}
std::expected<void, Exception> EmbeddedQueryManager::unregister(const QueryId queryId) noexcept
{
    return worker.unregisterQuery(queryId);
}
std::optional<QuerySummary> EmbeddedQueryManager::status(QueryId queryId) const
{
    return worker.getQuerySummary(queryId);
}
}