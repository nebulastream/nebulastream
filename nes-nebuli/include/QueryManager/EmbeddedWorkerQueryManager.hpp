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
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

#include <QueryManager/QueryManager.hpp>

namespace NES
{
class EmbeddedWorkerQueryManager final : public QueryManager
{
public:
    explicit EmbeddedWorkerQueryManager(const SingleNodeWorkerConfiguration& configuration);
    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan) noexcept override;
    std::expected<void, Exception> start(QueryId queryId) noexcept override;
    std::expected<void, Exception> stop(QueryId queryId) noexcept override;
    std::expected<void, Exception> stopAll() noexcept override;
    std::expected<void, Exception> unregister(QueryId queryId) noexcept override;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(QueryId queryId) const noexcept override;

private:
    SingleNodeWorker worker;
};
}
