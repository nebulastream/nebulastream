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

#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryManager/QueryManager.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

namespace
{
/// Minimal backend that reports every stop as successful, so QueryManager::stop() reaches the
/// point where it removes the query from its state. Only stop() is exercised by this backend.
class SucceedingStopBackend final : public QuerySubmissionBackend
{
public:
    [[nodiscard]] std::expected<QueryId, Exception> start(LogicalPlan) override { return QueryId::invalid(); }

    std::expected<void, Exception> stop(QueryId) override { return {}; }

    [[nodiscard]] std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId) const override
    {
        return std::unexpected(QueryNotFound("not used"));
    }

    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point) const override
    {
        return std::unexpected(QueryNotFound("not used"));
    }

    [[nodiscard]] std::expected<VersionInfo, Exception> version() const override { return std::unexpected(QueryNotFound("not used")); }
};
}

class QueryManagerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("QueryManagerTest.log", LogLevel::LOG_DEBUG); }
};

/// Regression test: stop() must remove the query from the QueryManager's state. A prior bug moved
/// the by-value queryId into getQuery() and then erased with the emptied id, leaving the entry
/// registered forever.
TEST_F(QueryManagerTest, stopRemovesTheRegisteredQuery)
{
    const Host host{"127.0.0.1:9999"};
    const DistributedQueryId queryId{"regression-test"};

    auto workerCatalog = std::make_shared<WorkerCatalog>();
    ASSERT_TRUE(workerCatalog->addWorker(host, "127.0.0.1:9999", CapacityKind::Unlimited{}, {}));

    QueryManagerState state;
    state.queries.emplace(
        queryId, DistributedQuery{std::unordered_map<Host, std::vector<QueryId>>{{host, {QueryId::createDistributed(queryId)}}}});

    QueryManager queryManager{
        workerCatalog,
        [](const WorkerConfig&) -> UniquePtr<QuerySubmissionBackend> { return std::make_unique<SucceedingStopBackend>(); },
        std::move(state)};

    ASSERT_EQ(queryManager.queries().size(), 1U);

    const auto result = queryManager.stop(queryId);
    ASSERT_TRUE(result.has_value()) << "stop() reported failure";

    EXPECT_TRUE(queryManager.queries().empty()) << "stop() reported success but left the query registered";
    EXPECT_FALSE(queryManager.getQuery(queryId).has_value());
}

}
