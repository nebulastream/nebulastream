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
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <CompilationCache.hpp>
#include <LegacyOptimizer.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <QueryOptimizer.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <StatementHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <options.hpp>

namespace NES
{
namespace
{
struct QueryScenario
{
    std::vector<std::string> setupStatements;
    std::string query;
};
}

class PlanRecoveryCompilationCacheTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("PlanRecoveryCompilationCacheTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup PlanRecoveryCompilationCacheTest test case.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        sourceCatalog = std::make_shared<SourceCatalog>();
        sinkCatalog = std::make_shared<SinkCatalog>();
        binder = std::make_shared<StatementBinder>(
            sourceCatalog,
            [](auto&& queryContext)
            { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(queryContext)>(queryContext)); });
        sourceStatementHandler = std::make_unique<SourceStatementHandler>(sourceCatalog);
        sinkStatementHandler = std::make_unique<SinkStatementHandler>(sinkCatalog);
        legacyOptimizer = std::make_unique<LegacyOptimizer>(sourceCatalog, sinkCatalog);
    }

protected:
    void applyCatalogStatement(std::string_view sql) const
    {
        const auto statementResult = binder->parseAndBindSingle(sql);
        ASSERT_TRUE(statementResult.has_value()) << statementResult.error();
        const auto& statement = statementResult.value();

        if (std::holds_alternative<CreateLogicalSourceStatement>(statement))
        {
            const auto result = sourceStatementHandler->apply(std::get<CreateLogicalSourceStatement>(statement));
            ASSERT_TRUE(result.has_value()) << result.error();
            return;
        }
        if (std::holds_alternative<CreatePhysicalSourceStatement>(statement))
        {
            const auto result = sourceStatementHandler->apply(std::get<CreatePhysicalSourceStatement>(statement));
            ASSERT_TRUE(result.has_value()) << result.error();
            return;
        }
        if (std::holds_alternative<CreateSinkStatement>(statement))
        {
            const auto result = sinkStatementHandler->apply(std::get<CreateSinkStatement>(statement));
            ASSERT_TRUE(result.has_value()) << result.error();
            return;
        }

        FAIL() << "Unexpected non-catalog statement in setup: " << sql;
    }

    [[nodiscard]] LogicalPlan buildOptimizedPlan(const QueryScenario& scenario) const
    {
        for (const auto& sql : scenario.setupStatements)
        {
            applyCatalogStatement(sql);
        }

        const auto statementResult = binder->parseAndBindSingle(scenario.query);
        EXPECT_TRUE(statementResult.has_value()) << statementResult.error();
        EXPECT_TRUE(std::holds_alternative<QueryStatement>(statementResult.value()));

        auto optimizedPlan = legacyOptimizer->optimize(std::get<QueryStatement>(statementResult.value()));
        optimizedPlan.setQueryId(QueryId(1));
        return optimizedPlan;
    }

    static void appendPipelineCacheKeysRecursively(
        const std::shared_ptr<Pipeline>& pipeline,
        QueryCompilation::CompilationCache& compilationCache,
        std::unordered_set<uint64_t>& visitedPipelineIds,
        std::vector<std::string>& cacheKeys)
    {
        if (pipeline->isSinkPipeline())
        {
            return;
        }

        if (pipeline->isOperatorPipeline())
        {
            const auto pipelineId = pipeline->getPipelineId().getRawValue();
            if (!visitedPipelineIds.insert(pipelineId).second)
            {
                return;
            }

            nautilus::engine::Options options;
            compilationCache.configureEngineOptionsForPipeline(options, pipeline);
            const auto cacheKey = options.getOptionOrDefault<std::string>("engine.Blob.CacheKey", "");
            EXPECT_FALSE(cacheKey.empty());
            cacheKeys.emplace_back(cacheKey);
        }

        for (const auto& successor : pipeline->getSuccessors())
        {
            appendPipelineCacheKeysRecursively(successor, compilationCache, visitedPipelineIds, cacheKeys);
        }
    }

    [[nodiscard]] std::vector<std::string> collectExplicitPipelineCacheKeys(const LogicalPlan& plan) const
    {
        auto physicalPlan = queryOptimizer.optimize(plan);
        auto compilationCache = QueryCompilation::CompilationCache(QueryCompilation::CompilationCache::Settings{true, "/tmp/unused"});
        compilationCache.prepareForQuery(physicalPlan);

        auto pipelinedQueryPlan = QueryCompilation::PipeliningPhase::apply(physicalPlan);
        compilationCache.resetPipelineOrdinals();

        std::unordered_set<uint64_t> visitedPipelineIds;
        std::vector<std::string> cacheKeys;
        for (const auto& sourcePipeline : pipelinedQueryPlan->getSourcePipelines())
        {
            for (const auto& successor : sourcePipeline->getSuccessors())
            {
                appendPipelineCacheKeysRecursively(successor, compilationCache, visitedPipelineIds, cacheKeys);
            }
        }
        return cacheKeys;
    }

    void expectRoundTripPlanRecoveryPreservesExplicitCacheKeys(const QueryScenario& scenario)
    {
        const auto optimizedPlan = buildOptimizedPlan(scenario);
        const auto serializedPlan = QueryPlanSerializationUtil::serializeQueryPlan(optimizedPlan);
        const auto recoveredPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedPlan);

        EXPECT_EQ(recoveredPlan, optimizedPlan);
        EXPECT_EQ(recoveredPlan.getQueryId(), optimizedPlan.getQueryId());

        const auto explicitCacheKeysBeforeRecovery = collectExplicitPipelineCacheKeys(optimizedPlan);
        ASSERT_FALSE(explicitCacheKeysBeforeRecovery.empty());

        const auto explicitCacheKeysAfterRecovery = collectExplicitPipelineCacheKeys(recoveredPlan);
        EXPECT_EQ(explicitCacheKeysAfterRecovery, explicitCacheKeysBeforeRecovery);
    }

    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
    std::shared_ptr<StatementBinder> binder;
    std::unique_ptr<SourceStatementHandler> sourceStatementHandler;
    std::unique_ptr<SinkStatementHandler> sinkStatementHandler;
    std::unique_ptr<LegacyOptimizer> legacyOptimizer;
    QueryOptimizer queryOptimizer{QueryExecutionConfiguration{}};
};

TEST_F(PlanRecoveryCompilationCacheTest, AggregationCheckpointPlanRecoveryPreservesCompilationCacheKeys)
{
    const auto scenario = QueryScenario{
        .setupStatements
        = {
            "CREATE LOGICAL SOURCE stream1(i64 INT64, ts UINT64)",
            R"(CREATE PHYSICAL SOURCE FOR stream1 TYPE File SET ('/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER))",
            R"(CREATE SINK sinkStream1(stream1.start UINT64, stream1.end UINT64, stream1.i64_out UINT64) TYPE File SET ('/dev/null' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT))"},
        .query = "SELECT start, end, COUNT(i64) as i64_out FROM stream1 WINDOW TUMBLING(ts, size 100ms) INTO sinkStream1"};

    expectRoundTripPlanRecoveryPreservesExplicitCacheKeys(scenario);
}

TEST_F(PlanRecoveryCompilationCacheTest, JoinCheckpointPlanRecoveryPreservesCompilationCacheKeys)
{
    const auto scenario = QueryScenario{
        .setupStatements
        = {
            "CREATE LOGICAL SOURCE left_stream(id UINT64, ts UINT64)",
            R"(CREATE PHYSICAL SOURCE FOR left_stream TYPE File SET ('/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER))",
            "CREATE LOGICAL SOURCE right_stream(id2 UINT64, ts UINT64)",
            R"(CREATE PHYSICAL SOURCE FOR right_stream TYPE File SET ('/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER))",
            R"(CREATE SINK sinkJoinCk(left_streamright_stream.start UINT64, left_streamright_stream.end UINT64, left_stream.id UINT64, left_stream.ts UINT64, right_stream.id2 UINT64, right_stream.ts UINT64) TYPE File SET ('/dev/null' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT))"},
        .query = R"(SELECT *
FROM (SELECT * FROM left_stream) INNER JOIN (SELECT * FROM right_stream)
ON id = id2 WINDOW TUMBLING(ts, size 100ms)
INTO sinkJoinCk)"};

    expectRoundTripPlanRecoveryPreservesExplicitCacheKeys(scenario);
}

}
