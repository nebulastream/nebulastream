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

/// Runs ISQP/ISQD end to end with embedded workers that communicate over the in-memory transport: a query runs on a leaf, a base station
/// and the cloud while the leaf moves to another base station. The query must deliver every tuple exactly once.

#include <QueryManager/IncrementalPlacementRequest.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <Configurations/Util.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <nes-network-bindings/lib.h>
#include <BaseUnitTest.hpp>
#include <DistributedQuery.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <TopologyChangeEvent.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <WorkerOptimizerConfig.hpp>

namespace NES
{

class IncrementalPlacementRequestTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("IncrementalPlacementRequestTest.log", LogLevel::LOG_DEBUG);
        enable_memcom();
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        outputFile = std::filesystem::temp_directory_path() / ("isqd-" + UUIDToString(generateUUID()) + ".csv");
    }

    void TearDown() override
    {
        std::filesystem::remove(outputFile);
        BaseUnitTest::TearDown();
    }

    /// leaf ---> base1 ---> cloud
    ///           base2 ---/
    /// Worker names get a per-test suffix, as the in-memory transport is process-wide.
    void setUpCluster(const std::string& suffix, const size_t baseCapacity)
    {
        prefix = suffix;
        const auto add = [&](const std::string& name, const Capacity capacity, const std::vector<Host>& downstream)
        { workerCatalog->addWorker(host(name), name + suffix + ":9090", capacity, downstream); };
        add("cloud", CapacityKind::Unlimited{}, {});
        add("base1", CapacityKind::Limited{baseCapacity}, {host("cloud")});
        add("base2", CapacityKind::Limited{baseCapacity}, {host("cloud")});
        add("leaf", CapacityKind::Limited{0}, {host("base1")});

        const Schema<UnqualifiedUnboundField, Ordered> schema{UnqualifiedUnboundField{Identifier::parse("id"), DataType::Type::UINT64}};
        const auto logicalSource = sourceCatalog->addLogicalSource(Identifier::parse("stream"), schema);
        ASSERT_TRUE(logicalSource.has_value());
        ASSERT_TRUE(sourceCatalog
                        ->addPhysicalSource(
                            *logicalSource,
                            Identifier::parse("Generator"),
                            host("leaf"),
                            {{Identifier::parse("stop_generator_when_sequence_finishes"), "ALL"},
                             {Identifier::parse("seed"), "1"},
                             {Identifier::parse("generator_rate_type"), "FIXED"},
                             {Identifier::parse("generator_rate_config"), "emit_rate 2000"},
                             {Identifier::parse("generator_schema"), fmt::format("SEQUENCE UINT64 0 {} 1", NUMBER_OF_TUPLES)}},
                            {{Identifier::parse("type"), "CSV"}})
                        .has_value());
        ASSERT_TRUE(sinkCatalog
                        ->addSinkDescriptor(
                            Identifier::parse("snk"),
                            schema,
                            Identifier::parse("File"),
                            host("cloud"),
                            {{Identifier::parse("FILE_PATH"), outputFile.string()}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}},
                            {})
                        .has_value());
    }

    [[nodiscard]] Host host(const std::string& name) const { return Host(name + prefix + ":8080"); }

    std::shared_ptr<QueryManager> createQueryManager()
    {
        auto resolveWorker = [](const WorkerConfig&) { return defaultConfiguration<WorkerOptimizerConfig>().worker; };
        return std::make_shared<QueryManager>(copyPtr(workerCatalog), createEmbeddedBackend(std::move(resolveWorker)));
    }

    DistributedQueryId startQuery(QueryManager& queryManager, const std::string& sql)
    {
        const QueryOptimizer optimizer(
            defaultConfiguration<QueryOptimizerConfiguration>(),
            copyPtr(sourceCatalog),
            copyPtr(sinkCatalog),
            copyPtr(workerCatalog),
            modelCatalog);
        auto started = queryManager.start(optimizer.optimize(AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql)));
        EXPECT_TRUE(started.has_value());
        return started.value_or(DistributedQueryId(DistributedQueryId::INVALID));
    }

    IncrementalPlacementResult moveLeafToBase2(QueryManager& queryManager, const bool incremental)
    {
        IncrementalPlacementRequest request(
            queryManager,
            copyPtr(workerCatalog),
            QueryDecomposer(copyPtr(workerCatalog), copyPtr(sourceCatalog), copyPtr(sinkCatalog)),
            defaultConfiguration<QueryOptimizerConfiguration>().network,
            IncrementalPlacementConfiguration{.enableIncrementalPlacement = incremental, .drainTimeout = std::chrono::seconds(30)});
        return request.execute(
            {RemoveLinkEvent{.upstream = host("leaf"), .downstream = host("base1")},
             AddLinkEvent{.upstream = host("leaf"), .downstream = host("base2")}});
    }

    static void waitForStop(QueryManager& queryManager, const DistributedQueryId& queryId)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            const auto status = queryManager.status(queryId);
            ASSERT_TRUE(status.has_value());
            const auto global = status->getGlobalQueryStatus();
            ASSERT_NE(global, DistributedQueryStatus::Failed);
            if (global == DistributedQueryStatus::Stopped)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        FAIL() << "Query did not stop in time";
    }

    /// Every tuple of the generator must be in the output exactly once.
    void expectEveryTupleExactlyOnce() const
    {
        std::ifstream file(outputFile);
        std::string line;
        std::multiset<uint64_t> ids;
        while (std::getline(file, line))
        {
            if (line.empty() or not std::isdigit(static_cast<unsigned char>(line.front())))
            {
                continue;
            }
            ids.insert(std::stoull(line));
        }
        ASSERT_FALSE(ids.empty());
        const std::set<uint64_t> distinct(ids.begin(), ids.end());
        EXPECT_EQ(distinct.size(), ids.size()) << "duplicates in the output";
        EXPECT_EQ(*distinct.begin(), 0U);
        EXPECT_EQ(*distinct.rbegin() + 1, distinct.size()) << "missing tuples in the output";
        EXPECT_GE(distinct.size(), NUMBER_OF_TUPLES);
    }

    static constexpr uint64_t NUMBER_OF_TUPLES = 4000;
    std::string prefix;
    std::filesystem::path outputFile;
    SharedPtr<WorkerCatalog> workerCatalog = std::make_shared<WorkerCatalog>();
    SharedPtr<SourceCatalog> sourceCatalog = std::make_shared<SourceCatalog>();
    SharedPtr<SinkCatalog> sinkCatalog = std::make_shared<SinkCatalog>();
    std::shared_ptr<ModelCatalog> modelCatalog = std::make_shared<ModelCatalog>();
};

/// Only the path changes: the forwarding plan on base1 is drained, the plan of the leaf is updated to send over base2, and the plan of
/// the sink keeps running.
TEST_F(IncrementalPlacementRequestTest, PathChange)
{
    setUpCluster("-path", 0);
    const auto queryManager = createQueryManager();
    const auto queryId = startQuery(*queryManager, "SELECT * FROM stream INTO snk");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    const auto result = moveLeafToBase2(*queryManager, true);
    EXPECT_EQ(result.numberOfAffectedQueries, 1U);
    EXPECT_EQ(result.numberOfFailedAmendments, 0U);
    const auto plans = queryManager->getGlobalExecutionPlan().getDecomposedQueryPlans(queryId);
    EXPECT_EQ(queryManager->getGlobalExecutionPlan().getDecomposedQueryPlans(queryId, host("base1")).size(), 0U);
    EXPECT_EQ(queryManager->getGlobalExecutionPlan().getDecomposedQueryPlans(queryId, host("base2")).size(), 1U);
    EXPECT_EQ(plans.size(), 3U);

    waitForStop(*queryManager, queryId);
    expectEveryTupleExactlyOnce();
}

/// The selection on base1 is re-placed: its plan is drained and a new plan runs it on a worker reachable from the leaf.
TEST_F(IncrementalPlacementRequestTest, OperatorReplacement)
{
    setUpCluster("-operator", 1);
    const auto queryManager = createQueryManager();
    const auto queryId = startQuery(*queryManager, "SELECT * FROM stream WHERE id >= 0 INTO snk");
    ASSERT_EQ(queryManager->getGlobalExecutionPlan().getDecomposedQueryPlans(queryId, host("base1")).size(), 1U);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    const auto result = moveLeafToBase2(*queryManager, true);
    EXPECT_EQ(result.numberOfFailedAmendments, 0U);
    EXPECT_EQ(queryManager->getGlobalExecutionPlan().getDecomposedQueryPlans(queryId, host("base1")).size(), 0U);

    waitForStop(*queryManager, queryId);
    expectEveryTupleExactlyOnce();
}

}
