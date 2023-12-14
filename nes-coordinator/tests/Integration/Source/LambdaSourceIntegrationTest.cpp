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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Operators/LogicalOperators/Sinks/LogicalSinkOperator.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/QueryService.hpp>
#include <Util/TestUtils.hpp>

using std::string;
namespace NES {
class LambdaSourceIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LambdaSourceIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LambdaSourceIntegrationTest test class.");
    }
};

TEST_F(LambdaSourceIntegrationTest, testTwoLambdaSources) {
    NES::CoordinatorConfigurationPtr coordinatorConfig = NES::CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->coordinatorHealthCheckWaitTime = 1;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_DEBUG("E2EBase: Start coordinator");
    auto crd = std::make_shared<NES::NesCoordinator>(coordinatorConfig);

    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    auto input = Schema::create()
                     ->addField(createField("id", BasicType::UINT64))
                     ->addField(createField("value", BasicType::UINT64))
                     ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("input1", input);
    crd->getSourceCatalogService()->registerLogicalSource("input2", input);

    NES_DEBUG("E2EBase: Start worker 1");
    NES::WorkerConfigurationPtr wrkConf = NES::WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    wrkConf->workerHealthCheckWaitTime = 1;
    wrkConf->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;

    auto func1 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        auto* records = buffer.getBuffer<Record>();
        auto ts = time(nullptr);
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = ts;
        }
    };

    auto func2 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        auto* records = buffer.getBuffer<Record>();
        auto ts = time(nullptr);
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = ts;
        }
    };

    auto lambdaSourceType1 =
        LambdaSourceType::create("input1", "test_stream1", std::move(func1), 3, 10, GatheringMode::INTERVAL_MODE);
    auto lambdaSourceType2 =
        LambdaSourceType::create("input2", "test_stream2", std::move(func2), 3, 10, GatheringMode::INTERVAL_MODE);
    wrkConf->physicalSourceTypes.add(lambdaSourceType1);
    wrkConf->physicalSourceTypes.add(lambdaSourceType2);
    auto wrk1 = std::make_shared<NES::NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);

    auto query = Query::from("input1")
                     .joinWith(Query::from("input2"))
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                     .sink(NullOutputSinkDescriptor::create());

    NES::QueryServicePtr queryService = crd->getQueryService();
    auto queryCatalog = crd->getQueryCatalogService();
    auto queryId = queryService->validateAndQueueAddQueryRequest(query.getQueryPlan()->toString(),
                                                                 query.getQueryPlan(),
                                                                 Optimizer::PlacementStrategy::BottomUp);

    bool ret = NES::TestUtils::checkStoppedOrTimeout(queryId, queryCatalog);
    if (!ret) {
        NES_ERROR("query was not stopped within 30 sec");
    }

    NES_DEBUG("E2EBase: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    ASSERT_TRUE(retStopWrk1);

    NES_DEBUG("E2EBase: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    ASSERT_TRUE(retStopCord);
    NES_DEBUG("E2EBase: Test finished");
}

TEST_F(LambdaSourceIntegrationTest, testTwoLambdaSourcesWithSamePhysicalName) {
    NES::CoordinatorConfigurationPtr crdConf = NES::CoordinatorConfiguration::createDefault();
    crdConf->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_DEBUG("E2EBase: Start coordinator");
    auto crd = std::make_shared<NES::NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    auto input = Schema::create()
                     ->addField(createField("id", BasicType::UINT64))
                     ->addField(createField("value", BasicType::UINT64))
                     ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("input1", input);
    crd->getSourceCatalogService()->registerLogicalSource("input2", input);

    NES_DEBUG("E2EBase: Start worker 1");
    NES::WorkerConfigurationPtr wrkConf = NES::WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    wrkConf->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto func1 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        auto* records = buffer.getBuffer<Record>();
        auto ts = time(nullptr);
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = ts;
        }
    };

    auto func2 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        auto* records = buffer.getBuffer<Record>();
        auto ts = time(nullptr);
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = ts;
        }
    };

    auto lambdaSourceType1 =
        LambdaSourceType::create("input1", "test_stream", std::move(func1), 3, 10, GatheringMode::INTERVAL_MODE);
    auto lambdaSourceType2 =
        LambdaSourceType::create("input2", "test_stream", std::move(func2), 3, 10, GatheringMode::INTERVAL_MODE);
    wrkConf->physicalSourceTypes.add(lambdaSourceType1);
    wrkConf->physicalSourceTypes.add(lambdaSourceType2);
    auto wrk1 = std::make_shared<NES::NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    NES_ASSERT(retStart1, "retStart1");

    auto query1 = Query::from("input1").filter(Attribute("value") > 10000).sink(NullOutputSinkDescriptor::create());

    auto query2 = Query::from("input2").filter(Attribute("value") > 10000).sink(NullOutputSinkDescriptor::create());

    NES::QueryServicePtr queryService = crd->getQueryService();
    auto queryCatalog = crd->getQueryCatalogService();
    auto queryId1 = queryService->validateAndQueueAddQueryRequest(query1.getQueryPlan()->toString(),
                                                                  query1.getQueryPlan(),
                                                                  Optimizer::PlacementStrategy::BottomUp);

    auto queryId2 = queryService->validateAndQueueAddQueryRequest(query2.getQueryPlan()->toString(),
                                                                  query2.getQueryPlan(),
                                                                  Optimizer::PlacementStrategy::BottomUp);

    bool ret = NES::TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog);
    if (!ret) {
        NES_ERROR("query 1 was not stopped within 30 sec");
    }
    bool ret2 = NES::TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog);
    if (!ret2) {
        NES_ERROR("query 2 was not stopped within 30 sec");
    }

    NES_DEBUG("E2EBase: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    NES_ASSERT(retStopWrk1, "retStopWrk1");

    NES_DEBUG("E2EBase: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    NES_ASSERT(retStopCord, "retStopCord");
    NES_DEBUG("E2EBase: Test finished");
}

TEST_F(LambdaSourceIntegrationTest, testTwoLambdaSourcesMultiThread) {
    NES::CoordinatorConfigurationPtr coordinatorConfig = NES::CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.numWorkerThreads = 4;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;

    NES_DEBUG("E2EBase: Start coordinator");

    auto crd = std::make_shared<NES::NesCoordinator>(coordinatorConfig);
    auto port = crd->startCoordinator(/**blocking**/ false);
    auto input = Schema::create()
                     ->addField(createField("id", BasicType::UINT64))
                     ->addField(createField("value", BasicType::UINT64))
                     ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("input", input);

    NES::WorkerConfigurationPtr wrkConf = NES::WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    wrkConf->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    for (int64_t i = 0; i < 2; i++) {
        auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
            struct Record {
                uint64_t id;
                uint64_t value;
                uint64_t timestamp;
            };

            auto* records = buffer.getBuffer<Record>();
            auto ts = time(nullptr);
            for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                records[u].id = u;
                //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                records[u].value = u % 10;
                records[u].timestamp = ts;
            }
            return;
        };

        auto lambdaSourceType = LambdaSourceType::create("input",
                                                         "test_stream" + std::to_string(i),
                                                         std::move(func),
                                                         30,
                                                         0,
                                                         GatheringMode::INTERVAL_MODE);
        wrkConf->physicalSourceTypes.add(lambdaSourceType);
    }

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("MillisecondIntervalTest: Worker1 started successfully");

    auto query = Query::from("input").filter(Attribute("value") > 5).sink(NullOutputSinkDescriptor::create());

    NES::QueryServicePtr queryService = crd->getQueryService();
    auto queryCatalog = crd->getQueryCatalogService();
    auto queryId = queryService->validateAndQueueAddQueryRequest(query.getQueryPlan()->toString(),
                                                                 query.getQueryPlan(),
                                                                 Optimizer::PlacementStrategy::BottomUp);

    bool ret = NES::TestUtils::checkStoppedOrTimeout(queryId, queryCatalog);
    if (!ret) {
        NES_ERROR("query was not stopped within 30 sec");
    }

    NES_DEBUG("E2EBase: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    ASSERT_TRUE(retStopCord);
    NES_DEBUG("E2EBase: Test finished");
}

}// namespace NES