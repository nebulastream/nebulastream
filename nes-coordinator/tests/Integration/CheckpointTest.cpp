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
#include <API/Query.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Network/NetworkSink.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

using namespace std;
namespace NES {

using namespace Configurations;
const int timestamp = 1644426604;
const uint64_t numberOfTupleBuffers = 100;
const uint64_t numberOfBuffersToProduceInTuples = 10;  // 1000000
const uint64_t ingestionRate = 20;

class CheckpointTest : public Testing::BaseIntegrationTest {
  public:
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr workerConfig;
    LambdaSourceTypePtr lambdaSource;
    CSVSourceTypePtr csvSourceTypeFinite;
    SchemaPtr inputSchema;
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("CheckpointTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup CheckpointTest test class.");
    }

    void SetUp() override {
        BaseIntegrationTest::SetUp();

        bufferManager = std::make_shared<Runtime::BufferManager>(1024, 100);

        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->numberOfBuffersPerEpoch = 2;

        workerConfig = WorkerConfiguration::create();
        workerConfig->coordinatorPort = *rpcCoordinatorPort;
        workerConfig->numWorkerThreads = 1;
        workerConfig->numberOfBuffersPerEpoch = 2;

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

        lambdaSource = LambdaSourceType::create("window",
                                                "x1",
                                                std::move(func1),
                                                numberOfBuffersToProduceInTuples,
                                                ingestionRate,
                                                GatheringMode::INGESTION_RATE_MODE);

        csvSourceTypeFinite = CSVSourceType::create("window", "x1");
        csvSourceTypeFinite->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window-out-of-order.csv");
        csvSourceTypeFinite->setNumberOfTuplesToProducePerBuffer(numberOfTupleBuffers - 1);
        csvSourceTypeFinite->setNumberOfBuffersToProduce(numberOfTupleBuffers);

        inputSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());
    }
};

/*
 * @brief test if CheckpointTest backup doesn't fail
 */
TEST_F(CheckpointTest, testCheckpointTestTest) {
    NES_INFO("CheckpointTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalog()->addLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("CheckpointTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("CheckpointTest: Start worker 1");
    workerConfig->physicalSourceTypes.add(lambdaSource);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("CheckpointTest: Worker1 started successfully");

    auto queryCatalog = crd->getQueryCatalog();
    auto requestHandlerService = crd->getRequestHandlerService();

    std::string outputFilePath = getTestResourceFolder() / "testCheckpoint.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("CheckpointTest: Submit query");
    auto query = Query::from("window").sink(NullOutputSinkDescriptor::create());

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(),
                                                                             Optimizer::PlacementStrategy::BottomUp,
                                                                             FaultToleranceType::CH);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));
    std::this_thread::sleep_for(std::chrono::milliseconds(1000000));

    NES_INFO("CheckpointTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("CheckpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("CheckpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("CheckpointTest: Test finished");
}
}// namespace NES
