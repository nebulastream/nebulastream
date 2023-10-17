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
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <iostream>
using namespace std;

namespace NES {

using namespace Configurations;

class YSBDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("YSBDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup YSBDeploymentTest test class.");
    }
};

/**
 * @brief test ysb query deployment
 */
TEST_F(YSBDeploymentTest, testYSBWindow) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("YSBDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto input = Schema::create()
                     ->addField("ysb$user_id", BasicType::UINT64)
                     ->addField("ysb$page_id", BasicType::UINT64)
                     ->addField("ysb$campaign_id", BasicType::UINT64)
                     ->addField("ysb$ad_type", BasicType::UINT64)
                     ->addField("ysb$event_type", BasicType::UINT64)
                     ->addField("ysb$current_ms", BasicType::UINT64)
                     ->addField("ysb$ip", BasicType::UINT64)
                     ->addField("ysb$d1", BasicType::UINT64)
                     ->addField("ysb$d2", BasicType::UINT64)
                     ->addField("ysb$d3", BasicType::UINT32)
                     ->addField("ysb$d4", BasicType::UINT16);
    ASSERT_TRUE(crd->getSourceCatalogService()->registerLogicalSource("ysb", input));
    NES_DEBUG("YSBDeploymentTest: Coordinator started successfully");

    NES_DEBUG("YSBDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->coordinatorPort = port;
    workerConfig->queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto ysbSchema = Schema::create()
                         ->addField("ysb$user_id", BasicType::UINT64)
                         ->addField("ysb$page_id", BasicType::UINT64)
                         ->addField("ysb$campaign_id", BasicType::UINT64)
                         ->addField("ysb$ad_type", BasicType::UINT64)
                         ->addField("ysb$event_type", BasicType::UINT64)
                         ->addField("ysb$current_ms", BasicType::UINT64)
                         ->addField("ysb$ip", BasicType::UINT64)
                         ->addField("ysb$d1", BasicType::UINT64)
                         ->addField("ysb$d2", BasicType::UINT64)
                         ->addField("ysb$d3", BasicType::UINT32)
                         ->addField("ysb$d4", BasicType::UINT16);

    auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct __attribute__((packed)) YsbRecord {
            YsbRecord() = default;
            YsbRecord(uint64_t userId,
                      uint64_t pageId,
                      uint64_t campaignId,
                      uint64_t adType,
                      uint64_t eventType,
                      uint64_t currentMs,
                      uint64_t ip)
                : userId(userId), pageId(pageId), campaignId(campaignId), adType(adType), eventType(eventType),
                  currentMs(currentMs), ip(ip) {}

            uint64_t userId{};
            uint64_t pageId{};
            uint64_t campaignId{};
            uint64_t adType{};
            uint64_t eventType{};
            uint64_t currentMs{};
            uint64_t ip{};

            // placeholder to reach 78 bytes
            uint64_t dummy1{0};
            uint64_t dummy2{0};
            uint32_t dummy3{0};
            uint16_t dummy4{0};

            YsbRecord(const YsbRecord& rhs) {
                userId = rhs.userId;
                pageId = rhs.pageId;
                campaignId = rhs.campaignId;
                adType = rhs.adType;
                eventType = rhs.eventType;
                currentMs = rhs.currentMs;
                ip = rhs.ip;
            }
            [[nodiscard]] std::string toString() const {
                return "YsbRecord(userId=" + std::to_string(userId) + ", pageId=" + std::to_string(pageId)
                    + ", campaignId=" + std::to_string(campaignId) + ", adType=" + std::to_string(adType) + ", eventType="
                    + std::to_string(eventType) + ", currentMs=" + std::to_string(currentMs) + ", ip=" + std::to_string(ip);
            }
        };

        auto* records = buffer.getBuffer<YsbRecord>();
        auto ts =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();

        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            //                    memset(&records, 0, sizeof(YsbRecord));
            records[u].userId = 1;
            records[u].pageId = 0;
            records[u].adType = 0;
            records[u].campaignId = rand() % 10000;
            records[u].eventType = u % 3;
            records[u].currentMs = ts;
            records[u].ip = 0x01020304;
        }
        NES_WARNING("Lambda last entry is={}", records[numberOfTuplesToProduce - 1].toString());
    };

    auto lambdaSourceType = LambdaSourceType::create(func, 10, 100, GatheringMode::INTERVAL_MODE);
    auto physicalSource = PhysicalSource::create("ysb", "YSB_phy", lambdaSourceType);
    workerConfig->physicalSourceTypes.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("YSBDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "ysb.out";

    NES_INFO("YSBDeploymentTest: Submit query");
    auto query = Query::from("ysb")
                     .window(TumblingWindow::of(EventTime(Attribute("current_ms")), Milliseconds(10)))
                     .byKey(Attribute("campaign_id"))
                     .apply(Sum(Attribute("user_id")))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId = queryService->addQueryRequest(query.getQueryPlan()->toString(),
                                                    query.getQueryPlan(),
                                                    Optimizer::PlacementStrategy::BottomUp,
                                                    FaultToleranceType::NONE,
                                                    LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    EXPECT_TRUE(TestUtils::checkIfOutputFileIsNotEmtpy(1, outputFilePath));

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    //here we can only check if the file exists and has some content
    // TODO add result checking
    NES_INFO("YSBDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("YSBDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("YSBDeploymentTest: Test finished");
}
}// namespace NES