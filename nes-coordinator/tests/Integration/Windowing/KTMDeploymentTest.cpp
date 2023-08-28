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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <iostream>
using namespace std;

namespace NES {

using namespace Configurations;

class KTMDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KTMDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup KTMDeploymentTest test class.");
    }
};

/**
 * @brief test tumbling window with multiple aggregations
 */
TEST_F(KTMDeploymentTest, ktmQuery) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;

    //register logical source qnv
    auto ktmSchema = Schema::create()
                         ->addField(createField("Time", BasicType::UINT64))
                         ->addField(createField("Dist", BasicType::UINT64))
                         ->addField(createField("ABS_Front_Wheel_Press", BasicType::FLOAT64))
                         ->addField(createField("ABS_Rear_Wheel_Press", BasicType::FLOAT64))
                         ->addField(createField("ABS_Front_Wheel_Speed", BasicType::FLOAT64))
                         ->addField(createField("ABS_Rear_Wheel_Speed", BasicType::FLOAT64))
                         ->addField(createField("V_GPS", BasicType::FLOAT64))
                         ->addField(createField("MMDD", BasicType::FLOAT64))
                         ->addField(createField("HHMM", BasicType::FLOAT64))
                         ->addField(createField("LAS_Ax1", BasicType::FLOAT64))
                         ->addField(createField("LAS_Ay1", BasicType::FLOAT64))
                         ->addField(createField("LAS_Az_Vertical_Acc", BasicType::FLOAT64))
                         ->addField(createField("ABS_Lean_Angle", BasicType::FLOAT64))
                         ->addField(createField("ABS_Pitch_Info", BasicType::FLOAT64))
                         ->addField(createField("ECU_Gear_Position", BasicType::FLOAT64))
                         ->addField(createField("ECU_Accel_Position", BasicType::FLOAT64))
                         ->addField(createField("ECU_Engine_Rpm", BasicType::FLOAT64))
                         ->addField(createField("ECU_Water_Temperature", BasicType::FLOAT64))
                         ->addField(createField("ECU_Oil_Temp_Sensor_Data", BasicType::UINT64))
                         ->addField(createField("ECU_Side_StanD", BasicType::UINT64))
                         ->addField(createField("Longitude", BasicType::FLOAT64))
                         ->addField(createField("Latitude", BasicType::FLOAT64))
                         ->addField(createField("Altitude", BasicType::FLOAT64));

    NES_INFO("KTMDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("ktm", ktmSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    ASSERT_EQ(port, *rpcCoordinatorPort);
    NES_DEBUG("KTMDeploymentTest: Coordinator started successfully");

    NES_INFO("KTMDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    QueryCompilerConfiguration queryCompilerConfiguration;
    queryCompilerConfiguration.windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;
    workerConfig1->queryCompiler = queryCompilerConfiguration;
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    // create source
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("ktm", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "ktm.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart2 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=3
    ASSERT_TRUE(retStart2);
    NES_INFO("KTMDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = "ktm-results.csv";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("KTMDeploymentTest: Submit query");
    auto query = Query::from("ktm")
                     .window(TumblingWindow::of(EventTime(Attribute("Time")), Seconds(1)))
                     .apply(Avg(Attribute("ABS_Lean_Angle"))->as(Attribute("avg_value_1")),
                            Avg(Attribute("ABS_Pitch_Info"))->as(Attribute("avg_value_2")),
                            Avg(Attribute("ABS_Front_Wheel_Speed"))->as(Attribute("avg_value_3")),
                            Count()->as(Attribute("count_value")))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query.getQueryPlan()->toString(),
                                                    query.getQueryPlan(),
                                                    Optimizer::PlacementStrategy::BottomUp);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    string expectedContent = "ktm$start:INTEGER(64 bits),ktm$end:INTEGER(64 bits),ktm$avg_value_1:Float(64 bits),ktm$avg_value_2:"
                             "Float(64 bits),ktm$avg_value_3:Float(64 bits),ktm$count_value:INTEGER(64 bits)\n"
                             "1543620000000,1543620001000,14.400000,0.800000,0.500000,2\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("KTMDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("KTMDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("KTMDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    remove(outputFilePath.c_str());
    NES_INFO("KTMDeploymentTest: Test finished");
}
}// namespace NES