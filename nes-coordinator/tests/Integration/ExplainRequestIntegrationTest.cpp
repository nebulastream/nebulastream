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
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;
namespace NES {

using namespace Configurations;

class ExplainRequestIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ExplainRequestIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ExplainRequestIntegrationTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ExplainRequestIntegrationTest class."); }
};

TEST_F(ExplainRequestIntegrationTest, executeExplainRequest) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("Coordinator started successfully");

    //Register the logical source
    auto schema = Schema::create()
                      ->addField("id", BasicType::UINT32)
                      ->addField("timestamp", BasicType::UINT64)
                      ->addField("speed", BasicType::UINT64)
                      ->addField("type", BasicType::TEXT);
    auto sourceCatalog = crd->getSourceCatalogService();
    sourceCatalog->registerLogicalSource("test_source", schema);

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    auto phySource = DefaultSourceType::create("test_source", "phy1");
    wrkConf1->physicalSourceTypes.add(phySource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("worker1 started successfully");

    // Send first explain request
    auto requestHandlerService = crd->getRequestHandlerService();
    auto query1 = Query::from("test_source").project(Attribute("id")).sink(NullOutputSinkDescriptor::create());
    auto response1 =
        requestHandlerService->validateAndQueueExplainQueryRequest(query1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    //Assertion
    NES_INFO("Explain Request Output: {}", response1.dump());
    ASSERT_FALSE(response1.empty());

    // Send Second explain request
    auto query2 = Query::from("test_source").project(Attribute("id")).sink(NullOutputSinkDescriptor::create());
    auto response2 =
        requestHandlerService->validateAndQueueExplainQueryRequest(query2.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    //Assertion
    NES_INFO("Explain Request Output: {}", response2.dump());
    ASSERT_FALSE(response2.empty());

    NES_INFO("stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    NES_INFO("stopping worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);
}
}// namespace NES