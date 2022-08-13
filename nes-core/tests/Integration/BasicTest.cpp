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
#include "Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp"
#include "Optimizer/QueryMerger/DefaultQueryMergerRule.hpp"
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/FaultToleranceBasedQueryMergerRule.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryService.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <chrono>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>
#include <gtest/gtest.h>
#include <iostream>
#include <thread>
#include <z3++.h>

using namespace utility;
using namespace std;
using namespace web;
// Common features like URIs.
using namespace web::http;
// Common HTTP functionality
using namespace web::http::client;
// HTTP client features
using namespace concurrency::streams;

namespace NES {

using namespace Configurations;
const int timestamp = 1644426604;
const uint64_t numberOfTupleBuffers = 4;

class BasicTest : public Testing::NESBaseTest {
  public:
    std::string ipAddress = "127.0.0.1";
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr workerConfig;
    CSVSourceTypePtr csvSourceTypeInfinite;
    CSVSourceTypePtr csvSourceTypeFinite;
    SchemaPtr inputSchema;
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("BasicTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup BasicTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();

        bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1);

        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->optimizer.queryMergerRule = Optimizer::QueryMergerRule::FaultToleranceBasedQueryMergerRule;
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;



        workerConfig = WorkerConfiguration::create();
        workerConfig->coordinatorPort = *rpcCoordinatorPort;

        csvSourceTypeInfinite = CSVSourceType::create();
        csvSourceTypeInfinite->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
        csvSourceTypeInfinite->setNumberOfTuplesToProducePerBuffer(numberOfTupleBuffers);
        csvSourceTypeInfinite->setNumberOfBuffersToProduce(0);

        csvSourceTypeFinite = CSVSourceType::create();
        csvSourceTypeFinite->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
        csvSourceTypeFinite->setNumberOfTuplesToProducePerBuffer(numberOfTupleBuffers - 1);
        csvSourceTypeFinite->setNumberOfBuffersToProduce(numberOfTupleBuffers);

        inputSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

        std::shared_ptr<Catalogs::UdfCatalog> udfCatalog;
        SourceCatalogPtr sourceCatalog;
        SchemaPtr schema = Schema::create()
                     ->addField("ts", BasicType::UINT32)
                     ->addField("type", BasicType::UINT32)
                     ->addField("id", BasicType::UINT32)
                     ->addField("value", BasicType::UINT64)
                     ->addField("id1", BasicType::UINT32)
                     ->addField("value1", BasicType::UINT64);
        sourceCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
        sourceCatalog->addLogicalSource("car", schema);
        sourceCatalog->addLogicalSource("bike", schema);
        sourceCatalog->addLogicalSource("truck", schema);





    }
};

/*
 * @brief test timestamp of watermark processor
 */
TEST_F(BasicTest, firstTest) {
    NES_INFO("BasicTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("BasicTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("BasicTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("BasicTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("BasicTest: Submit query");

    SinkDescriptorPtr printSinkDescriptor1 = PrintSinkDescriptor::create();
    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor1);

    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);
    queryPlan1->setFaultToleranceType(NES::FaultToleranceType::AT_MOST_ONCE);

    /*SinkDescriptorPtr printSinkDescriptor2 = PrintSinkDescriptor::create();
    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("value") < 5)
                       .filter(Attribute("id") < 30)
                       .sink(printSinkDescriptor2);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);
    queryPlan2->setFaultToleranceType(NES::FaultToleranceType::AT_LEAST_ONCE);*/



    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::FaultToleranceBasedQueryMergerRule);
    signatureInferencePhase->execute(queryPlan1);
    //signatureInferencePhase->execute(queryPlan2);
    //signatureInferencePhase->execute(queryPlan3);

    globalQueryPlan->addQueryPlan(queryPlan1);
    //globalQueryPlan->addQueryPlan(queryPlan2);
    //globalQueryPlan->addQueryPlan(queryPlan3);

    //execute
    auto faultToleranceBasedQueryMergerRule = Optimizer::DefaultQueryMergerRule::create();
    faultToleranceBasedQueryMergerRule->apply(globalQueryPlan);

    NES_INFO("==================FT RELEVANT DATA=====================");
    NES_INFO("Worker CPU Slots: " + std::to_string(wrk1->getWorkerConfiguration()->numberOfSlots.getValue()));
    NES_INFO("Leafs: " +  std::to_string(queryPlan1->getLeafOperators().size()));
    NES_INFO("Roots: " +  std::to_string(queryPlan1->getRootOperators().size()));
    NES_INFO("Source: " +  std::to_string(queryPlan1->getSourceOperators().size()));
    NES_INFO("Sink: " +  std::to_string(queryPlan1->getSinkOperators().size()));
    NES_INFO(queryPlan1->getLeafOperators()[0]->toString());
    NES_INFO(queryPlan1->getRootOperators()[0]->toString());
    NES_INFO(queryPlan1->getSinkOperators()[0]->toString());

    NES_INFO("Topology toString: " + crd->getTopology()->toString())
    NES_INFO("Root toString: " + crd->getTopology()->getRoot()->toString())



    for (auto& sinkOperator : queryPlan1->getSinkOperators()){
        for (auto& parent : sinkOperator->getChildren()){
            NES_INFO("PARENT: " + parent->toString())

        }

    }




    //NES_INFO(queryPlan2->toString());

    //NES_INFO("IDDD: " + to_string(globalQueryPlan->getSharedQueryId(queryId2)));

    if(globalQueryPlan->getQueryPlansToAdd().empty()){
        NES_WARNING("SharedQueryPlans empty")
    }

    if(globalQueryPlan->getAllSharedQueryPlans().empty()){
        NES_WARNING("SharedQueryPlans empty")
    }

    for(auto& sharedQueryPlan : globalQueryPlan->getAllSharedQueryPlans()){
        NES_INFO("SharedQueryPlan ID: " + std::to_string(sharedQueryPlan->getSharedQueryId()) + " | SharedQueryPlan to String: \n" + sharedQueryPlan->getQueryPlan()->toString());
    }

    NES_INFO("BasicTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("BasicTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("BasicTest: Test finished");
}
/*
 * @brief test message passing between sink-coordinator-sources
 */

/*
 * @brief test if upstream backup doesn't fail
 */
}// namespace NES