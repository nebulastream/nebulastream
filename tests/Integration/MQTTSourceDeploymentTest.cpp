/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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


#include <gtest/gtest.h>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>

using namespace std;

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
    static uint64_t restPort = 8081;
    static uint64_t rpcPort = 4000;

    class MQTTSourceDeploymentTest : public testing::Test {
    public:
        CoordinatorConfigPtr coConf;
        WorkerConfigPtr wrkConf;
        SourceConfigPtr srcConf;

        static void SetUpTestCase() {
            NES::setupLogging("MQTTSinkDeploymentTest.log", NES::LOG_DEBUG);
            NES_INFO("Setup MQTTSinkDeploymentTest test class.");
        }

        void SetUp() override {

            rpcPort = rpcPort + 30;
            restPort = restPort + 2;
            coConf = CoordinatorConfig::create();
            wrkConf = WorkerConfig::create();
            srcConf = SourceConfig::create();
            coConf->setRpcPort(rpcPort);
            coConf->setRestPort(restPort);
            wrkConf->setCoordinatorPort(rpcPort);
        }

        void TearDown() override { NES_INFO("Tear down MQTTSourceDeploymentTest class."); }
    };

/**
 * Test deploying an MQTT sink and sending data via the deployed sink to an MQTT broker
 * DISABLED for now, because it requires a manually set up MQTT broker -> fails otherwise
 */

TEST_F(MQTTSourceDeploymentTest, testDeployOneWorker) {
        coConf->resetCoordinatorOptions();
        wrkConf->resetWorkerOptions();
        srcConf->resetSourceOptions();
        NES_INFO("MQTTSourceDeploymentTest: Start coordinator");
        NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
        uint64_t port = crd->startCoordinator(/**blocking**/ false);
        EXPECT_NE(port, 0UL);
        NES_INFO("MQTTSourceDeploymentTest: Coordinator started successfully");


        std::string mqtt =
                R"(Schema::create()->addField(createField("id", UINT64))->addField(createField("creation_timestamp", UINT64))->addField(createField("data", UINT64))->addField(createField("ingestion_timestamp", UINT64))->addField(createField("expulsion_timestamp", UINT64));)";
        std::string testSchemaFileName = "mqtt.hpp";
        std::ofstream out(testSchemaFileName);
        out << mqtt;
        out.close();


        NES_INFO("MQTTSourceDeploymentTest: Start worker 1");
        wrkConf->setCoordinatorPort(port);
        wrkConf->setRpcPort(port + 10);
        wrkConf->setDataPort(port + 11);
        NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
        bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart1);
        NES_INFO("MQTTSourceDeploymentTest: Worker1 started successfully");
        wrk1->registerLogicalStream("mqtt", testSchemaFileName);
        sleep(3);
        srcConf->setSourceType("MQTTSource");
        srcConf->setSourceConfig("tcp://127.0.0.1:1883;nes;nes;test;JSON;1;false");
        srcConf->setPhysicalStreamName("mqttP");
        srcConf->setLogicalStreamName("mqtt");
        srcConf->setSkipHeader(true);
        srcConf->setSourceFrequency(500);
        //srcConf->setNumberOfTuplesToProducePerBuffer(1);
        //srcConf->setNumberOfBuffersToProduce(10);

        PhysicalStreamConfigPtr mqttConfig = PhysicalStreamConfig::create(srcConf);
        wrk1->registerPhysicalStream(mqttConfig);


        QueryServicePtr queryService = crd->getQueryService();
        QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

        std::string outputFilePath = "MQTTSourceTest.csv";
        remove(outputFilePath.c_str());

        NES_INFO("MQTTSourceDeploymentTest: Submit query");

        // arguments are given so that ThingsBoard accepts the messages sent by the MQTT client
        string query = R"(Query::from("mqtt").map(Attribute("id") = Attribute("id")*100).map(Attribute("id") = Attribute("id")*100).sink(FileSinkDescriptor::create(")" + outputFilePath + R"(" , "CSV_FORMAT", "APPEND"));)";

        QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

        // Comment for better understanding: From here on at some point the DataSource.cpp 'runningRoutine()' function is called
        // this function, because "default_logical" is used, uses 'DefaultSource.cpp', which create a TupleBuffer with 10 id:value
        // pairs, each being 1,1
        GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
        ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
        sleep(30);
        queryService->validateAndQueueStopRequest(queryId);
        EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));
        //QueryId queryIdNew = queryService->validateAndQueueAddRequest(query, "BottomUp");
        //ASSERT_TRUE(TestUtils::waitForQueryToStart(queryIdNew, queryCatalog));

//        sleep(30);
//        queryService->validateAndQueueStopRequest(queryIdNew);
//        EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryIdNew, queryCatalog));
        NES_INFO("MQTTSourceDeploymentTest: Stop worker 1");
        bool retStopWrk1 = wrk1->stop(true);
        EXPECT_TRUE(retStopWrk1);

        NES_INFO("MQTTSourceDeploymentTest: Stop Coordinator");
        bool retStopCord = crd->stopCoordinator(true);
        EXPECT_TRUE(retStopCord);
        NES_INFO("MQTTSourceDeploymentTest: Test finished");
    }
}