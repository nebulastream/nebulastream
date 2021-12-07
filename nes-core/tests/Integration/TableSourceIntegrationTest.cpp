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

#include <API/QueryAPI.hpp>
#include <Catalogs/TableSourceStreamConfig.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <iostream>

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;
uint64_t restPort = 8081;

class TableSourceIntegrationTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("TableSourceIntegrationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup TableSourceIntegrationTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }
};

/// This test checks that a deployed TableSource can write M records spanning exactly N records
TEST_F(TableSourceIntegrationTest, testTableSource) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("TableSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TableSourceIntegrationTest: Coordinator started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    auto streamCatalog = crd->getStreamCatalog();

//    struct Record {
//        uint64_t id;
//        uint64_t table_col_1;
//        uint64_t table_col_2;
//    };
//    static_assert(sizeof(Record) == 24);

    auto schema = Schema::create()
                      ->addField("id", DataTypeFactory::createUInt64())
                      ->addField("table_col_1", DataTypeFactory::createUInt64())
                      ->addField("table_col_2", DataTypeFactory::createUInt64());
//    ASSERT_EQ(schema->getSchemaSizeInBytes(), sizeof(Record));

    streamCatalog->addLogicalStream("table_stream", schema);

    NES_INFO("TableSourceIntegrationTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("TableSourceIntegrationTest: Worker1 started successfully");

    AbstractPhysicalStreamConfigPtr conf = TableSourceStreamConfig::create("TableSource",
                                                                            "table_stream_0",
                                                                            "table_stream",
                                                                            0);  // placeholder
    wrk1->registerPhysicalStream(conf);

    // local fs
    std::string filePath = "tableSourceTestOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("table_stream").sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("TableSourceIntegrationTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //    NES_INFO("TableSourceIntegrationTest: content=" << content);
    EXPECT_TRUE(!content.empty());

    std::ifstream infile(filePath.c_str());
    std::string line;
    std::size_t lineCnt = 0;
    while (std::getline(infile, line)) {
        if (lineCnt > 0) {
            std::string expectedString = std::to_string(lineCnt - 1)
                    + "," + std::to_string(1000 + (lineCnt - 1))
                    + "," + std::to_string(2000 + (lineCnt - 1));
            NES_DEBUG(line);
            ASSERT_EQ(line, expectedString);
        }
        lineCnt++;
    }

//    ASSERT_EQ(recordsToExpect, lineCnt - 1);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES