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
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <iostream>

namespace NES {

auto schema_customer = Schema::create()
        ->addField("C_CUSTKEY", BasicType::UINT64)
        ->addField("C_NAME", DataTypeFactory::createFixedChar(25+1))      // var text
        ->addField("C_ADDRESS", DataTypeFactory::createFixedChar(40+1))   // var text
        ->addField("C_NATIONKEY", BasicType::UINT64)
        ->addField("C_PHONE", DataTypeFactory::createFixedChar(15+1))     // fixed text
        ->addField("C_ACCTBAL", DataTypeFactory::createDouble())                 // decimal
        ->addField("C_MKTSEGMENT", DataTypeFactory::createFixedChar(10+1))            // fixed text
        ->addField("C_COMMENT", DataTypeFactory::createFixedChar(117+1))  // var text
        ;
auto schema_nation = Schema::create()
        ->addField("N_NATIONKEY", BasicType::UINT64)
        ->addField("N_NAME", DataTypeFactory::createFixedChar(25+1))      // var text
        ->addField("N_REGIONKEY", BasicType::UINT64)
        ->addField("N_COMMENT", DataTypeFactory::createFixedChar(152+1))  // var text
        ;
struct __attribute__((packed)) record_customer {
    uint64_t C_CUSTKEY;
    char C_NAME[25+1];
    char C_ADDRESS[40+1];
    uint64_t C_NATIONKEY;
    char C_PHONE[15+1];
    double C_ACCTBAL;
    char C_MKTSEGMENT[10+1];
    char C_COMMENT[117+1];
};
struct __attribute__((packed)) record_nation {
    uint64_t N_NATIONKEY;
    char N_NAME[25+1];
    uint64_t N_REGIONKEY;
    char N_COMMENT[152+1];
};

const std::string table_path_customer_l0200 = "./test_data/tpch_l0200_customer.tbl";
const std::string table_path_nation_s0001 = "./test_data/tpch_s0001_nation.tbl";

class TableSourceIntegrationTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES_INFO("Setup TableSourceIntegrationTest test class.");
        NES::setupLogging("TableSourceIntegrationTest.log", NES::LOG_DEBUG);

        // check validity of schemas and table files to be used
        ASSERT_EQ(sizeof(record_customer), 236UL);
        ASSERT_EQ(schema_customer->getSchemaSizeInBytes(), 236ULL);
        {
            std::ifstream file;
            file.open(table_path_customer_l0200);
            NES_ASSERT(file.is_open(), "Invalid path.");
            int num_lines = std::count(std::istreambuf_iterator<char>(file),
                                       std::istreambuf_iterator<char>(), '\n');
            NES_ASSERT(num_lines==200, "The table file table_path_customer_l0200 does not contain exactly 200 lines.");
        }

        ASSERT_EQ(sizeof(record_nation), 195UL);
        ASSERT_EQ(schema_nation->getSchemaSizeInBytes(), 195ULL);
        {
            std::ifstream file;
            file.open(table_path_nation_s0001);
            NES_ASSERT(file.is_open(), "Invalid path.");
            int num_lines = std::count(std::istreambuf_iterator<char>(file),
                                       std::istreambuf_iterator<char>(), '\n');
            NES_ASSERT(num_lines==25, "The table file table_path_nation_s0001 does not contain exactly 25 lines.");
        }
    }

    void SetUp() override {
        //FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
        // by assigning a different RPC port for each test case
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }
    uint64_t rpcPort = 4000;
    uint64_t restPort = 8081;
};


// This test checks that a deployed TableSource can be initialized and wueried with a simple query
TEST_F(TableSourceIntegrationTest, testCustomerTable) {
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
    streamCatalog->addLogicalStream("tpch_customer", schema_customer);

    NES_INFO("TableSourceIntegrationTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("TableSourceIntegrationTest: Worker1 started successfully");

    AbstractPhysicalStreamConfigPtr conf = TableSourceStreamConfig::create("TableSource",
                                                                            "tpch_l0200_customer",
                                                                            "tpch_customer",
                                                                            table_path_customer_l0200,
                                                                            0);  // <-- placeholder
    wrk1->registerPhysicalStream(conf);

    // local fs
    std::string filePath = "testCustomerTableOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
            R"(Query::from("tpch_customer").filter(Attribute("C_CUSTKEY") < 10).sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("TableSourceIntegrationTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected =
            "tpch_customer$C_CUSTKEY:INTEGER,tpch_customer$C_NAME:ArrayType,tpch_customer$C_ADDRESS:ArrayType,tpch_customer$C_NATIONKEY:INTEGER,tpch_customer$C_PHONE:ArrayType,tpch_customer$C_ACCTBAL:(Float),tpch_customer$C_MKTSEGMENT:ArrayType,tpch_customer$C_COMMENT:ArrayType\n"
            "1,Customer#000000001,IVhzIApeRb ot,c,E,15,25-989-741-2988,711.560000,BUILDING,to the even, regular platelets. regular, ironic epitaphs nag e\n"
            "2,Customer#000000002,XSTf4,NCwDVaWNe6tEgvwfmRchLXak,13,23-768-687-3665,121.650000,AUTOMOBILE,l accounts. blithely ironic theodolites integrate boldly: caref\n"
            "3,Customer#000000003,MG9kdTD2WBHm,1,11-719-748-3364,7498.120000,AUTOMOBILE, deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov\n"
            "4,Customer#000000004,XxVSJsLAGtn,4,14-128-190-5944,2866.830000,MACHINERY, requests. final, regular ideas sleep final accou\n"
            "5,Customer#000000005,KvpyuHCplrB84WgAiGV6sYpZq7Tj,3,13-750-942-6364,794.470000,HOUSEHOLD,n accounts will have to unwind. foxes cajole accor\n"
            "6,Customer#000000006,sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn,20,30-114-968-4951,7638.570000,AUTOMOBILE,tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious\n"
            "7,Customer#000000007,TcGe5gaZNgVePxU5kRrvXBfkasDTea,18,28-190-982-9759,9561.950000,AUTOMOBILE,ainst the ironic, express theodolites. express, even pinto beans among the exp\n"
            "8,Customer#000000008,I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5,17,27-147-574-9335,6819.740000,BUILDING,among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide\n"
            "9,Customer#000000009,xKiAFTjUsCuxfeleNqefumTrjS,8,18-338-906-3675,8324.070000,FURNITURE,r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl\n";
    EXPECT_EQ(content, expected);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// simple test for nation table
TEST_F(TableSourceIntegrationTest, testNationTable) {
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
    streamCatalog->addLogicalStream("tpch_nation", schema_nation);

    NES_INFO("TableSourceIntegrationTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("TableSourceIntegrationTest: Worker1 started successfully");

    AbstractPhysicalStreamConfigPtr conf = TableSourceStreamConfig::create("TableSource",
                                                                            "tpch_s0001_nation",
                                                                            "tpch_nation",
                                                                            table_path_nation_s0001,
                                                                            0);  // <-- placeholder
    wrk1->registerPhysicalStream(conf);

    // local fs
    std::string filePath = "testNationTableOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
            R"(Query::from("tpch_nation").filter(Attribute("N_NATIONKEY") > 20).sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("TableSourceIntegrationTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected =
            "tpch_nation$N_NATIONKEY:INTEGER,tpch_nation$N_NAME:ArrayType,tpch_nation$N_REGIONKEY:INTEGER,tpch_nation$N_COMMENT:ArrayType\n"
            "21,VIETNAM,2,hely enticingly express accounts. even, final \n"
            "22,RUSSIA,3, requests against the platelets use never according to the quickly regular pint\n"
            "23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. carefull\n"
            "24,UNITED STATES,1,y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be\n";
    EXPECT_EQ(content, expected);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// simple test for nation table
TEST_F(TableSourceIntegrationTest, testTwoTableJoin) {
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
    streamCatalog->addLogicalStream("tpch_customer", schema_customer);
    streamCatalog->addLogicalStream("tpch_nation", schema_nation);

    NES_INFO("TableSourceIntegrationTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("TableSourceIntegrationTest: Worker1 started successfully");

    AbstractPhysicalStreamConfigPtr confCustomer = TableSourceStreamConfig::create("TableSource",
                                                                           "tpch_l0200_customer",
                                                                           "tpch_customer",
                                                                           table_path_customer_l0200,
                                                                           0);  // <-- placeholder
    wrk1->registerPhysicalStream(confCustomer);
    AbstractPhysicalStreamConfigPtr confNation = TableSourceStreamConfig::create("TableSource",
                                                                            "tpch_s0001_nation",
                                                                            "tpch_nation",
                                                                            table_path_nation_s0001,
                                                                            0);  // <-- placeholder
    wrk1->registerPhysicalStream(confNation);

    // local fs
    std::string filePath = "testTwoTableJoinOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
            R"(Query::from("tpch_nation").filter(Attribute("N_NATIONKEY") == 21).map(Attribute("timeForWindow") = 1)
            .joinWith(Query::from("tpch_customer").map(Attribute("timeForWindow") = 1))
                .where(Attribute("NATIONKEY")).equalsTo(Attribute("N_NATIONKEY"))
                .window(TumblingWindow::of(EventTime(Attribute("timeForWindow")), Milliseconds(1)))
            .sink(FileSinkDescriptor::create(")" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect));

    NES_INFO("TableSourceIntegrationTest: Remove query");
    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected =
            "tpch_nation$C_NATIONKEY:INTEGER,tpch_nation$C_NAME:ArrayType,tpch_nation$C_REGIONKEY:INTEGER,tpch_nation$C_COMMENT:ArrayType\n"
            "21,VIETNAM,2,hely enticingly express accounts. even, final \n"
            "22,RUSSIA,3, requests against the platelets use never according to the quickly regular pint\n"
            "23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. carefull\n"
            "24,UNITED STATES,1,y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be\n";
    EXPECT_EQ(content, expected);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES