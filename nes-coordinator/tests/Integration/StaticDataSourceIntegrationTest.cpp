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
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/StaticDataSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>
#include <iostream>
namespace NES::Experimental {

class StaticDataSourceIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    struct __attribute__((packed)) record_customer {
        uint64_t C_CUSTKEY;
        char C_NAME[25 + 1];
        char C_ADDRESS[40 + 1];
        uint64_t C_NATIONKEY;
        char C_PHONE[15 + 1];
        double C_ACCTBAL;
        char C_MKTSEGMENT[10 + 1];
        char C_COMMENT[117 + 1];
    };
    struct __attribute__((packed)) record_nation {
        uint64_t N_NATIONKEY;
        char N_NAME[25 + 1];
        uint64_t N_REGIONKEY;
        char N_COMMENT[152 + 1];
    };
    struct __attribute__((packed)) record_integers_0 {
        uint64_t id;
        uint64_t value;
    };
    struct __attribute__((packed)) record_integers_1 {
        uint64_t id;
        uint64_t one;
        uint64_t value;
    };

    const std::string table_path_customer_l0200 = "./test_data/tpch_l0200_customer.tbl";
    const std::string table_path_customer_s0001 = "./test_data/tpch_s0001_customer.tbl";
    const std::string table_path_nation_s0001 = "./test_data/tpch_s0001_nation.tbl";
    const std::string table_path_integers_0 = "./test_data/static_data_0.tbl";
    const std::string table_path_integers_0a = "./test_data/static_data_0a.tbl";
    const std::string table_path_integers_0b = "./test_data/static_data_0b.tbl";
    const std::string table_path_integers_1 = "./test_data/static_data_1.tbl";
    const std::string table_path_integers_1a = "./test_data/static_data_1a.tbl";
    const std::string table_path_integers_1b = "./test_data/static_data_1b.tbl";
    const std::string table_path_integers_2 = "./test_data/static_data_2.tbl";

    static void SetUpTestCase() {
        NES::Logger::setupLogging("StaticDataSourceIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StaticDataSourceIntegrationTest test class.");
    }
    SchemaPtr schema_customer;
    SchemaPtr schema_nation;
    SchemaPtr schema_integers_0;
    SchemaPtr schema_integers_1;

    void SetUp() {
        Testing::BaseIntegrationTest::SetUp();
        schema_customer = Schema::create()
                              ->addField("C_CUSTKEY", BasicType::UINT64)
                              ->addField("C_NAME", DataTypeFactory::createFixedChar(25 + 1))   // var text
                              ->addField("C_ADDRESS", DataTypeFactory::createFixedChar(40 + 1))// var text
                              ->addField("C_NATIONKEY", BasicType::UINT64)
                              ->addField("C_PHONE", DataTypeFactory::createFixedChar(15 + 1))     // fixed text
                              ->addField("C_ACCTBAL", DataTypeFactory::createDouble())            // decimal
                              ->addField("C_MKTSEGMENT", DataTypeFactory::createFixedChar(10 + 1))// fixed text
                              ->addField("C_COMMENT", DataTypeFactory::createFixedChar(117 + 1))  // var text
            ;
        schema_nation = Schema::create()
                            ->addField("N_NATIONKEY", BasicType::UINT64)
                            ->addField("N_NAME", DataTypeFactory::createFixedChar(25 + 1))// var text
                            ->addField("N_REGIONKEY", BasicType::UINT64)
                            ->addField("N_COMMENT", DataTypeFactory::createFixedChar(152 + 1));// var text
        schema_integers_0 = Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64);
        schema_integers_1 = Schema::create()
                                ->addField("id", BasicType::UINT64)
                                ->addField("one", BasicType::UINT64)
                                ->addField("value", BasicType::UINT64);

        ASSERT_EQ(sizeof(record_customer), 236UL);
        ASSERT_EQ(schema_customer->getSchemaSizeInBytes(), 236ULL);
        {
            std::ifstream file;
            file.open(table_path_customer_l0200);
            NES_ASSERT(file.is_open(), "Invalid path.");
            int num_lines = std::count(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>(), '\n');
            NES_ASSERT(num_lines == 200, "The table file table_path_customer_l0200 does not contain exactly 200 lines.");
        }

        ASSERT_EQ(sizeof(record_nation), 195UL);
        ASSERT_EQ(schema_nation->getSchemaSizeInBytes(), 195ULL);
        {
            std::ifstream file;
            file.open(table_path_nation_s0001);
            NES_ASSERT(file.is_open(), "Invalid path.");
            int num_lines = std::count(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>(), '\n');
            NES_ASSERT(num_lines == 25, "The table file table_path_nation_s0001 does not contain exactly 25 lines.");
        }

        ASSERT_EQ(sizeof(record_integers_0), 16UL);
        ASSERT_EQ(schema_integers_0->getSchemaSizeInBytes(), 16ULL);
        {
            std::ifstream file;
            file.open(table_path_integers_0);
            NES_ASSERT(file.is_open(), "Invalid path.");
            int num_lines = std::count(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>(), '\n');
            NES_ASSERT(num_lines == 10, "The table file table_path_integers_0 does not contain exactly 10 lines.");
        }

        ASSERT_EQ(sizeof(record_integers_1), 24UL);
        ASSERT_EQ(schema_integers_1->getSchemaSizeInBytes(), 24ULL);
        {
            std::ifstream file;
            file.open(table_path_integers_1);
            NES_ASSERT(file.is_open(), "Invalid path.");
            int num_lines = std::count(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>(), '\n');
            NES_ASSERT(num_lines == 10, "The table file table_path_integers_1 does not contain exactly 10 lines.");
        }
    }

    uint64_t printTotalQueryRuntime(std::vector<Runtime::QueryStatisticsPtr> stats) {
        EXPECT_EQ(stats.size(), 1);
        uint64_t queryStart = stats[0]->getTimestampQueryStart();
        uint64_t firstTask = stats[0]->getTimestampFirstProcessedTask();
        uint64_t lastTask = stats[0]->getTimestampLastProcessedTask();
        EXPECT_NE(lastTask, 0);
        EXPECT_NE(firstTask, 0);
        EXPECT_NE(queryStart, 0);

        uint64_t diffToStart = lastTask - queryStart;
        uint64_t diffToFirstTask = lastTask - firstTask;
        NES_INFO("Total query runtime since query start: {}ms.", diffToStart);
        NES_INFO("Total query runtime since first Task: {}ms.", diffToFirstTask);
        return diffToFirstTask;
    }
};

// This test checks that a deployed StaticDataSource can be initialized and queries with a simple query
// Worker and coordinator on different nodes.
TEST_F(StaticDataSourceIntegrationTest, testCustomerTableDistributed) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();
    sourceCatalog->addLogicalSource("tpch_customer", schema_customer);

    NES_INFO("StaticDataSourceIntegrationTest: Start worker 1");
    wrkConf->coordinatorPort = port;

    PhysicalSourceTypePtr sourceType =
        StaticDataSourceType::create(table_path_customer_l0200, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ false);
    auto physicalSource = PhysicalSource::create("tpch_customer", "tpch_l0200_customer", sourceType);
    wrkConf->physicalSources.add(physicalSource);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("StaticDataSourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = getTestResourceFolder() / "testCustomerTableOut.csv";
    remove(filePath.c_str());

    bool benchmark = true;

    //register query
    std::string queryLogic = R"(Query::from("tpch_customer").filter(Attribute("C_CUSTKEY") < 10))";
    std::string querySink = benchmark ? ".sink(NullOutputSinkDescriptor::create());"
                                      : ".sink(FileSinkDescriptor::create(\"" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    std::string queryString = queryLogic + querySink;

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToASSERT = 1;
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToASSERT, true));

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    // extract total query runtime from statistics
    auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
    printTotalQueryRuntime(stats);

    if (!benchmark) {
        std::ifstream ifs(filePath.c_str());
        ASSERT_TRUE(ifs.good());
        std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

        const std::string expected =
            "tpch_customer$C_CUSTKEY:INTEGER,tpch_customer$C_NAME:ArrayType,tpch_customer$C_ADDRESS:ArrayType,tpch_customer$C_"
            "NATIONKEY:INTEGER,tpch_customer$C_PHONE:ArrayType,tpch_customer$C_ACCTBAL:(Float),tpch_customer$C_MKTSEGMENT:"
            "ArrayType,"
            "tpch_customer$C_COMMENT:ArrayType\n"
            "1,Customer#000000001,IVhzIApeRb ot,c,E,15,25-989-741-2988,711.560000,BUILDING,to the even, regular platelets. "
            "regular, "
            "ironic epitaphs nag e\n"
            "2,Customer#000000002,XSTf4,NCwDVaWNe6tEgvwfmRchLXak,13,23-768-687-3665,121.650000,AUTOMOBILE,l accounts. blithely "
            "ironic theodolites integrate boldly: caref\n"
            "3,Customer#000000003,MG9kdTD2WBHm,1,11-719-748-3364,7498.120000,AUTOMOBILE, deposits eat slyly ironic, even "
            "instructions. express foxes detect slyly. blithely even accounts abov\n"
            "4,Customer#000000004,XxVSJsLAGtn,4,14-128-190-5944,2866.830000,MACHINERY, requests. final, regular ideas sleep "
            "final "
            "accou\n"
            "5,Customer#000000005,KvpyuHCplrB84WgAiGV6sYpZq7Tj,3,13-750-942-6364,794.470000,HOUSEHOLD,n accounts will have to "
            "unwind. foxes cajole accor\n"
            "6,Customer#000000006,sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn,20,30-114-968-4951,7638.570000,AUTOMOBILE,tions. even "
            "deposits boost according to the slyly bold packages. final accounts cajole requests. furious\n"
            "7,Customer#000000007,TcGe5gaZNgVePxU5kRrvXBfkasDTea,18,28-190-982-9759,9561.950000,AUTOMOBILE,ainst the ironic, "
            "express "
            "theodolites. express, even pinto beans among the exp\n"
            "8,Customer#000000008,I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5,17,27-147-574-9335,6819.740000,BUILDING,among the slyly "
            "regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide\n"
            "9,Customer#000000009,xKiAFTjUsCuxfeleNqefumTrjS,8,18-338-906-3675,8324.070000,FURNITURE,r theodolites according to "
            "the "
            "requests wake thinly excuses: pending requests haggle furiousl\n";
        ASSERT_EQ(content, expected);
    }

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

// This test checks that a deployed StaticDataSource can be initialized and wueried with a simple query.
// Worker on ame node as coordinator. Otherwise equivalent to above.
TEST_F(StaticDataSourceIntegrationTest, testCustomerTableNotDistributed) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;

    crdConf->logicalSources.add(LogicalSource::create("tpch_customer", schema_customer));
    PhysicalSourceTypePtr sourceType =
        StaticDataSourceType::create(table_path_customer_l0200, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ false);
    auto physicalSource = PhysicalSource::create("tpch_customer", "tpch_l0200_customer", sourceType);
    crdConf->worker.physicalSources.add(physicalSource);

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    // local fs
    std::string filePath = getTestResourceFolder() / "testCustomerTableOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("tpch_customer").filter(Attribute("C_CUSTKEY") < 10).sink(FileSinkDescriptor::create(")" + filePath
        + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    // extract total query runtime from statistics
    auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
    printTotalQueryRuntime(stats);

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected =
        "tpch_customer$C_CUSTKEY:INTEGER,tpch_customer$C_NAME:ArrayType,tpch_customer$C_ADDRESS:ArrayType,tpch_customer$C_"
        "NATIONKEY:INTEGER,tpch_customer$C_PHONE:ArrayType,tpch_customer$C_ACCTBAL:(Float),tpch_customer$C_MKTSEGMENT:ArrayType,"
        "tpch_customer$C_COMMENT:ArrayType\n"
        "1,Customer#000000001,IVhzIApeRb ot,c,E,15,25-989-741-2988,711.560000,BUILDING,to the even, regular platelets. regular, "
        "ironic epitaphs nag e\n"
        "2,Customer#000000002,XSTf4,NCwDVaWNe6tEgvwfmRchLXak,13,23-768-687-3665,121.650000,AUTOMOBILE,l accounts. blithely "
        "ironic theodolites integrate boldly: caref\n"
        "3,Customer#000000003,MG9kdTD2WBHm,1,11-719-748-3364,7498.120000,AUTOMOBILE, deposits eat slyly ironic, even "
        "instructions. express foxes detect slyly. blithely even accounts abov\n"
        "4,Customer#000000004,XxVSJsLAGtn,4,14-128-190-5944,2866.830000,MACHINERY, requests. final, regular ideas sleep final "
        "accou\n"
        "5,Customer#000000005,KvpyuHCplrB84WgAiGV6sYpZq7Tj,3,13-750-942-6364,794.470000,HOUSEHOLD,n accounts will have to "
        "unwind. foxes cajole accor\n"
        "6,Customer#000000006,sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn,20,30-114-968-4951,7638.570000,AUTOMOBILE,tions. even "
        "deposits boost according to the slyly bold packages. final accounts cajole requests. furious\n"
        "7,Customer#000000007,TcGe5gaZNgVePxU5kRrvXBfkasDTea,18,28-190-982-9759,9561.950000,AUTOMOBILE,ainst the ironic, express "
        "theodolites. express, even pinto beans among the exp\n"
        "8,Customer#000000008,I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5,17,27-147-574-9335,6819.740000,BUILDING,among the slyly "
        "regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide\n"
        "9,Customer#000000009,xKiAFTjUsCuxfeleNqefumTrjS,8,18-338-906-3675,8324.070000,FURNITURE,r theodolites according to the "
        "requests wake thinly excuses: pending requests haggle furiousl\n";
    EXPECT_EQ(content, expected);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StaticDataSourceIntegrationTest, DISABLED_testCustomerTableProjection) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->numberOfBuffersInGlobalBufferManager = 60;

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();
    sourceCatalog->addLogicalSource("tpch_customer", schema_customer);

    NES_INFO("StaticDataSourceIntegrationTest: Start worker 1");
    wrkConf->coordinatorPort = port;

    PhysicalSourceTypePtr sourceType =
        StaticDataSourceType::create(table_path_customer_s0001, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ false);
    auto physicalSource = PhysicalSource::create("tpch_customer", "tpch_s0001_customer", sourceType);
    wrkConf->physicalSources.add(physicalSource);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("StaticDataSourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = getTestResourceFolder() / "testCustomerTableProjectionOut.csv";
    remove(filePath.c_str());

    bool benchmark = false;

    //register query
    std::string queryLogic =
        R"(Query::from("tpch_customer").project(Attribute("C_CUSTKEY"), Attribute("C_NATIONKEY"), Attribute("C_ACCTBAL")))";
    std::string querySink = benchmark ? ".sink(NullOutputSinkDescriptor::create());"
                                      : ".sink(FileSinkDescriptor::create(\"" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    std::string queryString = queryLogic + querySink;

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    const auto timeoutSeconds = std::chrono::seconds(120);
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true, timeoutSeconds));

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::ifstream ifsInput(table_path_customer_s0001.c_str());
    EXPECT_TRUE(ifsInput.good());

    int numResultTuples = std::count(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>(), '\n');
    int numExpectedTuples = std::count(std::istreambuf_iterator<char>(ifsInput), std::istreambuf_iterator<char>(), '\n')
        + 1;// the .csv file contains a header, so we expect one more

    NES_ASSERT(numResultTuples == numExpectedTuples,
               "The Projection query changed the number of tuples from " << numExpectedTuples << " to " << numResultTuples);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// simple test for nation table (distributed)
TEST_F(StaticDataSourceIntegrationTest, testNationTable) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();
    sourceCatalog->addLogicalSource("tpch_nation", schema_nation);

    NES_INFO("StaticDataSourceIntegrationTest: Start worker 1");
    wrkConf->coordinatorPort = port;

    PhysicalSourceTypePtr sourceType =
        StaticDataSourceType::create(table_path_nation_s0001, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ false);
    auto physicalSource = PhysicalSource::create("tpch_nation", "tpch_s0001_nation", sourceType);
    wrkConf->physicalSources.add(physicalSource);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("StaticDataSourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = getTestResourceFolder() / "testNationTableOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("tpch_nation").filter(Attribute("N_NATIONKEY") > 20).sink(FileSinkDescriptor::create(")" + filePath
        + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToASSERT = 1;
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToASSERT, true));

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected = "tpch_nation$N_NATIONKEY:INTEGER,tpch_nation$N_NAME:ArrayType,tpch_nation$N_REGIONKEY:INTEGER,"
                                 "tpch_nation$N_COMMENT:ArrayType\n"
                                 "21,VIETNAM,2,hely enticingly express accounts. even, final \n"
                                 "22,RUSSIA,3, requests against the platelets use never according to the quickly regular pint\n"
                                 "23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. carefull\n"
                                 "24,UNITED STATES,1,y final packages. slow foxes cajole quickly. quickly silent platelets "
                                 "breach ironic accounts. unusual pinto be\n";
    ASSERT_EQ(content, expected);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

// simple test for table with artificial data - integers only
TEST_F(StaticDataSourceIntegrationTest, testTableIntegersOnlyDistributed) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();
    sourceCatalog->addLogicalSource("static_integers_only_0", schema_integers_0);

    NES_INFO("StaticDataSourceIntegrationTest: Start worker 1");
    wrkConf->coordinatorPort = port;

    PhysicalSourceTypePtr sourceType =
        StaticDataSourceType::create(table_path_integers_0, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ false);
    auto physicalSource = PhysicalSource::create("static_integers_only_0", "static_integers_only_0", sourceType);
    wrkConf->physicalSources.add(physicalSource);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("StaticDataSourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = getTestResourceFolder() / "testIntegerTable0Out.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("static_integers_only_0").filter(Attribute("id") < 6).sink(FileSinkDescriptor::create(")" + filePath
        + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected = "static_integers_only_0$id:INTEGER,static_integers_only_0$value:INTEGER\n"
                                 "1,10\n"
                                 "2,20\n"
                                 "3,30\n"
                                 "4,40\n"
                                 "5,50\n";
    EXPECT_EQ(content, expected);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// incomplete
// this test is supposed to join two table sources relying on the streaming join operator
TEST_F(StaticDataSourceIntegrationTest, DISABLED_testTwoTableStreamingJoin) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();
    sourceCatalog->addLogicalSource("tpch_customer", schema_customer);
    sourceCatalog->addLogicalSource("tpch_nation", schema_nation);

    NES_INFO("StaticDataSourceIntegrationTest: Start worker 1");
    wrkConf->coordinatorPort = port;

    PhysicalSourceTypePtr sourceType0 =
        StaticDataSourceType::create(table_path_nation_s0001, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ false);
    auto physicalSource0 = PhysicalSource::create("tpch_nation", "tpch_s0001_nation", sourceType0);
    wrkConf->physicalSources.add(physicalSource0);

    PhysicalSourceTypePtr sourceType1 =
        StaticDataSourceType::create(table_path_customer_l0200, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ false);
    auto physicalSource1 = PhysicalSource::create("tpch_customer", "tpch_l0200_customer", sourceType1);
    wrkConf->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("StaticDataSourceIntegrationTest: Worker1 started successfully");

    // local fs
    std::string filePath = getTestResourceFolder() / "testTwoTableJoinOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("tpch_nation").filter(Attribute("N_NATIONKEY") == 21).map(Attribute("timeForWindow") = 1)
            .joinWith(Query::from("tpch_customer").map(Attribute("timeForWindow") = 1))
                .where(Attribute("NATIONKEY") == Attribute("N_NATIONKEY"))
                .window(TumblingWindow::of(EventTime(Attribute("timeForWindow")), Milliseconds(1)))
            .sink(FileSinkDescriptor::create(")"
        + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToASSERT = 1;
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToASSERT, true));

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected = "tpch_nation$C_NATIONKEY:INTEGER,tpch_nation$C_NAME:ArrayType,tpch_nation$C_REGIONKEY:INTEGER,"
                                 "tpch_nation$C_COMMENT:ArrayType\n"
                                 "21,VIETNAM,2,hely enticingly express accounts. even, final \n"
                                 "22,RUSSIA,3, requests against the platelets use never according to the quickly regular pint\n"
                                 "23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. carefull\n"
                                 "24,UNITED STATES,1,y final packages. slow foxes cajole quickly. quickly silent platelets "
                                 "breach ironic accounts. unusual pinto be\n";
    ASSERT_EQ(content, expected);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// join two static data sources together with the batch join operator
TEST_F(StaticDataSourceIntegrationTest, testBatchJoinNationCustomer200lines) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;

    // use deprecated feature "logicalSources" to register logical streams before physical streams
    crdConf->logicalSources.add(LogicalSource::create("tpch_customer", schema_customer));
    crdConf->logicalSources.add(LogicalSource::create("tpch_nation", schema_nation));

    PhysicalSourceTypePtr sourceType0 =
        StaticDataSourceType::create(table_path_nation_s0001, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource0 = PhysicalSource::create("tpch_nation", "tpch_s0001_nation", sourceType0);
    crdConf->worker.physicalSources.add(physicalSource0);

    PhysicalSourceTypePtr sourceType1 =
        StaticDataSourceType::create(table_path_customer_l0200, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource1 = PhysicalSource::create("tpch_customer", "tpch_l0200_customer", sourceType1);
    crdConf->worker.physicalSources.add(physicalSource1);

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    // local fs
    std::string filePath = getTestResourceFolder() / "testTwoTableJoinOut.csv";
    remove(filePath.c_str());

    // todo adding this as line 3 of query breaks it because the join operator needs to be immmediatly after its sources:
    // .project(Attribute("tpch_nation$N_NATIONKEY"), Attribute("tpch_nation$N_REGIONKEY"))

    //register query
    // we perform two project operations which have no effect. (just to check, if the batch join works with them)
    std::string queryString =
        R"(Query::from("tpch_nation")
            .project(Attribute("tpch_nation$N_NATIONKEY"), Attribute("tpch_nation$N_NAME"), Attribute("tpch_nation$N_REGIONKEY"), Attribute("tpch_nation$N_COMMENT"))
            .batchJoinWith(Query::from("tpch_customer").project(Attribute("tpch_customer$C_CUSTKEY"), Attribute("tpch_customer$C_NAME"), Attribute("tpch_customer$C_ADDRESS"), Attribute("tpch_customer$C_NATIONKEY"), Attribute("tpch_customer$C_PHONE"), Attribute("tpch_customer$C_ACCTBAL"), Attribute("tpch_customer$C_MKTSEGMENT"), Attribute("tpch_customer$C_COMMENT")))
                .where(Attribute("C_NATIONKEY"))
                .equalsTo(Attribute("N_NATIONKEY"))
            .sink(FileSinkDescriptor::create(")"
        + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

    // extract total query runtime from statistics
    auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
    printTotalQueryRuntime(stats);

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expectedContent =
        "tpch_nation$N_NATIONKEY:INTEGER,tpch_nation$N_NAME:ArrayType,tpch_nation$N_REGIONKEY:INTEGER,tpch_nation$N_COMMENT:"
        "ArrayType,tpch_customer$C_CUSTKEY:INTEGER,tpch_customer$C_NAME:ArrayType,tpch_customer$C_ADDRESS:ArrayType,tpch_"
        "customer$C_NATIONKEY:INTEGER,tpch_customer$C_PHONE:ArrayType,tpch_customer$C_ACCTBAL:(Float),tpch_customer$C_MKTSEGMENT:"
        "ArrayType,tpch_customer$C_COMMENT:ArrayType\n15,MOROCCO,0,rns. blithely bold courts among the closely regular packages "
        "use furiously bold platelets?,1,Customer#000000001,IVhzIApeRb ot,c,E,15,25-989-741-2988,711.560000,BUILDING,to the "
        "even, regular platelets. regular, ironic epitaphs nag e\n13,JORDAN,4,ic deposits are blithely about the carefully "
        "regular pa,2,Customer#000000002,XSTf4,NCwDVaWNe6tEgvwfmRchLXak,13,23-768-687-3665,121.650000,AUTOMOBILE,l accounts. "
        "blithely ironic theodolites integrate boldly: caref\n1,ARGENTINA,1,al foxes promise slyly according to the regular "
        "accounts. bold requests alon,3,Customer#000000003,MG9kdTD2WBHm,1,11-719-748-3364,7498.120000,AUTOMOBILE, deposits eat "
        "slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov\n4,EGYPT,4,y above the "
        "carefully unusual theodolites. final dugouts are quickly across the furiously regular "
        "d,4,Customer#000000004,XxVSJsLAGtn,4,14-128-190-5944,2866.830000,MACHINERY, requests. final, regular ideas sleep final "
        "accou\n3,CANADA,1,eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily "
        "bold,5,Customer#000000005,KvpyuHCplrB84WgAiGV6sYpZq7Tj,3,13-750-942-6364,794.470000,HOUSEHOLD,n accounts will have to "
        "unwind. foxes cajole accor\n20,SAUDI ARABIA,4,ts. silent requests haggle. closely express packages sleep across the "
        "blithely,6,Customer#000000006,sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn,20,30-114-968-4951,7638.570000,AUTOMOBILE,tions. "
        "even deposits boost according to the slyly bold packages. final accounts cajole requests. furious\n18,CHINA,2,c "
        "dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. "
        "depos,7,Customer#000000007,TcGe5gaZNgVePxU5kRrvXBfkasDTea,18,28-190-982-9759,9561.950000,AUTOMOBILE,ainst the ironic, "
        "express theodolites. express, even pinto beans among the exp\n17,PERU,1,platelets. blithely pending dependencies use "
        "fluffily across the even pinto beans. carefully silent accoun,8,Customer#000000008,I0B10bB0AymmC, "
        "0PrRYBCP1yGJ8xcBPmWhl5,17,27-147-574-9335,6819.740000,BUILDING,among the slyly regular theodolites kindle blithely "
        "courts. carefully even theodolites haggle slyly along the ide\n8,INDIA,2,ss excuses cajole slyly across the packages. "
        "deposits print aroun,9,Customer#000000009,xKiAFTjUsCuxfeleNqefumTrjS,8,18-338-906-3675,8324.070000,FURNITURE,r "
        "theodolites according to the requests wake thinly excuses: pending requests haggle furiousl\n5,ETHIOPIA,0,ven packages "
        "wake quickly. regu,10,Customer#000000010,6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2,5,15-741-346-9870,2753.540000,HOUSEHOLD,es "
        "regular deposits haggle. fur\n23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. "
        "carefull,11,Customer#000000011,PkWS 3HlXqwTuzrKg633BEi,23,33-464-151-3439,-272.600000,BUILDING,ckages. requests sleep "
        "slyly. quickly even pinto beans promise above the slyly regular pinto beans. \n13,JORDAN,4,ic deposits are blithely "
        "about the carefully regular pa,12,Customer#000000012,9PWKuhzT4Zr1Q,13,23-791-276-1263,3396.490000,HOUSEHOLD, to the "
        "carefully final braids. blithely regular requests nag. ironic theodolites boost quickly along\n3,CANADA,1,eas hang "
        "ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily "
        "bold,13,Customer#000000013,nsXQu0oVjD7PM659uC3SRSp,3,13-761-547-5974,3857.340000,BUILDING,ounts sleep carefully after "
        "the close frays. carefully bold notornis use ironic requests. blithely\n1,ARGENTINA,1,al foxes promise slyly according "
        "to the regular accounts. bold requests alon,14,Customer#000000014,KXkletMlL2JQEA "
        ",1,11-845-129-3851,5266.300000,FURNITURE,, ironic packages across the unus\n23,UNITED KINGDOM,3,eans boost carefully "
        "special requests. accounts are. "
        "carefull,15,Customer#000000015,YtWggXoOLdwdo7b0y,BZaGUQMLJMX1Y,EC,6Dn,23,33-687-542-7601,2788.520000,HOUSEHOLD, "
        "platelets. regular deposits detect asymptotes. blithely unusual packages nag slyly at the fluf\n10,IRAN,4,efully "
        "alongside of the slyly final dependencies. ,16,Customer#000000016,cYiaeMLZSMAOQ2 "
        "d0W,,10,20-781-609-3107,4681.030000,FURNITURE,kly silent courts. thinly regular theodolites sleep fluffily after "
        "\n2,BRAZIL,1,y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special "
        ",17,Customer#000000017,izrh 6jdqtp2eqdtbkswDD8SG4SzXruMfIXyR7,2,12-970-682-3487,6.340000,AUTOMOBILE,packages wake! "
        "blithely even pint\n6,FRANCE,3,refully final requests. regular, ironi,18,Customer#000000018,3txGO "
        "AiuFux3zT0Z9NYaFRnZt,6,16-155-215-1315,5494.430000,BUILDING,s sleep. carefully even instructions nag furiously "
        "alongside of t\n18,CHINA,2,c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. "
        "depos,19,Customer#000000019,uc,3bHIx84H,wdrmLOjVsiqXCq2tr,18,28-396-526-5053,8914.710000,HOUSEHOLD, nag. furiously "
        "careful packages are slyly at the accounts. furiously regular in\n22,RUSSIA,3, requests against the platelets use never "
        "according to the quickly regular pint,20,Customer#000000020,JrPk8Pqplj4Ne,22,32-957-234-8742,7603.400000,FURNITURE,g "
        "alongside of the special excuses-- fluffily enticing packages wake \n8,INDIA,2,ss excuses cajole slyly across the "
        "packages. deposits print aroun,21,Customer#000000021,XYmVpr9yAHDEn,8,18-902-614-8344,1428.250000,MACHINERY, quickly "
        "final accounts integrate blithely furiously u\n3,CANADA,1,eas hang ironic, silent packages. slyly regular packages are "
        "furiously over the tithes. fluffily "
        "bold,22,Customer#000000022,QI6p41,FNs5k7RZoCCVPUTkUdYpB,3,13-806-545-9701,591.980000,MACHINERY,s nod furiously above "
        "the furiously ironic ideas. \n3,CANADA,1,eas hang ironic, silent packages. slyly regular packages are furiously over "
        "the tithes. fluffily bold,23,Customer#000000023,OdY "
        "W13N7Be3OC5MpgfmcYss0Wn6TKT,3,13-312-472-8245,3332.020000,HOUSEHOLD,deposits. special deposits cajole slyly. fluffily "
        "special deposits about the furiously \n13,JORDAN,4,ic deposits are blithely about the carefully regular "
        "pa,24,Customer#000000024,HXAFgIAyjxtdqwimt13Y3OZO 4xeLe7U8PqG,13,23-127-851-8031,9255.670000,MACHINERY,into beans. "
        "fluffily final ideas haggle fluffily\n12,JAPAN,2,ously. final, express gifts cajole "
        "a,25,Customer#000000025,Hp8GyFQgGHFYSilH5tBfe,12,22-603-468-3533,7133.700000,FURNITURE,y. accounts sleep ruthlessly "
        "according to the regular theodolites. unusual instructions sleep. ironic, final\n22,RUSSIA,3, requests against the "
        "platelets use never according to the quickly regular "
        "pint,26,Customer#000000026,8ljrc5ZeMl7UciP,22,32-363-455-4837,5182.050000,AUTOMOBILE,c requests use furiously ironic "
        "requests. slyly ironic dependencies us\n3,CANADA,1,eas hang ironic, silent packages. slyly regular packages are "
        "furiously over the tithes. fluffily "
        "bold,27,Customer#000000027,IS8GIyxpBrLpMT0u7,3,13-137-193-2709,5679.840000,BUILDING, about the carefully ironic pinto "
        "beans. accoun\n8,INDIA,2,ss excuses cajole slyly across the packages. deposits print "
        "aroun,28,Customer#000000028,iVyg0daQ,Tha8x2WPWA9m2529m,8,18-774-241-1462,1007.180000,FURNITURE, along the regular "
        "deposits. furiously final pac\n0,ALGERIA,0, haggle. carefully final deposits detect slyly "
        "agai,29,Customer#000000029,sJ5adtfyAkCK63df2,vF25zyQMVYE34uh,0,10-773-203-7342,7618.270000,FURNITURE,its after the "
        "carefully final platelets x-ray against \n1,ARGENTINA,1,al foxes promise slyly according to the regular accounts. bold "
        "requests "
        "alon,30,Customer#000000030,nJDsELGAavU63Jl0c5NKsKfL8rIJQQkQnYL2QJY,1,11-764-165-5076,9321.010000,BUILDING,lithely final "
        "requests. furiously unusual account\n23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. "
        "carefull,31,Customer#000000031,LUACbO0viaAv6eXOAebryDB xjVst,23,33-197-837-7094,5236.890000,HOUSEHOLD,s use among the "
        "blithely pending depo\n15,MOROCCO,0,rns. blithely bold courts among the closely regular packages use furiously bold "
        "platelets?,32,Customer#000000032,jD2xZzi UmId,DCtNBLXKj9q0Tlp2iQ6ZcO3J,15,25-430-914-2194,3471.530000,BUILDING,cial "
        "ideas. final, furious requests across the e\n17,PERU,1,platelets. blithely pending dependencies use fluffily across the "
        "even pinto beans. carefully silent accoun,33,Customer#000000033,qFSlMuLucBmx9xnn5ib2csWUweg "
        "D,17,27-375-391-1280,-78.560000,AUTOMOBILE,s. slyly regular accounts are furiously. carefully pending "
        "requests\n15,MOROCCO,0,rns. blithely bold courts among the closely regular packages use furiously bold "
        "platelets?,34,Customer#000000034,Q6G9wZ6dnczmtOx509xgE,M2KV,15,25-344-968-5422,8589.700000,HOUSEHOLD,nder against the "
        "even, pending accounts. even\n17,PERU,1,platelets. blithely pending dependencies use fluffily across the even pinto "
        "beans. carefully silent accoun,35,Customer#000000035,TEjWGE4nBzJL2,17,27-566-888-7431,1228.240000,HOUSEHOLD,requests. "
        "special, express requests nag slyly furiousl\n21,VIETNAM,2,hely enticingly express accounts. even, final "
        ",36,Customer#000000036,3TvCzjuPzpJ0,DdJ8kW5U,21,31-704-669-5769,4987.270000,BUILDING,haggle. enticing, quiet platelets "
        "grow quickly bold sheaves. carefully regular acc\n8,INDIA,2,ss excuses cajole slyly across the packages. deposits print "
        "aroun,37,Customer#000000037,7EV4Pwh,3SboctTWt,8,18-385-235-7162,-917.750000,FURNITURE,ilent packages are carefully "
        "among the deposits. furiousl\n12,JAPAN,2,ously. final, express gifts cajole a,38,Customer#000000038,a5Ee5e9568R8RLP "
        "2ap7,12,22-306-880-7212,6345.110000,HOUSEHOLD,lar excuses. closely even asymptotes cajole blithely excuses. carefully "
        "silent pinto beans sleep carefully fin\n2,BRAZIL,1,y alongside of the pending deposits. carefully special packages are "
        "about the ironic forges. slyly special ,39,Customer#000000039,nnbRg,Pvy33dfkorYE "
        "FdeZ60,2,12-387-467-6509,6264.310000,AUTOMOBILE,tions. slyly silent excuses slee\n3,CANADA,1,eas hang ironic, silent "
        "packages. slyly regular packages are furiously over the tithes. fluffily "
        "bold,40,Customer#000000040,gOnGWAyhSV1ofv,3,13-652-915-8939,1335.300000,BUILDING,rges impress after the slyly ironic "
        "courts. foxes are. blithely \n10,IRAN,4,efully alongside of the slyly final dependencies. "
        ",41,Customer#000000041,IM9mzmyoxeBmvNw8lA7G3Ydska2nkZF,10,20-917-711-4011,270.950000,HOUSEHOLD,ly regular accounts hang "
        "bold, silent packages. unusual foxes haggle slyly above the special, final depo\n5,ETHIOPIA,0,ven packages wake "
        "quickly. regu,42,Customer#000000042,ziSrvyyBke,5,15-416-330-4175,8727.010000,BUILDING,ssly according to the pinto "
        "beans: carefully special requests across the even, pending accounts wake special\n19,ROMANIA,3,ular asymptotes are "
        "about the furious multipliers. express dependencies nag above the ironically ironic "
        "account,43,Customer#000000043,ouSbjHk8lh5fKX3zGso3ZSIj9Aa3PoaFd,19,29-316-665-2897,9904.280000,MACHINERY,ial requests: "
        "carefully pending foxes detect quickly. carefully final courts cajole quickly. carefully\n16,MOZAMBIQUE,0,s. ironic, "
        "unusual asymptotes wake blithely "
        "r,44,Customer#000000044,Oi,dOSPwDu4jo4x,,P85E0dmhZGvNtBwi,16,26-190-260-5375,7315.940000,AUTOMOBILE,r requests around "
        "the unusual, bold a\n9,INDONESIA,2, slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey "
        "players sleep blithely. "
        "carefull,45,Customer#000000045,4v3OcpFgoOmMG,CbnF,4mdC,9,19-715-298-9917,9983.380000,AUTOMOBILE,nto beans haggle slyly "
        "alongside of t\n6,FRANCE,3,refully final requests. regular, "
        "ironi,46,Customer#000000046,eaTXWWm10L9,6,16-357-681-2007,5744.590000,AUTOMOBILE,ctions. accounts sleep furiously even "
        "requests. regular, regular accounts cajole blithely around the final pa\n2,BRAZIL,1,y alongside of the pending "
        "deposits. carefully special packages are about the ironic forges. slyly special ,47,Customer#000000047,b0UgocSqEW5 "
        "gdVbhNT,2,12-427-271-9466,274.580000,BUILDING,ions. express, ironic instructions sleep furiously ironic ideas. "
        "furi\n0,ALGERIA,0, haggle. carefully final deposits detect slyly agai,48,Customer#000000048,0UU "
        "iPhBupFvemNB,0,10-508-348-5882,3792.500000,BUILDING,re fluffily pending foxes. pending, bold platelets sleep slyly. "
        "even platelets cajo\n10,IRAN,4,efully alongside of the slyly final dependencies. "
        ",49,Customer#000000049,cNgAeX7Fqrdf7HQN9EwjUa4nxT,68L FKAxzl,10,20-908-631-4424,4573.940000,FURNITURE,nusual foxes! "
        "fluffily pending packages maintain to the regular \n6,FRANCE,3,refully final requests. regular, "
        "ironi,50,Customer#000000050,9SzDYlkzxByyJ1QeTI o,6,16-658-112-3221,4266.130000,MACHINERY,ts. furiously ironic accounts "
        "cajole furiously slyly ironic dinos.\n12,JAPAN,2,ously. final, express gifts cajole "
        "a,51,Customer#000000051,uR,wEaiTvo4,12,22-344-885-4251,855.870000,FURNITURE,eposits. furiously regular requests "
        "integrate carefully packages. furious\n11,IRAQ,4,nic deposits boost atop the quickly final requests? quickly "
        "regula,52,Customer#000000052,7 QOqGqqSy9jfV51BC71jcHJSD0,11,21-186-284-5998,5630.280000,HOUSEHOLD,ic platelets use "
        "evenly even accounts. stealthy theodolites cajole furiou\n15,MOROCCO,0,rns. blithely bold courts among the closely "
        "regular packages use furiously bold "
        "platelets?,53,Customer#000000053,HnaxHzTfFTZs8MuCpJyTbZ47Cm4wFOOgib,15,25-168-852-5363,4113.640000,HOUSEHOLD,ar "
        "accounts are. even foxes are blithely. fluffily pending deposits boost\n4,EGYPT,4,y above the carefully unusual "
        "theodolites. final dugouts are quickly across the furiously regular d,54,Customer#000000054,,k4vf "
        "5vECGWFy,hosTE,,4,14-776-370-4745,868.900000,AUTOMOBILE,sual, silent accounts. furiously express accounts cajole "
        "special deposits. final, final accounts use furi\n10,IRAN,4,efully alongside of the slyly final dependencies. "
        ",55,Customer#000000055,zIRBR4KNEl HzaiV3a i9n6elrxzDEh8r8pDom,10,20-180-440-8525,4572.110000,MACHINERY,ully unusual "
        "packages wake bravely bold packages. unusual requests boost deposits! blithely ironic packages ab\n10,IRAN,4,efully "
        "alongside of the slyly final dependencies. "
        ",56,Customer#000000056,BJYZYJQk4yD5B,10,20-895-685-6920,6530.860000,FURNITURE,. notornis wake carefully. carefully "
        "fluffy requests are furiously even accounts. slyly expre\n21,VIETNAM,2,hely enticingly express accounts. even, final "
        ",57,Customer#000000057,97XYbsuOPRXPWU,21,31-835-306-1650,4151.930000,AUTOMOBILE,ove the carefully special packages. "
        "even, unusual deposits sleep slyly pend\n13,JORDAN,4,ic deposits are blithely about the carefully regular "
        "pa,58,Customer#000000058,g9ap7Dk1Sv9fcXEWjpMYpBZIRUohi T,13,23-244-493-2508,6478.460000,HOUSEHOLD,ideas. ironic ideas "
        "affix furiously express, final instructions. regular excuses use quickly e\n1,ARGENTINA,1,al foxes promise slyly "
        "according to the regular accounts. bold requests "
        "alon,59,Customer#000000059,zLOCP0wh92OtBihgspOGl4,1,11-355-584-3112,3458.600000,MACHINERY,ously final packages haggle "
        "blithely after the express deposits. furiou\n12,JAPAN,2,ously. final, express gifts cajole "
        "a,60,Customer#000000060,FyodhjwMChsZmUz7Jz0H,12,22-480-575-5866,2741.870000,MACHINERY,latelets. blithely unusual courts "
        "boost furiously about the packages. blithely final instruct\n17,PERU,1,platelets. blithely pending dependencies use "
        "fluffily across the even pinto beans. carefully silent accoun,61,Customer#000000061,9kndve4EAJxhg3veF BfXr7AqOsT39o "
        "gtqjaYE,17,27-626-559-8599,1536.240000,FURNITURE,egular packages shall have to impress along the \n7,GERMANY,3,l "
        "platelets. regular accounts x-ray: unusual, regular "
        "acco,62,Customer#000000062,upJK2Dnw13,,7,17-361-978-7059,595.610000,MACHINERY,kly special dolphins. pinto beans are "
        "slyly. quickly regular accounts are furiously a\n21,VIETNAM,2,hely enticingly express accounts. even, final "
        ",63,Customer#000000063,IXRSpVWWZraKII,21,31-952-552-9584,9331.130000,AUTOMOBILE,ithely even accounts detect slyly above "
        "the fluffily ir\n3,CANADA,1,eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. "
        "fluffily bold,64,Customer#000000064,MbCeGY20kaKK3oalJD,OT,3,13-558-731-7204,-646.640000,BUILDING,structions after the "
        "quietly ironic theodolites cajole be\n23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. "
        "carefull,65,Customer#000000065,RGT yzQ0y4l0H90P783LG4U95bXQFDRXbWa1sl,X,23,33-733-623-5267,8795.160000,AUTOMOBILE,y "
        "final foxes serve carefully. theodolites are carefully. pending i\n22,RUSSIA,3, requests against the platelets use "
        "never according to the quickly regular "
        "pint,66,Customer#000000066,XbsEqXH1ETbJYYtA1A,22,32-213-373-5094,242.770000,HOUSEHOLD,le slyly accounts. carefully "
        "silent packages benea\n9,INDONESIA,2, slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey "
        "players sleep blithely. carefull,67,Customer#000000067,rfG0cOgtr5W8 "
        "xILkwp9fpCS8,9,19-403-114-4356,8166.590000,MACHINERY,indle furiously final, even theodo\n12,JAPAN,2,ously. final, "
        "express gifts cajole a,68,Customer#000000068,o8AibcCRkXvQFh8hF,7o,12,22-918-832-2411,6853.370000,HOUSEHOLD, pending "
        "pinto beans impress realms. final dependencies \n9,INDONESIA,2, slyly express asymptotes. regular deposits haggle "
        "slyly. carefully ironic hockey players sleep blithely. "
        "carefull,69,Customer#000000069,Ltx17nO9Wwhtdbe9QZVxNgP98V7xW97uvSH1prEw,9,19-225-978-5670,1709.280000,HOUSEHOLD,thely "
        "final ideas around the quickly final dependencies affix carefully quickly final theodolites. final accounts "
        "c\n22,RUSSIA,3, requests against the platelets use never according to the quickly regular "
        "pint,70,Customer#000000070,mFowIuhnHjp2GjCiYYavkW kUwOjIaTCQ,22,32-828-107-2832,4867.520000,FURNITURE,fter the special "
        "asymptotes. ideas after the unusual frets cajole quickly regular pinto be\n7,GERMANY,3,l platelets. regular accounts "
        "x-ray: unusual, regular "
        "acco,71,Customer#000000071,TlGalgdXWBmMV,6agLyWYDyIz9MKzcY8gl,w6t1B,7,17-710-812-5403,-611.190000,HOUSEHOLD,g courts "
        "across the regular, final pinto beans are blithely pending ac\n2,BRAZIL,1,y alongside of the pending deposits. "
        "carefully special packages are about the ironic forges. slyly special "
        ",72,Customer#000000072,putjlmskxE,zs,HqeIA9Wqu7dhgH5BVCwDwHHcf,2,12-759-144-9689,-362.860000,FURNITURE,ithely final "
        "foxes sleep always quickly bold accounts. final wat\n0,ALGERIA,0, haggle. carefully final deposits detect slyly "
        "agai,73,Customer#000000073,8IhIxreu4Ug6tt5mog4,0,10-473-439-3214,4288.500000,BUILDING,usual, unusual packages sleep "
        "busily along the furiou\n4,EGYPT,4,y above the carefully unusual theodolites. final dugouts are quickly across the "
        "furiously regular d,74,Customer#000000074,IkJHCA3ZThF7qL7VKcrU nRLl,kylf ,4,14-199-862-7209,2764.430000,MACHINERY,onic "
        "accounts. blithely slow packages would haggle carefully. qui\n18,CHINA,2,c dependencies. furiously express notornis "
        "sleep slyly regular accounts. ideas sleep. depos,75,Customer#000000075,Dh "
        "6jZ,cwxWLKQfRKkiGrzv6pm,18,28-247-803-9025,6684.100000,AUTOMOBILE, instructions cajole even, even deposits. finally "
        "bold deposits use above the even pains. slyl\n0,ALGERIA,0, haggle. carefully final deposits detect slyly "
        "agai,76,Customer#000000076,m3sbCvjMOHyaOofH,e UkGPtqc4,0,10-349-718-3044,5745.330000,FURNITURE,pecial deposits. ironic "
        "ideas boost blithely according to the closely ironic theodolites! furiously final deposits n\n17,PERU,1,platelets. "
        "blithely pending dependencies use fluffily across the even pinto beans. carefully silent "
        "accoun,77,Customer#000000077,4tAE5KdMFGD4byHtXF92vx,17,27-269-357-4674,1738.870000,BUILDING,uffily silent requests. "
        "carefully ironic asymptotes among the ironic hockey players are carefully bli\n9,INDONESIA,2, slyly express asymptotes. "
        "regular deposits haggle slyly. carefully ironic hockey players sleep blithely. "
        "carefull,78,Customer#000000078,HBOta,ZNqpg3U2cSL0kbrftkPwzX,9,19-960-700-9191,7136.970000,FURNITURE,ests. blithely bold "
        "pinto beans h\n15,MOROCCO,0,rns. blithely bold courts among the closely regular packages use furiously bold "
        "platelets?,79,Customer#000000079,n5hH2ftkVRwW8idtD,BmM2,15,25-147-850-4166,5121.280000,MACHINERY,es. packages haggle "
        "furiously. regular, special requests poach after the quickly express ideas. blithely pending re\n0,ALGERIA,0, haggle. "
        "carefully final deposits detect slyly agai,80,Customer#000000080,K,vtXp8qYB "
        ",0,10-267-172-7101,7383.530000,FURNITURE,tect among the dependencies. bold accounts engage closely even pinto beans. "
        "ca\n20,SAUDI ARABIA,4,ts. silent requests haggle. closely express packages sleep across the "
        "blithely,81,Customer#000000081,SH6lPA7JiiNC6dNTrR,20,30-165-277-3269,2023.710000,BUILDING,r packages. fluffily ironic "
        "requests cajole fluffily. ironically regular theodolit\n18,CHINA,2,c dependencies. furiously express notornis sleep "
        "slyly regular accounts. ideas sleep. "
        "depos,82,Customer#000000082,zhG3EZbap4c992Gj3bK,3Ne,Xn,18,28-159-442-5305,9468.340000,AUTOMOBILE,s wake. bravely "
        "regular accounts are furiously. regula\n22,RUSSIA,3, requests against the platelets use never according to the quickly "
        "regular pint,83,Customer#000000083,HnhTNB5xpnSF20JBH4Ycs6psVnkC3RDf,22,32-817-154-4122,6463.510000,BUILDING,ccording to "
        "the quickly bold warhorses. final, regular foxes integrate carefully. bold packages nag blithely ev\n11,IRAQ,4,nic "
        "deposits boost atop the quickly final requests? quickly regula,84,Customer#000000084,lpXz6Fwr9945rnbtMc8PlueilS1WmASr "
        "CB,11,21-546-818-3802,5174.710000,FURNITURE,ly blithe foxes. special asymptotes haggle blithely against the furiously "
        "regular depo\n5,ETHIOPIA,0,ven packages wake quickly. regu,85,Customer#000000085,siRerlDwiolhYR "
        "8FgksoezycLj,5,15-745-585-8219,3386.640000,FURNITURE,ronic ideas use above the slowly pendin\n0,ALGERIA,0, haggle. "
        "carefully final deposits detect slyly "
        "agai,86,Customer#000000086,US6EGGHXbTTXPL9SBsxQJsuvy,0,10-677-951-2353,3306.320000,HOUSEHOLD,quests. pending dugouts "
        "are carefully aroun\n23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. "
        "carefull,87,Customer#000000087,hgGhHVSWQl 6jZ6Ev,23,33-869-884-7053,6327.540000,FURNITURE,hely ironic requests "
        "integrate according to the ironic accounts. slyly regular pla\n16,MOZAMBIQUE,0,s. ironic, unusual asymptotes wake "
        "blithely r,88,Customer#000000088,wtkjBN9eyrFuENSMmMFlJ3e7jE5KXcg,16,26-516-273-2566,8031.440000,AUTOMOBILE,s are "
        "quickly above the quickly ironic instructions; even requests about the carefully final deposi\n14,KENYA,0, pending "
        "excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t,89,Customer#000000089,dtR, "
        "y9JQWUO6FoJExyp8whOU,14,24-394-451-5404,1530.760000,FURNITURE,counts are slyly beyond the slyly final accounts. quickly "
        "final ideas wake. r\n16,MOZAMBIQUE,0,s. ironic, unusual asymptotes wake blithely "
        "r,90,Customer#000000090,QxCzH7VxxYUWwfL7,16,26-603-491-1238,7354.230000,BUILDING,sly across the furiously even "
        "\n8,INDIA,2,ss excuses cajole slyly across the packages. deposits print "
        "aroun,91,Customer#000000091,S8OMYFrpHwoNHaGBeuS6E 6zhHGZiprw1b7 q,8,18-239-400-3677,4643.140000,AUTOMOBILE,onic "
        "accounts. fluffily silent pinto beans boost blithely according to the fluffily exp\n2,BRAZIL,1,y alongside of the "
        "pending deposits. carefully special packages are about the ironic forges. slyly special ,92,Customer#000000092,obP "
        "PULk2LH LqNF,K9hcbNqnLAkJVsl5xqSrY,,2,12-446-416-8471,1182.910000,MACHINERY,. pinto beans hang slyly final deposits. "
        "ac\n7,GERMANY,3,l platelets. regular accounts x-ray: unusual, regular "
        "acco,93,Customer#000000093,EHXBr2QGdh,7,17-359-388-5266,2182.520000,MACHINERY,press deposits. carefully regular "
        "platelets r\n9,INDONESIA,2, slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players "
        "sleep blithely. "
        "carefull,94,Customer#000000094,IfVNIN9KtkScJ9dUjK3Pg5gY1aFeaXewwf,9,19-953-499-8833,5500.110000,HOUSEHOLD,latelets "
        "across the bold, final requests sleep according to the fluffily bold accounts. unusual deposits amon\n15,MOROCCO,0,rns. "
        "blithely bold courts among the closely regular packages use furiously bold "
        "platelets?,95,Customer#000000095,EU0xvmWvOmUUn5J,2z85DQyG7QCJ9Xq7,15,25-923-255-2929,5327.380000,MACHINERY,ithely. "
        "ruthlessly final requests wake slyly alongside of the furiously silent pinto beans. even the\n8,INDIA,2,ss excuses "
        "cajole slyly across the packages. deposits print "
        "aroun,96,Customer#000000096,vWLOrmXhRR,8,18-422-845-1202,6323.920000,AUTOMOBILE,press requests believe furiously. "
        "carefully final instructions snooze carefully. \n17,PERU,1,platelets. blithely pending dependencies use fluffily across "
        "the even pinto beans. carefully silent accoun,97,Customer#000000097,OApyejbhJG,0Iw3j "
        "rd1M,17,27-588-919-5638,2164.480000,AUTOMOBILE,haggle slyly. bold, special ideas are blithely above the thinly bold "
        "theo\n12,JAPAN,2,ously. final, express gifts cajole "
        "a,98,Customer#000000098,7yiheXNSpuEAwbswDW,12,22-885-845-6889,-551.370000,BUILDING,ages. furiously pending accounts are "
        "quickly carefully final foxes: busily pe\n15,MOROCCO,0,rns. blithely bold courts among the closely regular packages use "
        "furiously bold platelets?,99,Customer#000000099,szsrOiPtCHVS97Lt,15,25-515-237-9232,4088.650000,HOUSEHOLD,cajole slyly "
        "about the regular theodolites! furiously bold requests nag along the pending, regular packages. somas\n20,SAUDI "
        "ARABIA,4,ts. silent requests haggle. closely express packages sleep across the "
        "blithely,100,Customer#000000100,fptUABXcmkC5Wx,20,30-749-445-4907,9889.890000,FURNITURE,was furiously fluffily quiet "
        "deposits. silent, pending requests boost against \n2,BRAZIL,1,y alongside of the pending deposits. carefully special "
        "packages are about the ironic forges. slyly special ,101,Customer#000000101,sMmL2rNeHDltovSm "
        "Y,2,12-514-298-3699,7470.960000,MACHINERY, sleep. pending packages detect slyly ironic pack\n19,ROMANIA,3,ular "
        "asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic "
        "account,102,Customer#000000102,UAtflJ06 fn9zBfKjInkQZlWtqaA,19,29-324-978-8538,8462.170000,BUILDING,ously regular "
        "dependencies nag among the furiously express dinos. blithely final\n9,INDONESIA,2, slyly express asymptotes. regular "
        "deposits haggle slyly. carefully ironic hockey players sleep blithely. "
        "carefull,103,Customer#000000103,8KIsQX4LJ7QMsj6DrtFtXu0nUEdV,8a,9,19-216-107-2107,2757.450000,BUILDING,furiously "
        "pending notornis boost slyly around the blithely ironic ideas? final, even instructions cajole fl\n10,IRAN,4,efully "
        "alongside of the slyly final dependencies. ,104,Customer#000000104,9mcCK L7rt0SwiYtrbO88DiZS7U "
        "d7M,10,20-966-284-8065,-588.380000,FURNITURE,rate carefully slyly special pla\n10,IRAN,4,efully alongside of the slyly "
        "final dependencies. ,105,Customer#000000105,4iSJe4L SPjg7kJj98Yz3z0B,10,20-793-553-6417,9091.820000,MACHINERY,l pains "
        "cajole even accounts. quietly final instructi\n1,ARGENTINA,1,al foxes promise slyly according to the regular accounts. "
        "bold requests alon,106,Customer#000000106,xGCOEAUjUNG,1,11-751-989-4627,3288.420000,MACHINERY,lose slyly. ironic "
        "accounts along the evenly regular theodolites wake about the special, final gifts. \n15,MOROCCO,0,rns. blithely bold "
        "courts among the closely regular packages use furiously bold "
        "platelets?,107,Customer#000000107,Zwg64UZ,q7GRqo3zm7P1tZIRshBDz,15,25-336-529-9919,2514.150000,AUTOMOBILE,counts cajole "
        "slyly. regular requests wake. furiously regular deposits about the blithely final fo\n5,ETHIOPIA,0,ven packages wake "
        "quickly. regu,108,Customer#000000108,GPoeEvpKo1,5,15-908-619-7526,2259.380000,BUILDING,refully ironic deposits sleep. "
        "regular, unusual requests wake slyly\n16,MOZAMBIQUE,0,s. ironic, unusual asymptotes wake blithely "
        "r,109,Customer#000000109,OOOkYBgCMzgMQXUmkocoLb56rfrdWp2NE2c,16,26-992-422-8153,-716.100000,BUILDING,es. fluffily final "
        "dependencies sleep along the blithely even pinto beans. final deposits haggle furiously furiou\n10,IRAN,4,efully "
        "alongside of the slyly final dependencies. "
        ",110,Customer#000000110,mymPfgphaYXNYtk,10,20-893-536-2069,7462.990000,AUTOMOBILE,nto beans cajole around the even, "
        "final deposits. quickly bold packages according to the furiously regular dept\n22,RUSSIA,3, requests against the "
        "platelets use never according to the quickly regular "
        "pint,111,Customer#000000111,CBSbPyOWRorloj2TBvrK9qp9tHBs,22,32-582-283-7528,6505.260000,MACHINERY,ly unusual "
        "instructions detect fluffily special deposits-- theodolites nag carefully during the ironic "
        "dependencies\n19,ROMANIA,3,ular asymptotes are about the furious multipliers. express dependencies nag above the "
        "ironically ironic account,112,Customer#000000112,RcfgG3bO7QeCnfjqJT1,19,29-233-262-8382,2953.350000,FURNITURE,rmanently "
        "unusual multipliers. blithely ruthless deposits are furiously along the\n12,JAPAN,2,ously. final, express gifts cajole "
        "a,113,Customer#000000113,eaOl5UBXIvdY57rglaIzqvfPD,MYfK,12,22-302-930-4756,2912.000000,BUILDING,usly regular "
        "theodolites boost furiously doggedly pending instructio\n14,KENYA,0, pending excuses haggle furiously deposits. "
        "pending, express pinto beans wake fluffily past t,114,Customer#000000114,xAt "
        "5f5AlFIU,14,24-805-212-7646,1027.460000,FURNITURE,der the carefully express theodolites are after the packages. "
        "packages are. bli\n8,INDIA,2,ss excuses cajole slyly across the packages. deposits print "
        "aroun,115,Customer#000000115,0WFt1IXENmUT2BgbsB0ShVKJZt0HCBCbFl0aHc,8,18-971-699-1843,7508.920000,HOUSEHOLD,sits haggle "
        "above the carefully ironic theodolite\n16,MOZAMBIQUE,0,s. ironic, unusual asymptotes wake blithely "
        "r,116,Customer#000000116,yCuVxIgsZ3,qyK2rloThy3u,16,26-632-309-5792,8403.990000,BUILDING,as. quickly final sauternes "
        "haggle slyly carefully even packages. brave, ironic pinto beans are above the furious\n24,UNITED STATES,1,y final "
        "packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto "
        "be,117,Customer#000000117,uNhM,PzsRA3S,5Y Ge5Npuhi,24,34-403-631-3505,3950.830000,FURNITURE,affix. instructions are "
        "furiously sl\n18,CHINA,2,c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. "
        "depos,118,Customer#000000118,OVnFuHygK9wx3xpg8,18,28-639-943-7051,3582.370000,AUTOMOBILE,uick packages alongside of the "
        "furiously final deposits haggle above the fluffily even foxes. blithely dogged dep\n7,GERMANY,3,l platelets. regular "
        "accounts x-ray: unusual, regular "
        "acco,119,Customer#000000119,M1ETOIecuvH8DtM0Y0nryXfW,7,17-697-919-8406,3930.350000,FURNITURE,express ideas. blithely "
        "ironic foxes thrash. special acco\n12,JAPAN,2,ously. final, express gifts cajole "
        "a,120,Customer#000000120,zBNna00AEInqyO1,12,22-291-534-1571,363.750000,MACHINERY, quickly. slyly ironic requests cajole "
        "blithely furiously final dependen\n17,PERU,1,platelets. blithely pending dependencies use fluffily across the even "
        "pinto beans. carefully silent accoun,121,Customer#000000121,tv "
        "nCR2YKupGN73mQudO,17,27-411-990-2959,6428.320000,BUILDING,uriously stealthy ideas. carefully final courts use "
        "carefully\n3,CANADA,1,eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily "
        "bold,122,Customer#000000122,yp5slqoNd26lAENZW3a67wSfXA6hTF,3,13-702-694-4520,7865.460000,HOUSEHOLD, the special "
        "packages hinder blithely around the permanent requests. bold depos\n5,ETHIOPIA,0,ven packages wake quickly. "
        "regu,123,Customer#000000123,YsOnaaER8MkvK5cpf4VSlq,5,15-817-151-1168,5897.830000,BUILDING,ependencies. regular, ironic "
        "requests are fluffily regu\n18,CHINA,2,c dependencies. furiously express notornis sleep slyly regular accounts. ideas "
        "sleep. depos,124,Customer#000000124,aTbyVAW5tCd,v09O,18,28-183-750-7809,1842.490000,AUTOMOBILE,le fluffily even "
        "dependencies. quietly s\n19,ROMANIA,3,ular asymptotes are about the furious multipliers. express dependencies nag above "
        "the ironically ironic account,125,Customer#000000125,,wSZXdVR "
        "xxIIfm9s8ITyLl3kgjT6UC07GY0Y,19,29-261-996-3120,-234.120000,FURNITURE,x-ray finally after the packages? regular "
        "requests c\n22,RUSSIA,3, requests against the platelets use never according to the quickly regular "
        "pint,126,Customer#000000126,ha4EHmbx3kg DYCsP6DFeUOmavtQlHhcfaqr,22,32-755-914-7592,1001.390000,HOUSEHOLD,s about the "
        "even instructions boost carefully furiously ironic pearls. ruthless, \n21,VIETNAM,2,hely enticingly express accounts. "
        "even, final ,127,Customer#000000127,Xyge4DX2rXKxXyye1Z47LeLVEYMLf4Bfcj,21,31-101-672-2951,9280.710000,MACHINERY,ic, "
        "unusual theodolites nod silently after the final, ironic instructions: pending r\n4,EGYPT,4,y above the carefully "
        "unusual theodolites. final dugouts are quickly across the furiously regular "
        "d,128,Customer#000000128,AmKUMlJf2NRHcKGmKjLS,4,14-280-874-8044,-986.960000,HOUSEHOLD,ing packages integrate across the "
        "slyly unusual dugouts. blithely silent ideas sublate carefully. blithely expr\n7,GERMANY,3,l platelets. regular "
        "accounts x-ray: unusual, regular "
        "acco,129,Customer#000000129,q7m7rbMM0BpaCdmxloCgBDRCleXsXkdD8kf,7,17-415-148-7416,9127.270000,HOUSEHOLD, unusual "
        "deposits boost carefully furiously silent ideas. pending accounts cajole slyly across\n9,INDONESIA,2, slyly express "
        "asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. "
        "carefull,130,Customer#000000130,RKPx2OfZy0Vn 8wGWZ7F2EAvmMORl1k8iH,9,19-190-993-9281,5073.580000,HOUSEHOLD,ix slowly. "
        "express packages along the furiously ironic requests integrate daringly deposits. fur\n11,IRAQ,4,nic deposits boost "
        "atop the quickly final requests? quickly "
        "regula,131,Customer#000000131,jyN6lAjb1FtH10rMC,XzlWyCBrg75,11,21-840-210-3572,8595.530000,HOUSEHOLD,jole special "
        "packages. furiously final dependencies about the furiously speci\n4,EGYPT,4,y above the carefully unusual theodolites. "
        "final dugouts are quickly across the furiously regular "
        "d,132,Customer#000000132,QM5YabAsTLp9,4,14-692-150-9717,162.570000,HOUSEHOLD,uickly carefully special theodolites. "
        "carefully regular requests against the blithely unusual instructions \n17,PERU,1,platelets. blithely pending "
        "dependencies use fluffily across the even pinto beans. carefully silent "
        "accoun,133,Customer#000000133,IMCuXdpIvdkYO92kgDGuyHgojcUs88p,17,27-408-997-8430,2314.670000,AUTOMOBILE,t packages. "
        "express pinto beans are blithely along the unusual, even theodolites. silent packages use fu\n11,IRAQ,4,nic deposits "
        "boost atop the quickly final requests? quickly "
        "regula,134,Customer#000000134,sUiZ78QCkTQPICKpA9OBzkUp2FM,11,21-200-159-5932,4608.900000,BUILDING,yly fluffy foxes "
        "boost final ideas. b\n19,ROMANIA,3,ular asymptotes are about the furious multipliers. express dependencies nag above "
        "the ironically ironic account,135,Customer#000000135,oZK,oC0 fdEpqUML,19,29-399-293-6241,8732.910000,FURNITURE, the "
        "slyly final accounts. deposits cajole carefully. carefully sly packag\n7,GERMANY,3,l platelets. regular accounts x-ray: "
        "unusual, regular acco,136,Customer#000000136,QoLsJ0v5C1IQbh,DS1,7,17-501-210-4726,-842.390000,FURNITURE,ackages sleep "
        "ironic, final courts. even requests above the blithely bold requests g\n16,MOZAMBIQUE,0,s. ironic, unusual asymptotes "
        "wake blithely r,137,Customer#000000137,cdW91p92rlAEHgJafqYyxf1Q,16,26-777-409-5654,7838.300000,HOUSEHOLD,carefully "
        "regular theodolites use. silent dolphins cajo\n5,ETHIOPIA,0,ven packages wake quickly. "
        "regu,138,Customer#000000138,5uyLAeY7HIGZqtu66Yn08f,5,15-394-860-4589,430.590000,MACHINERY,ts doze on the busy ideas. "
        "regular\n9,INDONESIA,2, slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep "
        "blithely. carefull,139,Customer#000000139,3ElvBwudHKL02732YexGVFVt "
        ",9,19-140-352-1403,7897.780000,MACHINERY,nstructions. quickly ironic ideas are carefully. bold, \n4,EGYPT,4,y above the "
        "carefully unusual theodolites. final dugouts are quickly across the furiously regular "
        "d,140,Customer#000000140,XRqEPiKgcETII,iOLDZp5jA,4,14-273-885-6505,9963.150000,MACHINERY,ies detect slyly ironic "
        "accounts. slyly ironic theodolites hag\n1,ARGENTINA,1,al foxes promise slyly according to the regular accounts. bold "
        "requests "
        "alon,141,Customer#000000141,5IW,WROVnikc3l7DwiUDGQNGsLBGOL6Dc0,1,11-936-295-6204,6706.140000,FURNITURE,packages nag "
        "furiously. carefully unusual accounts snooze according to the fluffily regular pinto beans. slyly spec\n9,INDONESIA,2, "
        "slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. "
        "carefull,142,Customer#000000142,AnJ5lxtLjioClr2khl9pb8NLxG2,,9,19-407-425-2584,2209.810000,AUTOMOBILE,. even, express "
        "theodolites upo\n16,MOZAMBIQUE,0,s. ironic, unusual asymptotes wake blithely r,143,Customer#000000143,681r22uL452zqk "
        "8By7I9o9enQfx0,16,26-314-406-7725,2186.500000,MACHINERY,across the blithely unusual requests haggle "
        "theodo\n1,ARGENTINA,1,al foxes promise slyly according to the regular accounts. bold requests "
        "alon,144,Customer#000000144,VxYZ3ebhgbltnetaGjNC8qCccjYU05 fePLOno8y,1,11-717-379-4478,6417.310000,MACHINERY,ges. slyly "
        "regular accounts are slyly. bold, idle reque\n13,JORDAN,4,ic deposits are blithely about the carefully regular "
        "pa,145,Customer#000000145,kQjHmt2kcec cy3hfMh969u,13,23-562-444-8454,9748.930000,HOUSEHOLD,ests? express, express "
        "instructions use. blithely fina\n3,CANADA,1,eas hang ironic, silent packages. slyly regular packages are furiously over "
        "the tithes. fluffily "
        "bold,146,Customer#000000146,GdxkdXG9u7iyI1,,y5tq4ZyrcEy,3,13-835-723-3223,3328.680000,FURNITURE,ffily regular dinos are "
        "slyly unusual requests. slyly specia\n18,CHINA,2,c dependencies. furiously express notornis sleep slyly regular "
        "accounts. ideas sleep. "
        "depos,147,Customer#000000147,6VvIwbVdmcsMzuu,C84GtBWPaipGfi7DV,18,28-803-187-4335,8071.400000,AUTOMOBILE,ress packages "
        "above the blithely regular packages sleep fluffily blithely ironic accounts. \n11,IRAQ,4,nic deposits boost atop the "
        "quickly final requests? quickly regula,148,Customer#000000148,BhSPlEWGvIJyT9swk "
        "vCWE,11,21-562-498-6636,2135.600000,HOUSEHOLD,ing to the carefully ironic requests. carefully regular dependencies "
        "about the theodolites wake furious\n19,ROMANIA,3,ular asymptotes are about the furious multipliers. express "
        "dependencies nag above the ironically ironic "
        "account,149,Customer#000000149,3byTHCp2mNLPigUrrq,19,29-797-439-6760,8959.650000,AUTOMOBILE,al instructions haggle "
        "against the slyly bold w\n18,CHINA,2,c dependencies. furiously express notornis sleep slyly regular accounts. ideas "
        "sleep. depos,150,Customer#000000150,zeoGShTjCwGPplOWFkLURrh41O0AZ8dwNEEN4 ,18,28-328-564-7630,3849.480000,MACHINERY,ole "
        "blithely among the furiously pending packages. furiously bold ideas wake fluffily ironic idea\n19,ROMANIA,3,ular "
        "asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic "
        "account,151,Customer#000000151,LlyEtNEXT6kkZ,kGP46H,19,29-433-197-6339,5187.020000,HOUSEHOLD,regular dugouts: blithely "
        "even dolphins cajole furiously carefull\n8,INDIA,2,ss excuses cajole slyly across the packages. deposits print "
        "aroun,152,Customer#000000152,PDrllSkScKLh4lr19gmUZnK,8,18-585-850-3926,1215.180000,BUILDING,ously ironic accounts. "
        "furiously even accounts accord\n6,FRANCE,3,refully final requests. regular, "
        "ironi,153,Customer#000000153,kDzx11sIjjWJm1,6,16-342-316-2815,5454.260000,HOUSEHOLD,promise carefully. unusual deposits "
        "x-ray. carefully regular tithes u\n19,ROMANIA,3,ular asymptotes are about the furious multipliers. express dependencies "
        "nag above the ironically ironic account,154,Customer#000000154,2LAlU "
        "fDHkOqbXjHHDqw1mJQNC,19,29-522-835-6914,4695.120000,FURNITURE,nic packages haggle blithely across the\n0,ALGERIA,0, "
        "haggle. carefully final deposits detect slyly "
        "agai,155,Customer#000000155,l,sSphiStMgdrxpxi,0,10-566-282-8705,5902.850000,AUTOMOBILE, sleep ironic, bold requests. "
        "regular packages on the quiet dependencies\n9,INDONESIA,2, slyly express asymptotes. regular deposits haggle slyly. "
        "carefully ironic hockey players sleep blithely. "
        "carefull,156,Customer#000000156,5OS0edX2Y6B1cf9wJNuOQWgrrZccXk9,9,19-723-913-3943,9302.950000,AUTOMOBILE, regular foxes "
        "above the theodolites haggle \n15,MOROCCO,0,rns. blithely bold courts among the closely regular packages use furiously "
        "bold platelets?,157,Customer#000000157,HGEouzCcFrNd "
        "nBAdsCRjsMxKOvYZdbwA7he5w9v,15,25-207-442-1556,9768.730000,BUILDING, pinto beans against the carefully bold requests "
        "wake quickly alongside of the final accounts. accounts\n10,IRAN,4,efully alongside of the slyly final dependencies. "
        ",158,Customer#000000158,2HaYxi0J1620aoI1CdFyrW,rWOy,10,20-383-680-1329,6160.950000,AUTOMOBILE,ecoys. fluffily quick "
        "requests use flu\n10,IRAN,4,efully alongside of the slyly final dependencies. "
        ",159,Customer#000000159,KotsdDO6EHnysVu922s6pjZpG,vlT,10,20-888-668-2668,2060.060000,HOUSEHOLD,cingly express somas "
        "haggle above the theodolites. pinto beans use special theodolites. theodolites sleep \n13,JORDAN,4,ic deposits are "
        "blithely about the carefully regular "
        "pa,160,Customer#000000160,5soVQ3dOCRBWBS,13,23-428-666-4806,4363.170000,MACHINERY,olites. silently ironic accounts "
        "cajole furious\n7,GERMANY,3,l platelets. regular accounts x-ray: unusual, regular "
        "acco,161,Customer#000000161,2oRkx,NtjFUh,7,17-805-718-2449,3714.060000,MACHINERY,ptotes nag carefully instructions. "
        "silent accounts are. furiously even accounts alongside\n8,INDIA,2,ss excuses cajole slyly across the packages. deposits "
        "print aroun,162,Customer#000000162,JE398sXZt2QuKXfJd7poNpyQFLFtth,8,18-131-101-2267,6268.990000,MACHINERY,accounts "
        "along the doggedly special asymptotes boost blithely during the quickly regular theodolites. slyly \n21,VIETNAM,2,hely "
        "enticingly express accounts. even, final "
        ",163,Customer#000000163,OgrGcOnm4whd0f,21,31-863-349-4121,2948.610000,FURNITURE, nag furiously furiously final "
        "requests. slyly s\n4,EGYPT,4,y above the carefully unusual theodolites. final dugouts are quickly across the furiously "
        "regular d,164,Customer#000000164,YDW51PBWLXLnbQlKC,4,14-565-638-9768,208.450000,HOUSEHOLD,ironic, special pinto beans. "
        "ironic\n0,ALGERIA,0, haggle. carefully final deposits detect slyly "
        "agai,165,Customer#000000165,8pc6kwBmwBdEnfVP53aqL9DM4LymC4,0,10-927-209-5601,3349.920000,HOUSEHOLD, requests. final "
        "ideas cajole quickly at the special, ironic acco\n10,IRAN,4,efully alongside of the slyly final dependencies. "
        ",166,Customer#000000166,15HWGtwoP77EJfd95HxtMSTZUelV8NOKne2,10,20-320-530-5920,2042.210000,FURNITURE,the packages. "
        "blithely final packages are furiously unusual asymptotes. regular frets promise carefully u\n5,ETHIOPIA,0,ven packages "
        "wake quickly. "
        "regu,167,Customer#000000167,QNc2eOlRIzL6jpthwgDuB866uCIUPiOX,5,15-288-395-5501,1468.090000,AUTOMOBILE,espite the ironic "
        "excuses. furiously final deposits wake slyly. slyly ex\n12,JAPAN,2,ously. final, express gifts cajole "
        "a,168,Customer#000000168,GDcL5qU86P8,oaTwVBCLE6asM8rlxpE,211uziU,12,22-354-984-5361,-808.560000,FURNITURE,blithely "
        "final accounts sleep quickly along the regular ideas. furiously sly foxes nag across the\n18,CHINA,2,c dependencies. "
        "furiously express notornis sleep slyly regular accounts. ideas sleep. "
        "depos,169,Customer#000000169,NjhmHa7xrcjE,18,28-362-499-3728,4483.830000,FURNITURE,fully unusual pinto beans. blithely "
        "express asymptotes lose carefully regular instructions? accounts b\n15,MOROCCO,0,rns. blithely bold courts among the "
        "closely regular packages use furiously bold "
        "platelets?,170,Customer#000000170,5QmxmYubNhn6HAgLwTvphevM3OmpZTGsM,15,25-879-984-9818,7687.890000,BUILDING, regular "
        "requests. carefully regu\n7,GERMANY,3,l platelets. regular accounts x-ray: unusual, regular "
        "acco,171,Customer#000000171,RIhjJCrth89EU7xRSvN,7,17-513-603-7451,2379.910000,MACHINERY,ly furiously final requests. "
        "slyly final requests wake silently pending, silent accounts. exp\n22,RUSSIA,3, requests against the platelets use never "
        "according to the quickly regular "
        "pint,172,Customer#000000172,KwgdKUL1G2WacsMNF50yX,22,32-178-964-1847,1134.400000,MACHINERY,losely regular, unusual "
        "instructions. \n9,INDONESIA,2, slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players "
        "sleep blithely. carefull,173,Customer#000000173,Aue7KVz,FinSHpov "
        "Vk5ed,wSQ2BRSioJ0,9,19-443-196-8008,845.840000,BUILDING,s pinto beans use thinly slyly regular packages. instructions "
        "print along the s\n23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. "
        "carefull,174,Customer#000000174,R5 fCPMSeDXtUpp5Ax,23,33-845-455-8799,1944.730000,FURNITURE,oldly even requests haggle "
        "quickly blithely ironic accounts. idly final foxes doze slyly pending dep\n10,IRAN,4,efully alongside of the slyly "
        "final dependencies. "
        ",175,Customer#000000175,8YK1ZyTqoY3wMWnExl4itPMLL793GpEZb6T,10,20-427-617-9922,1975.350000,FURNITURE,ly final platelets "
        "are final pinto b\n13,JORDAN,4,ic deposits are blithely about the carefully regular "
        "pa,176,Customer#000000176,9hBepY2uz88HlCqToOLgeU770u81FeL,13,23-432-942-8830,-375.760000,FURNITURE,uriously. final "
        "requests sleep ironic packages. quickly\n1,ARGENTINA,1,al foxes promise slyly according to the regular accounts. bold "
        "requests alon,177,Customer#000000177,6wzEKPyZE9dmBCJZ8e7x7fiiK,k,1,11-917-786-9955,7457.500000,BUILDING,nal dolphins: "
        "blithely bold gifts wake slyly afte\n21,VIETNAM,2,hely enticingly express accounts. even, final "
        ",178,Customer#000000178,p HUSDg8Cgan4Fj8Drvcdz4gi4dSqV0a7n 0ag,21,31-436-268-6327,2272.500000,FURNITURE,unts. blithely "
        "regular dependencies kindle pending deposits. quietly express deposits wake above the Tiresias-- ex\n4,EGYPT,4,y above "
        "the carefully unusual theodolites. final dugouts are quickly across the furiously regular "
        "d,179,Customer#000000179,djez3CWg0nnCiu60jsF,4,14-703-953-2987,-43.080000,MACHINERY,st furiously. idly regular "
        "instructions wake fluffily slyl\n13,JORDAN,4,ic deposits are blithely about the carefully regular "
        "pa,180,Customer#000000180,DSGW3RFoYJE opVw,Y3wGCGcNULZi,13,23-678-802-2105,-92.580000,FURNITURE,lar accounts sublate "
        "above the slyly final\n9,INDONESIA,2, slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey "
        "players sleep blithely. carefull,181,Customer#000000181,YNviWd "
        "WrRkZvSw1OxIewBq,9,19-653-305-8440,3929.960000,FURNITURE,final requests cajole furiously acro\n3,CANADA,1,eas hang "
        "ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily "
        "bold,182,Customer#000000182,tdwvgepG316CCTHtMaF8Q,3,13-199-211-9023,4810.220000,AUTOMOBILE,quickly against the blithely "
        "even deposits; epitaphs unwind quickly along the carefully regular excuses. furio\n22,RUSSIA,3, requests against the "
        "platelets use never according to the quickly regular pint,183,Customer#000000183,aMAB2QSb8 "
        "86MAx,22,32-771-279-8154,4419.890000,HOUSEHOLD,sual accounts across the slyl\n21,VIETNAM,2,hely enticingly express "
        "accounts. even, final "
        ",184,Customer#000000184,uoOpBuRr42f1WIqnVYAhxbAA9bkK6HUGpOt,21,31-739-340-5476,170.460000,AUTOMOBILE,hely according to "
        "the furiously unusual accounts. furiously bold platele\n5,ETHIOPIA,0,ven packages wake quickly. "
        "regu,185,Customer#000000185,iHXzQgienOQ,5,15-760-572-8760,2788.760000,BUILDING,t the ironic accounts. fluffily regular "
        "requests wake slyly ironic pinto beans. slyly unusu\n3,CANADA,1,eas hang ironic, silent packages. slyly regular "
        "packages are furiously over the tithes. fluffily "
        "bold,186,Customer#000000186,BeVr6MzaobBENXRBC8pmOmkByMJI,3,13-518-743-2576,8737.500000,HOUSEHOLD,e slyly final "
        "dependencies. unusual instructions against the carefully pending instructions boost quickly\n4,EGYPT,4,y above the "
        "carefully unusual theodolites. final dugouts are quickly across the furiously regular "
        "d,187,Customer#000000187,OIlgR6oIRXV5g63q5YGudCjRD8kpod2p,4,14-716-294-6674,-774.220000,FURNITURE,r deposits. carefully "
        "silent packages after the fluffily even instructio\n5,ETHIOPIA,0,ven packages wake quickly. "
        "regu,188,Customer#000000188,58Srs6gEEoD3ZfwgXDM1OayRiaSY6K9YsveWwV,5,15-613-528-7811,9533.370000,BUILDING,st slyly "
        "special platelets. bold,\n22,RUSSIA,3, requests against the platelets use never according to the quickly regular "
        "pint,189,Customer#000000189,r51HSq Rg8wQgF1CBfG1Vbye3GK,22,32-980-348-1114,-594.050000,MACHINERY,sly express patterns. "
        "ideas on the regular d\n11,IRAQ,4,nic deposits boost atop the quickly final requests? quickly "
        "regula,190,Customer#000000190,F2X,GhSqLz8k "
        "u0gWsirsraFaEDEo6vIGtOTaO1T,11,21-730-373-8193,1657.460000,AUTOMOBILE,uickly-- fluffily pending instructions "
        "boo\n16,MOZAMBIQUE,0,s. ironic, unusual asymptotes wake blithely "
        "r,191,Customer#000000191,P1eCXsPWkv2y6ENQv,16,26-811-707-6869,2945.160000,BUILDING,o beans hinder slyly bold "
        "accounts.\n10,IRAN,4,efully alongside of the slyly final dependencies. ,192,Customer#000000192,rDmB2c9d1BJQ "
        "y6R9jTx86YI77D,10,20-750-712-2481,8239.960000,MACHINERY,ely unusual packages are fluffily \n23,UNITED KINGDOM,3,eans "
        "boost carefully special requests. accounts are. "
        "carefull,193,Customer#000000193,dUT4dtsPTZ6ZpkWLc,KGJCHY6JDJgPFH4,23,33-182-978-6287,8024.550000,MACHINERY,y even "
        "theodolites. final foxes print along the final pinto beans. theodoli\n16,MOZAMBIQUE,0,s. ironic, unusual asymptotes "
        "wake blithely r,194,Customer#000000194,mksKhdWuQ1pjbc4yffHp8rRmLOMcJ,16,26-597-636-3003,6696.490000,HOUSEHOLD,quickly "
        "across the fluffily dogged requests. regular platelets around the ironic, even requests cajole quickl\n22,RUSSIA,3, "
        "requests against the platelets use never according to the quickly regular "
        "pint,195,Customer#000000195,WiqQD8hscyKekjMcSBA7AX 0AbxvBV,22,32-757-684-6845,4873.910000,AUTOMOBILE, should detect "
        "blithely. quickly even packages above the deposits wak\n18,CHINA,2,c dependencies. furiously express notornis sleep "
        "slyly regular accounts. ideas sleep. "
        "depos,196,Customer#000000196,68RstNo6a2B,18,28-135-177-2472,7760.330000,FURNITURE,accounts wake. express instructions "
        "according to the s\n1,ARGENTINA,1,al foxes promise slyly according to the regular accounts. bold requests "
        "alon,197,Customer#000000197,UeVqssepNuXmtZ38D,1,11-107-312-6585,9860.220000,AUTOMOBILE,ickly final accounts cajole. "
        "furiously re\n1,ARGENTINA,1,al foxes promise slyly according to the regular accounts. bold requests "
        "alon,198,Customer#000000198,,7fcZHIUn,fUaQtK8U,Q8,1,11-237-758-6141,3824.760000,AUTOMOBILE,tions. slyly ironic waters "
        "wa\n4,EGYPT,4,y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular "
        "d,199,Customer#000000199,lBU3xll,a7e9TYm3 UyjDPCVMvnHKpq,9HW1X,4,14-136-924-5232,7654.310000,FURNITURE,fully busy pinto "
        "beans. packages cajole around the express, bold packages! quickly ironic tithes\n16,MOZAMBIQUE,0,s. ironic, unusual "
        "asymptotes wake blithely r,200,Customer#000000200,x1 "
        "H5c66DUgH2pgNTJhw6eZKgrAz,16,26-472-302-4189,9967.600000,BUILDING,e after the ironic, even realms. fluffily regular "
        "packages doze-- courts haggle carefully! blithely\n";
    EXPECT_EQ(content, expectedContent);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// join two static data sources together with the batch join operator.
// Joins the full 150k record Customer table, may take up to a minute (todo this is too slow)
TEST_F(StaticDataSourceIntegrationTest, testBatchJoinNationCustomerFull) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();
    crdConf->worker.numberOfBuffersInGlobalBufferManager = 10000;
    crdConf->worker.numWorkerThreads = 8;

    NES_DEBUG("num work {} num buff {}",
              crdConf->worker.numWorkerThreads.getValue(),
              crdConf->worker.numberOfBuffersInGlobalBufferManager.getValue());

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;

    // use deprecated feature "logicalSources" to register logical streams before physical streams
    crdConf->logicalSources.add(LogicalSource::create("tpch_customer", schema_customer));
    crdConf->logicalSources.add(LogicalSource::create("tpch_nation", schema_nation));

    PhysicalSourceTypePtr sourceType0 =
        StaticDataSourceType::create(table_path_nation_s0001, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource0 = PhysicalSource::create("tpch_nation", "tpch_s0001_nation", sourceType0);
    crdConf->worker.physicalSources.add(physicalSource0);

    PhysicalSourceTypePtr sourceType1 =
        StaticDataSourceType::create(table_path_customer_s0001, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource1 = PhysicalSource::create("tpch_customer", "tpch_s0001_customer", sourceType1);
    crdConf->worker.physicalSources.add(physicalSource1);

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    // local fs
    std::string filePath = getTestResourceFolder() / "testTwoTableJoinOut.csv";
    remove(filePath.c_str());

    bool benchmark = true;

    //register query
    std::string queryLogic =
        R"(Query::from("tpch_nation")
            .batchJoinWith(Query::from("tpch_customer"))
                .where(Attribute("C_NATIONKEY"))
                .equalsTo(Attribute("N_NATIONKEY")))";
    std::string querySink = benchmark ? ".sink(NullOutputSinkDescriptor::create());"
                                      : ".sink(FileSinkDescriptor::create(\"" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    std::string queryString = queryLogic + querySink;

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

    // extract total query runtime from statistics
    auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
    printTotalQueryRuntime(stats);

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    if (!benchmark) {
        std::ifstream ifs(filePath.c_str());
        std::ifstream ifsInputProbeSide(table_path_customer_s0001.c_str());
        EXPECT_TRUE(ifs.good());
        EXPECT_TRUE(ifsInputProbeSide.good());

        int numResultTuples = std::count(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>(), '\n');
        int numExpectedTuples =
            std::count(std::istreambuf_iterator<char>(ifsInputProbeSide), std::istreambuf_iterator<char>(), '\n')
            + 1;// the .csv file contains a header, so we expect one more

        NES_ASSERT(numResultTuples == numExpectedTuples,
                   "The Join changed the number of tuples from " << numExpectedTuples << " in the probe table to "
                                                                 << numResultTuples);
    }

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// join two static data sources together with the batch join operator
TEST_F(StaticDataSourceIntegrationTest, testBatchJoinIntegersOnly) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;

    // use deprecated feature "logicalSources" to register logical streams before physical streams
    crdConf->logicalSources.add(LogicalSource::create("static_integers_only_0", schema_integers_0));
    crdConf->logicalSources.add(LogicalSource::create("static_integers_only_1", schema_integers_1));

    PhysicalSourceTypePtr sourceType0 =
        StaticDataSourceType::create(table_path_integers_0, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource0 = PhysicalSource::create("static_integers_only_0", "static_integers_only_0", sourceType0);
    crdConf->worker.physicalSources.add(physicalSource0);

    PhysicalSourceTypePtr sourceType1 =
        StaticDataSourceType::create(table_path_integers_1, 0, "wrapBuffer", /* placeholder: */ 0, true);
    auto physicalSource1 = PhysicalSource::create("static_integers_only_1", "static_integers_only_1", sourceType1);
    crdConf->worker.physicalSources.add(physicalSource1);

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    // local fs
    std::string filePath = getTestResourceFolder() / "testBatchJoinIntegersOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("static_integers_only_0")
            .batchJoinWith(
                Query::from("static_integers_only_1")
                ).where(Attribute("static_integers_only_1$id") == Attribute("static_integers_only_0$id"))
            .sink(FileSinkDescriptor::create(")"
        + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

    // extract total query runtime from statistics
    auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
    printTotalQueryRuntime(stats);

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected =
        "static_integers_only_0$id:INTEGER,static_integers_only_0$value:INTEGER,"
        "static_integers_only_1$id:INTEGER,static_integers_only_1$one:INTEGER,static_integers_only_1$value:INTEGER\n"
        "1,10,1,1,1000\n"
        "2,20,2,1,2000\n"
        "3,30,3,1,3000\n"
        "4,40,4,1,4000\n"
        "5,50,5,1,5000\n"
        "6,60,6,1,6000\n"
        "7,70,7,1,7000\n"
        "8,80,8,1,8000\n"
        "9,90,9,1,9000\n"
        "10,100,10,1,10000\n";
    EXPECT_EQ(content, expected);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// join two static data sources together with the batch join operator
TEST_F(StaticDataSourceIntegrationTest, DISABLED_testBatchJoinIntegersOnlyPartitioned) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;

    // use deprecated feature "logicalSources" to register logical streams before physical streams
    crdConf->logicalSources.add(LogicalSource::create("static_integers_only_0", schema_integers_0));
    crdConf->logicalSources.add(LogicalSource::create("static_integers_only_1", schema_integers_1));

    // register one physical partitions of the build side source
    PhysicalSourceTypePtr sourceType0 =
        StaticDataSourceType::create(table_path_integers_0, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource0 = PhysicalSource::create("static_integers_only_0", "static_integers_only_0", sourceType0);
    crdConf->worker.physicalSources.add(physicalSource0);

    // register two physical partitions of the probe side source
    PhysicalSourceTypePtr sourceType1a =
        StaticDataSourceType::create(table_path_integers_1a, 0, "wrapBuffer", /* placeholder: */ 0, true);
    PhysicalSourceTypePtr sourceType1b =
        StaticDataSourceType::create(table_path_integers_1b, 0, "wrapBuffer", /* placeholder: */ 0, true);
    auto physicalSource1a = PhysicalSource::create("static_integers_only_1", "static_integers_only_1a", sourceType1a);
    auto physicalSource1b = PhysicalSource::create("static_integers_only_1", "static_integers_only_1", sourceType1b);
    crdConf->worker.physicalSources.add(physicalSource1a);
    crdConf->worker.physicalSources.add(physicalSource1b);

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    // local fs
    std::string filePath = getTestResourceFolder() / "testBatchJoinIntegersOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("static_integers_only_0")
            .batchJoinWith(
                Query::from("static_integers_only_1")
                ).where(Attribute("static_integers_only_1$id") == Attribute("static_integers_only_0$id"))
            .sink(FileSinkDescriptor::create(")"
        + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

    // extract total query runtime from statistics
    auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
    printTotalQueryRuntime(stats);

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected =
        "static_integers_only_0$id:INTEGER,static_integers_only_0$value:INTEGER,"
        "static_integers_only_1$id:INTEGER,static_integers_only_1$one:INTEGER,static_integers_only_1$value:INTEGER\n"
        "1,10,1,1,1000\n"
        "2,20,2,1,2000\n"
        "3,30,3,1,3000\n"
        "4,40,4,1,4000\n"
        "5,50,5,1,5000\n"
        "6,60,6,1,6000\n"
        "7,70,7,1,7000\n"
        "8,80,8,1,8000\n"
        "9,90,9,1,9000\n"
        "10,100,10,1,10000\n";
    EXPECT_EQ(content, expected);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// join two static data sources together with the batch join operator
TEST_F(StaticDataSourceIntegrationTest, testBatchJoinIntegersOnlyWithOtherOperations) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;

    // use deprecated feature "logicalSources" to register logical streams before physical streams
    crdConf->logicalSources.add(LogicalSource::create("static_integers_only_0", schema_integers_0));
    crdConf->logicalSources.add(LogicalSource::create("static_integers_only_1", schema_integers_1));

    PhysicalSourceTypePtr sourceType0 =
        StaticDataSourceType::create(table_path_integers_0, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource0 = PhysicalSource::create("static_integers_only_0", "static_integers_only_0", sourceType0);
    crdConf->worker.physicalSources.add(physicalSource0);

    PhysicalSourceTypePtr sourceType1 =
        StaticDataSourceType::create(table_path_integers_1, 0, "wrapBuffer", /* placeholder: */ 0, true);
    auto physicalSource1 = PhysicalSource::create("static_integers_only_1", "static_integers_only_1", sourceType1);
    crdConf->worker.physicalSources.add(physicalSource1);

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    // local fs
    std::string filePath = getTestResourceFolder() / "testBatchJoinIntegersOut.csv";
    remove(filePath.c_str());

    //register query

    // this applies a filter on the PROBE side:
    std::string queryString =
        R"(Query::from("static_integers_only_0")
            .filter(Attribute("static_integers_only_0$id") < 6)
            .project(Attribute("value"), Attribute("id"))
            .batchJoinWith(
                Query::from("static_integers_only_1")
                .project(Attribute("value"), Attribute("id"), Attribute("one"))
                ).where(Attribute("static_integers_only_1$id") == Attribute("static_integers_only_0$id"))
            .map(Attribute("IDplusID") = Attribute("static_integers_only_1$id") + Attribute("static_integers_only_0$id"))
            .sink(FileSinkDescriptor::create(")"
        + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

    // extract total query runtime from statistics
    auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
    printTotalQueryRuntime(stats);

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected = "static_integers_only_0$value:INTEGER,static_integers_only_0$id:INTEGER,"
                                 "static_integers_only_1$value:INTEGER,static_integers_only_1$id:INTEGER,"
                                 "static_integers_only_1$one:INTEGER,static_integers_only_0$IDplusID:INTEGER\n"
                                 "10,1,1000,1,1,2\n"
                                 "20,2,2000,2,1,4\n"
                                 "30,3,3000,3,1,6\n"
                                 "40,4,4000,4,1,8\n"
                                 "50,5,5000,5,1,10\n";
    EXPECT_EQ(content, expected);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// join two static data sources together with the batch join operator
TEST_F(StaticDataSourceIntegrationTest, DISABLED_testBatchJoinIntegersOnlyRemoteProbeSource) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();
    WorkerConfigurationPtr wrkConfRemote = WorkerConfiguration::create();

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;

    // use deprecated feature "logicalSources" to register logical streams before physical streams
    crdConf->logicalSources.add(LogicalSource::create("static_integers_only_0", schema_integers_0));
    crdConf->logicalSources.add(LogicalSource::create("static_integers_only_1", schema_integers_1));

    PhysicalSourceTypePtr sourceType0 =
        StaticDataSourceType::create(table_path_integers_0, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource0 = PhysicalSource::create("static_integers_only_0", "static_integers_only_0", sourceType0);
    crdConf->worker.physicalSources.add(physicalSource0);

    PhysicalSourceTypePtr sourceType1 =
        StaticDataSourceType::create(table_path_integers_1, 0, "wrapBuffer", /* placeholder: */ 0, true);
    auto physicalSource1 = PhysicalSource::create("static_integers_only_1", "static_integers_only_1", sourceType1);
    wrkConfRemote->physicalSources.add(physicalSource1);

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    wrkConfRemote->coordinatorPort = port;
    NesWorkerPtr wrkRemote = std::make_shared<NesWorker>(std::move(wrkConfRemote));
    bool retStartRemote = wrkRemote->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStartRemote);
    NES_INFO("StaticDataSourceIntegrationTest: Remote worker started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    // local fs
    std::string filePath = getTestResourceFolder() / "testBatchJoinIntegersOut.csv";
    remove(filePath.c_str());

    //register query
    std::string queryString =
        R"(Query::from("static_integers_only_0")
            .batchJoinWith(
                Query::from("static_integers_only_1")
                ).where(Attribute("static_integers_only_1$id") == Attribute("static_integers_only_0$id"))
            .sink(FileSinkDescriptor::create(")"
        + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

    // extract total query runtime from statistics
    auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
    printTotalQueryRuntime(stats);

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    const std::string expected =
        "static_integers_only_0$id:INTEGER,static_integers_only_0$value:INTEGER,"
        "static_integers_only_1$id:INTEGER,static_integers_only_1$one:INTEGER,static_integers_only_1$value:INTEGER\n"
        "1,10,1,1,1000\n"
        "2,20,2,1,2000\n"
        "3,30,3,1,3000\n"
        "4,40,4,1,4000\n"
        "5,50,5,1,5000\n"
        "6,60,6,1,6000\n"
        "7,70,7,1,7000\n"
        "8,80,8,1,8000\n"
        "9,90,9,1,9000\n"
        "10,100,10,1,10000\n";
    EXPECT_EQ(content, expected);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

// join two static data sources together with the batch join operator. CUSTOMER with an artificial table.
TEST_F(StaticDataSourceIntegrationTest, testBatchJoinCustomerWithIntTable) {
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();
    // preloading tpch:customer in static data sorce requires 8650 x 4MB buffers
    crdConf->worker.numberOfBuffersInGlobalBufferManager = 10000;
    crdConf->worker.numWorkerThreads = 8;

    NES_DEBUG("StaticDataSourceIntegrationTest::testBatchJoinCustomerWithIntTable: num work {} num buff {}",
              crdConf->worker.numWorkerThreads.getValue(),
              crdConf->worker.numberOfBuffersInGlobalBufferManager.getValue());

    crdConf->rpcPort = (*rpcCoordinatorPort);
    crdConf->restPort = *restPort;

    // use deprecated feature "logicalSources" to register logical streams before physical streams
    crdConf->logicalSources.add(LogicalSource::create("static_integers_only_2",
                                                      schema_integers_0));// (integers_only_2 has same schema as integers_only_0)
    crdConf->logicalSources.add(LogicalSource::create("tpch_customer", schema_customer));

    PhysicalSourceTypePtr sourceType0 =
        StaticDataSourceType::create(table_path_integers_2, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource0 = PhysicalSource::create("static_integers_only_2", "static_integers_only_2", sourceType0);
    crdConf->worker.physicalSources.add(physicalSource0);

    PhysicalSourceTypePtr sourceType1 =
        StaticDataSourceType::create(table_path_customer_s0001, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
    auto physicalSource1 = PhysicalSource::create("tpch_customer", "tpch_customer_s0001", sourceType1);
    crdConf->worker.physicalSources.add(physicalSource1);

    NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();
    auto sourceCatalog = crd->getSourceCatalog();

    // local fs
    std::string filePath = getTestResourceFolder() / "testBatchJoinIntegersOut.csv";
    remove(filePath.c_str());

    bool benchmark = false;

    //register query
    std::string queryLogic = R"(Query::from("static_integers_only_2")
        .batchJoinWith(Query::from("tpch_customer"))
            .where(Attribute("tpch_customer$C_NATIONKEY") == Attribute("static_integers_only_2$id")))";
    std::string querySink = benchmark ? ".sink(NullOutputSinkDescriptor::create());"
                                      : ".sink(FileSinkDescriptor::create(\"" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
    std::string queryString = queryLogic + querySink;

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    int buffersToExpect = 1;
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

    // extract total query runtime from statistics
    auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
    printTotalQueryRuntime(stats);

    NES_INFO("StaticDataSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    if (!benchmark) {
        // does the join result have same number of rows as the probe table?
        std::ifstream ifs(filePath.c_str());
        std::ifstream ifsInputProbeSide(table_path_customer_s0001.c_str());
        EXPECT_TRUE(ifs.good());
        EXPECT_TRUE(ifsInputProbeSide.good());

        int numResultTuples = std::count(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>(), '\n');
        int numExpectedTuples =
            std::count(std::istreambuf_iterator<char>(ifsInputProbeSide), std::istreambuf_iterator<char>(), '\n')
            + 1;// the .csv file contains a header, so we expect one more

        ifs.seekg(0, ifs.beg);

        NES_DEBUG("numResultTuples: {}, numExpectedTuples: {}", numResultTuples, numExpectedTuples);
        EXPECT_EQ(numResultTuples, numExpectedTuples);

        /* Deactivate this potion of the test, as the order might change
        // extract and validate first 10 records
        std::vector<std::string> content(11);
        for (int i=0; i<11; ++i) {
           std::getline(ifs, content[i], '\n');
        }
        const std::vector<std::string> expected = {
               "static_integers_only_2$id:INTEGER,static_integers_only_2$value:INTEGER,tpch_customer$C_CUSTKEY:INTEGER,tpch_customer$C_NAME:ArrayType,"
               "tpch_customer$C_ADDRESS:ArrayType,tpch_customer$C_NATIONKEY:INTEGER,tpch_customer$C_PHONE:ArrayType,"
               "tpch_customer$C_ACCTBAL:(Float),tpch_customer$C_MKTSEGMENT:ArrayType,tpch_customer$C_COMMENT:ArrayType",
               "15,150,1,Customer#000000001,IVhzIApeRb ot,c,E,15,25-989-741-2988,711.560000,BUILDING,to the even, regular platelets. regular, ironic epitaphs nag e",
               "13,130,2,Customer#000000002,XSTf4,NCwDVaWNe6tEgvwfmRchLXak,13,23-768-687-3665,121.650000,AUTOMOBILE,l accounts. blithely ironic theodolites integrate boldly: caref",
               "1,10,3,Customer#000000003,MG9kdTD2WBHm,1,11-719-748-3364,7498.120000,AUTOMOBILE, deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov",
               "4,40,4,Customer#000000004,XxVSJsLAGtn,4,14-128-190-5944,2866.830000,MACHINERY, requests. final, regular ideas sleep final accou",
               "3,30,5,Customer#000000005,KvpyuHCplrB84WgAiGV6sYpZq7Tj,3,13-750-942-6364,794.470000,HOUSEHOLD,n accounts will have to unwind. foxes cajole accor",
               "20,200,6,Customer#000000006,sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn,20,30-114-968-4951,7638.570000,AUTOMOBILE,tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious",
               "18,180,7,Customer#000000007,TcGe5gaZNgVePxU5kRrvXBfkasDTea,18,28-190-982-9759,9561.950000,AUTOMOBILE,ainst the ironic, express theodolites. express, even pinto beans among the exp",
               "17,170,8,Customer#000000008,I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5,17,27-147-574-9335,6819.740000,BUILDING,among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide",
               "8,80,9,Customer#000000009,xKiAFTjUsCuxfeleNqefumTrjS,8,18-338-906-3675,8324.070000,FURNITURE,r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl",
               "5,50,10,Customer#000000010,6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2,5,15-741-346-9870,2753.540000,HOUSEHOLD,es regular deposits haggle. fur"
        };
        EXPECT_EQ(content, expected);
        */
    }

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

// join two static data sources together with the batch join operator.
// two artificial tables of configurable length (generated during test)
// useful for benchmarking
TEST_F(StaticDataSourceIntegrationTest, testBatchJoinLargeIntTables) {
    std::string pathProbe = "./test_data/tmp_probe_table.tbl";
    std::string pathBuild = "./test_data/tmp_build_table.tbl";

    std::vector<int> sizeBuildTableVec = {250000};
    std::vector<int> sizeProbeTableVec = {
        50000,
        150000,
        //            500000,
        //            1500000,
        //            5000000,
        //            15000000
        //            40000000,
        //            60000000
    };
    for (int sizeBuildTable : sizeBuildTableVec) {

        {
            std::ofstream myfile(pathBuild);
            for (int i = 0; i < sizeBuildTable; ++i) {
                myfile << i << '|' << i * 10 << "|\n";
            }
            myfile.close();
        }

        for (int sizeProbeTable : sizeProbeTableVec) {
            {
                std::ofstream myfile(pathProbe);
                for (int i = 0; i < sizeProbeTable; ++i) {
                    myfile << (i % sizeBuildTable) << '|' << i << "|\n";
                }
                myfile.close();
            }

            CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::createDefault();
            // preloading tpch:customer in static data sorce requires 8650 x 4MB buffers
            crdConf->worker.numberOfBuffersInGlobalBufferManager = 1000000;
            crdConf->worker.numWorkerThreads = 2;

            NES_INFO("num work {} num buff {}",
                     crdConf->worker.numWorkerThreads.getValue(),
                     crdConf->worker.numberOfBuffersInGlobalBufferManager.getValue());

            crdConf->rpcPort = (*rpcCoordinatorPort);
            crdConf->restPort = *restPort;

            // use deprecated feature "logicalSources" to register logical streams before physical streams
            crdConf->logicalSources.add(
                LogicalSource::create("build_side", schema_integers_0));// (same schema as integers_only_0)
            crdConf->logicalSources.add(LogicalSource::create("probe_side", schema_integers_0));

            PhysicalSourceTypePtr sourceType0 =
                StaticDataSourceType::create(pathBuild, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
            auto physicalSource0 = PhysicalSource::create("build_side", "build_side", sourceType0);
            crdConf->worker.physicalSources.add(physicalSource0);

            PhysicalSourceTypePtr sourceType1 =
                StaticDataSourceType::create(pathProbe, 0, "wrapBuffer", /* placeholder: */ 0, /* late start? */ true);
            auto physicalSource1 = PhysicalSource::create("probe_side", "probe_side", sourceType1);
            crdConf->worker.physicalSources.add(physicalSource1);

            NES_INFO("StaticDataSourceIntegrationTest: Start coordinator");

            NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
            uint64_t port = crd->startCoordinator(/**blocking**/ false);
            EXPECT_NE(port, 0UL);
            NES_INFO("StaticDataSourceIntegrationTest: Coordinator started successfully");

            RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
            auto queryCatalog = crd->getQueryCatalog();
            auto sourceCatalog = crd->getSourceCatalog();

            // local fs
            std::string filePath = getTestResourceFolder() / "testBatchJoinIntegersOut.csv";
            remove(filePath.c_str());

            bool benchmark = true;

            //register query
            std::string queryLogic = R"(Query::from("build_side")
            .batchJoinWith(Query::from("probe_side"))
            .where(Attribute("probe_side$id") == Attribute("build_side$id")))";
            std::string querySink = benchmark
                ? ".sink(NullOutputSinkDescriptor::create());"
                : ".sink(FileSinkDescriptor::create(\"" + filePath + R"(" , "CSV_FORMAT", "APPEND"));)";
            std::string queryString = queryLogic + querySink;

            QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryString, "BottomUp");
            EXPECT_NE(queryId, INVALID_QUERY_ID);
            auto globalQueryPlan = crd->getGlobalQueryPlan();
            EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
            int buffersToExpect = 1;
            EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, buffersToExpect, true));

            // extract total query runtime from statistics
            auto stats = crd->getQueryStatistics(globalQueryPlan->getSharedQueryId(queryId));
            NES_WARNING("sizeProbeTable: {} sizeBuildTable: {}", sizeProbeTable, sizeBuildTable);
            printTotalQueryRuntime(stats);

            NES_INFO("StaticDataSourceIntegrationTest: Remove query");
            ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

            if (!benchmark) {
                // does the join result have same number of rows as the probe table?
                std::ifstream ifs(filePath.c_str());
                std::ifstream ifsInputProbeSide(table_path_customer_s0001.c_str());
                EXPECT_TRUE(ifs.good());
                EXPECT_TRUE(ifsInputProbeSide.good());

                int numResultTuples = std::count(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>(), '\n');
                int numExpectedTuples =
                    std::count(std::istreambuf_iterator<char>(ifsInputProbeSide), std::istreambuf_iterator<char>(), '\n')
                    + 1;// the .csv file contains a header, so we expect one more

                ifs.seekg(0, ifs.beg);

                NES_DEBUG("numResultTuples: {}, numExpectedTuples: {}", numResultTuples, numExpectedTuples);
                EXPECT_EQ(numResultTuples, numExpectedTuples);

                // extract and print first 10 records
                std::string content;
                for (int i = 0; i < 11; ++i) {
                    std::getline(ifs, content, '\n');
                    NES_INFO("{}", content);
                }
            }

            bool retStopCord = crd->stopCoordinator(false);
            EXPECT_TRUE(retStopCord);
        }
    }
}

}// namespace NES::Experimental
