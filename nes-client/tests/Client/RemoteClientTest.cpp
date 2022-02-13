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

#include "../../nes-core/tests/util/NesBaseTest.hpp"
#include "Util/TestUtils.hpp"
#include "gtest/gtest.h"
#include <API/Query.hpp>
#include <Client/QueryConfig.hpp>
#include <Client/RemoteClient.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Util/Logger.hpp>

#include <unistd.h>

using namespace std;

namespace NES {
using namespace Configurations;

class RemoteClientTest : public Testing::NESBaseTest {
  protected:
    static void SetUpTestCase() { NES::setupLogging("RemoteClientTest.log", NES::LOG_DEBUG); }
    static void TearDownTestCase() { NES_INFO("Tear down RemoteClientTest test class."); }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();

        auto crdConf = CoordinatorConfiguration::create();
        auto wrkConf = WorkerConfiguration::create();

        crdConf->rpcPort=(*rpcCoordinatorPort);
        crdConf->restPort = *restPort;
        wrkConf->coordinatorPort = *rpcCoordinatorPort;
        NES_DEBUG("RemoteClientTest: Start coordinator");
        crd = std::make_shared<NesCoordinator>(crdConf);
        uint64_t port = crd->startCoordinator(false);
        EXPECT_NE(port, 0UL);
        NES_DEBUG("RemoteClientTest: Coordinator started successfully");
        TestUtils::waitForWorkers(*restPort, 5, 0);

        NES_DEBUG("RemoteClientTest: Start worker 1");
        wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        bool retStart1 = wrk->start(false, true);
        EXPECT_TRUE(retStart1);
        NES_DEBUG("RemoteClientTest: Worker1 started successfully");
        TestUtils::waitForWorkers(*restPort, 5, 1);

        client = std::make_shared<Client::RemoteClient>("localhost", *restPort);
    }

    void TearDown() override {
        NES_DEBUG("Tear down RemoteClientTest class.");
        wrk->stop(true);
        crd->stopCoordinator(true);
        Testing::NESBaseTest::TearDown();
    }

    bool stopQuery(int64_t queryId) {
        for (int i = 0; i < 5; i++) {
            sleep(2);
            client->stopQuery(queryId);
            if (!crd->getQueryCatalog()->isQueryRunning(queryId)) {
                return true;
            }
        }
        return false;
    }

  public:
    NesCoordinatorPtr crd;
    NesWorkerPtr wrk;
    Client::RemoteClientPtr client;
};

/**
 * @brief Test if the testConnection call works properly
 */
TEST_F(RemoteClientTest, TestConnectionTest) {
    bool connect = client->testConnection();
    EXPECT_TRUE(connect);
}

/**
 * @brief Test if doploying a query over the REST api works properly
 * @result deployed query ID is valid
 */
TEST_F(RemoteClientTest, DeployQueryTest) {
    Query query = Query::from("default_logical");
    int64_t queryId = client->submitQuery(query);

    EXPECT_TRUE(crd->getQueryCatalog()->queryExists(queryId));
}

/**
 * @brief Test if deploying a query works properly
 * @result deployed query ID is valid
 */
TEST_F(RemoteClientTest, SubmitQueryTest) {
    Query query = Query::from("default_logical");
    auto queryPlan = query.getQueryPlan();
    int64_t queryId = client->submitQuery(queryPlan);

    EXPECT_TRUE(crd->getQueryCatalog()->queryExists(queryId));
    auto insertedQueryPlan = crd->getQueryCatalog()->getQueryCatalogEntry(queryId)->getInputQueryPlan();
    // Expect that the query id and query sub plan id from the deserialized query plan are valid
    EXPECT_FALSE(insertedQueryPlan->getQueryId() == INVALID_QUERY_ID);
    EXPECT_FALSE(insertedQueryPlan->getQuerySubPlanId() == INVALID_QUERY_SUB_PLAN_ID);

    stopQuery(queryId);
}

/**
 * @brief Test if retrieving the topology works properly
 * @result topology is as expected
 */
TEST_F(RemoteClientTest, GetTopologyTest) {
    std::string topology = client->getTopology();
    std::string expect = "{\"edges\":";
    EXPECT_TRUE(topology.compare(0, expect.size() - 1, expect));
}

/**
 * @brief Test if retrieving the query plan works properly
 * @result query plan is as expected
 */
TEST_F(RemoteClientTest, GetQueryPlanTest) {
    Query query = Query::from("default_logical");
    int64_t queryId = client->submitQuery(query);
    sleep(2);
    std::string query_plan = client->getQueryPlan(queryId);

    std::string expect = "{\"edges\":";
    EXPECT_TRUE(query_plan.compare(0, expect.size() - 1, expect));
    stopQuery(queryId);
}

/**
 * @brief Test if stopping a query works properly
 * @result query is stopped as expected
 */
TEST_F(RemoteClientTest, StopQueryTest) {
    Query query = Query::from("default_logical");
    int64_t queryId = client->submitQuery(query);

    for (int i = 0; i < 5; i++) {
        sleep(2);
        NES_INFO("StopQueryTest: client->stopQuery(queryId);" + to_string(crd->getQueryCatalog()->isQueryRunning(queryId)));
        client->stopQuery(queryId);
        NES_INFO("StopQueryTest: client->stopQuery(queryId);--");
        if (!crd->getQueryCatalog()->isQueryRunning(queryId)) {
            return;
        }
    }
    EXPECT_TRUE(!crd->getQueryCatalog()->isQueryRunning(queryId));
}

/**
 * @brief Test if retrieving the execution plan works properly
 * @result execution plan is as expected
 */
TEST_F(RemoteClientTest, GetExecutionPlanTest) {
    Query query = Query::from("default_logical");
    int64_t queryId = client->submitQuery(query);
    std::string execution_plan = client->getQueryExecutionPlan(queryId);
    NES_DEBUG("GetExecutionPlanTest: " + execution_plan);
    std::string expect = "{\"executionNodes\":[]}";

    EXPECT_TRUE(execution_plan.compare(0, expect.size() - 1, expect));
    stopQuery(queryId);
}

/**
 * @brief Test if adding and getting logical streams properly
 */
TEST_F(RemoteClientTest, AddAndGetLogicalStreamTest) {
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32);
    bool success = client->addLogicalStream(schema, "test");

    EXPECT_TRUE(success);
    std::string logical_stream = client->getLogicalStreams();
    NES_DEBUG("AddAndGetLogicalStreamTest " + logical_stream);
}

/**
 * @brief Test if retrieving the logical stream work properly
 * @note we assume that default_logical is predefined
 */
TEST_F(RemoteClientTest, GetLogicalStreamTest) {
    std::string logical_stream = client->getLogicalStreams();
    NES_DEBUG("GetLogicalStreamTest: " + logical_stream);
    // Check only for default stream
    std::string expect = "{\"default_logical\":\"id:INTEGER value:INTEGER \"";
    EXPECT_TRUE(logical_stream.compare(0, expect.size() - 1, expect));
}

/**
 * @brief Test if getting physical streams works properly
 * @note we assume that default_logical is predefined
 */
TEST_F(RemoteClientTest, GetPhysicalStreamTest) {
    std::string physicaStreams = client->getPhysicalStreams();
    NES_DEBUG("GetPhysicalStreamTest " + physicaStreams);
    // Check only for default stream
    std::string expect = "{\"default_logical\":\"id:INTEGER value:INTEGER";
    EXPECT_TRUE(physicaStreams.compare(0, expect.size() - 1, expect));
}

/**
 * @brief Test getting queries works properly
 */
TEST_F(RemoteClientTest, GetQueriesTest) {
    Query query = Query::from("default_logical");
    int64_t queryId = client->submitQuery(query);

    std::string queries = client->getQueries();
    std::string expect = "[{\"queryId\":";
    EXPECT_TRUE(queries.compare(0, expect.size() - 1, expect));
    stopQuery(queryId);
}

/**
 * @brief Test getting queries by status works properly
 */
TEST_F(RemoteClientTest, GetQueriesWithStatusTest) {
    Query query = Query::from("default_logical");
    int64_t queryId = client->submitQuery(query);

    std::string queries = client->getQueries(Registered);
    std::string expect = "[{\"queryId\":";
    EXPECT_TRUE(queries.compare(0, expect.size() - 1, expect));
    stopQuery(queryId);
}
}// namespace NES
