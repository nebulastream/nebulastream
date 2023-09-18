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

#include <gtest/gtest.h>

#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Query.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Client/ClientException.hpp>
#include <Client/QueryConfig.hpp>
#include <Client/RemoteClient.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <unistd.h>

using namespace std;

namespace NES {
using namespace Configurations;

class RemoteClientTest : public Testing::BaseIntegrationTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("RemoteClientTest.log", NES::LogLevel::LOG_DEBUG); }
    static void TearDownTestCase() { NES_INFO("Tear down RemoteClientTest test class."); }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();

        auto crdConf = CoordinatorConfiguration::createDefault();
        auto wrkConf = WorkerConfiguration::create();

        crdConf->rpcPort = (*rpcCoordinatorPort);
        crdConf->restPort = *restPort;
        wrkConf->coordinatorPort = *rpcCoordinatorPort;
        NES_DEBUG("RemoteClientTest: Start coordinator");
        crd = std::make_shared<NesCoordinator>(crdConf);
        uint64_t port = crd->startCoordinator(false);
        EXPECT_NE(port, 0UL);
        NES_DEBUG("RemoteClientTest: Coordinator started successfully");
        ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 5, 0));

        NES_DEBUG("RemoteClientTest: Start worker 1");
        DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create();
        auto physicalSource1 = PhysicalSource::create("default_logical", "physical_car", defaultSourceType1);
        wrkConf->physicalSources.add(physicalSource1);
        wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        bool retStart1 = wrk->start(false, true);
        ASSERT_TRUE(retStart1);
        NES_DEBUG("RemoteClientTest: Worker1 started successfully");
        ASSERT_TRUE(TestUtils::waitForWorkers(*restPort, 5, 1));

        client = std::make_shared<Client::RemoteClient>("localhost", *restPort, std::chrono::seconds(20), true);
    }

    void TearDown() override {
        NES_DEBUG("Tear down RemoteClientTest class.");
        wrk->stop(true);
        crd->stopCoordinator(true);
        Testing::BaseIntegrationTest::TearDown();
    }

    bool stopQuery(int64_t queryId) {
        auto res = client->stopQuery(queryId);
        if (!!res) {
            while (true) {
                auto statusStr = client->getQueryStatus(queryId);
                auto status = magic_enum::enum_cast<QueryState>(statusStr);
                if (status == QueryState::STOPPED) {
                    break;
                }
                NES_DEBUG("Query {} not stopped yet but {}", queryId, statusStr);
                usleep(500 * 1000);// 500ms
            }
            return true;
        }
        return false;
    }

    void checkForQueryStart(int64_t queryId) {
        auto defaultTimeout = std::chrono::seconds(10);
        auto timeoutInSec = std::chrono::seconds(defaultTimeout);
        auto startTs = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < startTs + timeoutInSec) {
            auto status = magic_enum::enum_cast<QueryState>(client->getQueryStatus(queryId));
            if (status == QueryState::REGISTERED || status == QueryState::OPTIMIZING || status == QueryState::DEPLOYED) {
                NES_DEBUG("Query {} not started yet", queryId);
                sleep(1);
            } else {
                return;
            }
        }
        throw Exceptions::RuntimeException("Test checkForQueryStart timeout exceeds.");
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
    ASSERT_TRUE(connect);
}

/**
 * @brief Test if deploying a query over the REST api works properly
 * @result deployed query ID is valid
 */
TEST_F(RemoteClientTest, DeployQueryTest) {
    Query query = Query::from("default_logical").sink(NullOutputSinkDescriptor::create());
    int64_t queryId = client->submitQuery(query);
    checkForQueryStart(queryId);
    ASSERT_TRUE(crd->getQueryCatalogService()->getEntryForQuery(queryId));
    ASSERT_TRUE(stopQuery(queryId));
}

/**
 * @brief Test if deploying a query works properly
 * @result deployed query ID is valid
 */
TEST_F(RemoteClientTest, SubmitQueryTest) {
    Query query = Query::from("default_logical").sink(NullOutputSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();
    int64_t queryId = client->submitQuery(queryPlan);
    checkForQueryStart(queryId);
    ASSERT_TRUE(crd->getQueryCatalogService()->getEntryForQuery(queryId));
    auto insertedQueryPlan = crd->getQueryCatalogService()->getEntryForQuery(queryId)->getInputQueryPlan();
    // Expect that the query id and query sub plan id from the deserialized query plan are valid
    EXPECT_FALSE(insertedQueryPlan->getQueryId() == INVALID_QUERY_ID);
    EXPECT_FALSE(insertedQueryPlan->getQuerySubPlanId() == INVALID_QUERY_SUB_PLAN_ID);

    ASSERT_TRUE(stopQuery(queryId));
}

TEST_F(RemoteClientTest, SubmitQueryWithWrongLogicalSourceNameTest) {
    Query query = Query::from("default_l").sink(NullOutputSinkDescriptor::create());
    try {
        client->submitQuery(query);
        FAIL();
    } catch (Client::ClientException const& e) {
        std::string errorMessage = e.what();
        constexpr auto expected = "The logical source 'default_l' can not be found in the SourceCatalog";
        EXPECT_NE(errorMessage.find(expected), std::string::npos);
    } catch (...) {
        // wrong exception
        FAIL();
    }
}

/**
 * @brief Test if retrieving the topology works properly
 * @result topology is as expected
 */
TEST_F(RemoteClientTest, GetTopologyTest) {
    std::string topology = client->getTopology();
    std::string expect = "{\"edges\":";
    ASSERT_TRUE(topology.compare(0, expect.size() - 1, expect));
}

/**
 * @brief Test if retrieving the query plan works properly
 * @result query plan is as expected
 */
TEST_F(RemoteClientTest, GetQueryPlanTest) {
    Query query = Query::from("default_logical").sink(NullOutputSinkDescriptor::create());
    int64_t queryId = client->submitQuery(query);
    checkForQueryStart(queryId);
    std::string query_plan = client->getQueryPlan(queryId);

    std::string expect = "{\"edges\":";
    ASSERT_TRUE(query_plan.compare(0, expect.size() - 1, expect));

    ASSERT_TRUE(stopQuery(queryId));
}

/**
 * @brief Test if correct error message is thrown for query plan retrieval with invalid query id
 * @result the information that query id does not exist
 */
TEST_F(RemoteClientTest, CorrectnessOfGetQueryPlan) {
    Query query = Query::from("default_logical").sink(NullOutputSinkDescriptor::create());
    int64_t queryId = client->submitQuery(query);
    checkForQueryStart(queryId);
    int64_t nonExistingQueryId = queryId + 1;
    std::string response = client->getQueryPlan(nonExistingQueryId);
    auto jsonResponse = nlohmann::json::parse(response);

    std::string restSDKResponse = "Provided QueryId: " + to_string(nonExistingQueryId) + " does not exist";
    std::string oatppResponse = "No query with given ID: " + to_string(nonExistingQueryId);
    EXPECT_TRUE(jsonResponse["message"] == restSDKResponse || jsonResponse["message"] == oatppResponse);
    ASSERT_TRUE(stopQuery(queryId));
}

/**
 * @brief Test if stopping a query works properly
 * @result query is stopped as expected
 */
TEST_F(RemoteClientTest, StopQueryTest) {
    Query query = Query::from("default_logical").sink(NullOutputSinkDescriptor::create());
    int64_t queryId = client->submitQuery(query);
    checkForQueryStart(queryId);
    auto res = client->stopQuery(queryId);
    ASSERT_TRUE(!!res);
    ASSERT_NE(crd->getQueryCatalogService()->getEntryForQuery(queryId)->getQueryState(), QueryState::RUNNING);
}

/**
 * @brief Test if retrieving the execution plan works properly
 * @result execution plan is as expected
 */
TEST_F(RemoteClientTest, GetExecutionPlanTest) {
    Query query = Query::from("default_logical").sink(NullOutputSinkDescriptor::create());
    int64_t queryId = client->submitQuery(query);
    checkForQueryStart(queryId);
    std::string execution_plan = client->getQueryExecutionPlan(queryId);
    NES_DEBUG("GetExecutionPlanTest: {}", execution_plan);
    std::string expect = "{\"executionNodes\":[]}";

    ASSERT_TRUE(execution_plan.compare(0, expect.size() - 1, expect));
    ASSERT_TRUE(stopQuery(queryId));
}

/**
 * @brief Test if adding and getting logical sources properly
 */
TEST_F(RemoteClientTest, AddAndGetLogicalSourceTest) {
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32);
    bool success = client->addLogicalSource(schema, "test");

    ASSERT_TRUE(success);
    std::string logical_source = client->getLogicalSources();
    NES_DEBUG("AddAndGetLogicalSourceTest {}", logical_source);
}

/**
 * @brief Test if retrieving the logical source work properly
 * @note we assume that default_logical is predefined
 */
TEST_F(RemoteClientTest, GetLogicalSourceTest) {
    std::string logical_source = client->getLogicalSources();
    NES_DEBUG("GetLogicalSourceTest: {}", logical_source);
    // Check only for default source
    std::string expect = "{\"default_logical\":\"id:INTEGER value:INTEGER \"";
    ASSERT_TRUE(logical_source.compare(0, expect.size() - 1, expect));
}

/**
 * @brief Test if getting physical sources works properly
 * @note we assume that default_logical is predefined
 */
TEST_F(RemoteClientTest, GetPhysicalSourceTest) {
    std::string physicaSources = client->getPhysicalSources("default_logical");
    NES_DEBUG("GetPhysicalSourceTest {}", physicaSources);
    // Check only for default source
    std::string expect = "{\"default_logical\":\"id:INTEGER value:INTEGER";
    ASSERT_TRUE(physicaSources.compare(0, expect.size() - 1, expect));
}

/**
 * @brief Test getting queryIdAndCatalogEntryMapping works properly
 */
TEST_F(RemoteClientTest, GetQueriesTest) {
    Query query = Query::from("default_logical").sink(NullOutputSinkDescriptor::create());
    int64_t queryId = client->submitQuery(query);
    checkForQueryStart(queryId);
    std::string queries = client->getQueries();
    std::string expect = "[{\"queryId\":";
    ASSERT_TRUE(queries.compare(0, expect.size() - 1, expect));
    ASSERT_TRUE(stopQuery(queryId));
}

/**
 * @brief Test getting queryIdAndCatalogEntryMapping by status works properly
 */
TEST_F(RemoteClientTest, GetQueriesWithStatusTest) {
    Query query = Query::from("default_logical").sink(NullOutputSinkDescriptor::create());
    int64_t queryId = client->submitQuery(query);
    checkForQueryStart(queryId);
    std::string queryStatus = client->getQueryStatus(queryId);

    std::string expect = "[{\"status\":";
    ASSERT_TRUE(queryStatus.compare(0, expect.size() - 1, expect));
    ASSERT_TRUE(stopQuery(queryId));
}

/**
 * @brief Test if retrieving the execution plan works properly
 * @result execution plan is as expected
 */
TEST_F(RemoteClientTest, StopAStoppedQuery) {
    Query query = Query::from("default_logical").sink(NullOutputSinkDescriptor::create());
    int64_t queryId = client->submitQuery(query);
    checkForQueryStart(queryId);
    EXPECT_TRUE(stopQuery(queryId));
    sleep(3);
    EXPECT_TRUE(stopQuery(queryId));
}

/**
  * @brief Test if retrieving the execution plan works properly
  * @result execution plan is as expected
  */
TEST_F(RemoteClientTest, StopInvalidQueryId) { ASSERT_FALSE(stopQuery(21)); }

/**
 * @brief Test if retrieving the execution plan works properly
 * @result execution plan is as expected
 */
TEST_F(RemoteClientTest, DeployInvalidQuery) {
    Query query = Query::from("default_logical");
    try {
        client->submitQuery(query);
        FAIL();
    } catch (Client::ClientException const& e) {
        std::string errorMessage = e.what();
        constexpr auto expected = "does not contain a valid sink operator as root";
        EXPECT_NE(errorMessage.find(expected), std::string::npos);
    } catch (...) {
        // wrong exception
        FAIL();
    }
}

}// namespace NES
