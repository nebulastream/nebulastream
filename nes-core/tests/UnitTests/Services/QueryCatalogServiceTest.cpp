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
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace NES;
using namespace std;
using namespace Catalogs::Query;

std::string ip = "127.0.0.1";
std::string host = "localhost";

class QueryCatalogServiceTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryCatalogServiceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryCatalogServiceTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("FINISHED ADDING 5 Serialization to topology");
        NES_DEBUG("Setup QueryCatalogServiceTest test case.");
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }
};

TEST_F(QueryCatalogServiceTest, testAddNewQuery) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto catalogEntry = queryCatalogService->createNewEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp);

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalogService->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    std::map<uint64_t, QueryCatalogEntryPtr> run = queryCatalogService->getAllEntriesInStatus("REGISTERED");
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPattern) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A WHERE A.currentSpeed< A.allowedSpeed INTO Print :: testSink ";
    const QueryPlanPtr queryPlan = queryParsingService->createPatternFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, Optimizer::PlacementStrategy::BottomUp);

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryState::REGISTERED);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewQueryAndStop) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);

    auto catalogEntry = queryCatalogService->createNewEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp);

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalogService->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    std::map<uint64_t, QueryCatalogEntryPtr> registeredQueries = queryCatalogService->getAllEntriesInStatus("REGISTERED");
    EXPECT_TRUE(registeredQueries.size() == 1U);

    //SendStop request
    bool success = queryCatalogService->checkAndMarkForHardStop(queryId);

    //Assert
    EXPECT_TRUE(success);
    registeredQueries = queryCatalogService->getAllEntriesInStatus("REGISTERED");
    EXPECT_TRUE(registeredQueries.empty());
    std::map<uint64_t, QueryCatalogEntryPtr> queriesMarkedForStop =
        queryCatalogService->getAllEntriesInStatus("MARKED_FOR_HARD_STOP");
    EXPECT_TRUE(queriesMarkedForStop.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testPrintQuery) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryServiceCatalog = std::make_shared<QueryCatalogService>(queryCatalog);

    auto catalogEntry = queryServiceCatalog->createNewEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp);

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryServiceCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewQueryWithMultipleSinks) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp);

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryState::REGISTERED);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, getAllQueriesWithoutAnyQueryRegistration) {
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);

    auto allRegisteredQueries = queryCatalogService->getAllQueryCatalogEntries();
    EXPECT_TRUE(allRegisteredQueries.empty());
}

TEST_F(QueryCatalogServiceTest, getAllQueriesAfterQueryRegistration) {

    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);

    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp);

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, std::string> allRegisteredQueries = queryCatalog->getAllQueries();
    EXPECT_EQ(allRegisteredQueries.size(), 1U);
    EXPECT_TRUE(allRegisteredQueries.find(queryId) != allRegisteredQueries.end());
}

TEST_F(QueryCatalogServiceTest, getAllRunningQueries) {

    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);

    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    queryCatalogService->createNewEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp);
    queryCatalogService->updateQueryStatus(queryId, QueryState::RUNNING, "");

    //Assert
    std::map<uint64_t, std::string> queries = queryCatalogService->getAllQueriesInStatus("RUNNING");
    EXPECT_EQ(queries.size(), 1U);
    EXPECT_TRUE(queries.find(queryId) != queries.end());
}

TEST_F(QueryCatalogServiceTest, throInvalidArgumentExceptionWhenQueryStatusIsUnknown) {

    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);

    EXPECT_THROW(queryCatalogService->getAllQueriesInStatus("something_random"), InvalidArgumentException);
}