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
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
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

class QueryCatalogTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryCatalogTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryCatalogTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("FINISHED ADDING 5 Serialization to topology");
        NES_DEBUG("Setup QueryCatalogTest test case.");
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }
};

TEST_F(QueryCatalogTest, testAddNewQuery) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    queryCatalog->createQueryCatalogEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp, QueryState::REGISTERED);

    //Assert
    auto reg = queryCatalog->getAllQueryEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto allQueryEntries = queryCatalog->getQueryEntriesWithStatus("REGISTERED");
    EXPECT_TRUE(allQueryEntries.size() == 1U);
}

TEST_F(QueryCatalogTest, testAddNewPattern) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A WHERE A.currentSpeed< A.allowedSpeed INTO Print :: testSink ";
    const QueryPlanPtr queryPlan = queryParsingService->createPatternFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->createQueryCatalogEntry(patternString,
                                          queryPlan,
                                          Optimizer::PlacementStrategy::BottomUp,
                                          QueryState::REGISTERED);

    //Assert
    auto reg = queryCatalog->getAllQueryEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueryEntriesWithStatus("REGISTERED");
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogTest, testAddNewQueryAndStop) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    queryCatalog->createQueryCatalogEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp, QueryState::REGISTERED);

    //Assert
    auto reg = queryCatalog->getAllQueryEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto registeredQueries = queryCatalog->getQueryEntriesWithStatus("REGISTERED");
    EXPECT_TRUE(registeredQueries.size() == 1U);

    //SendStop request
    queryCatalog->updateQueryStatus(queryId, QueryState::MARKED_FOR_HARD_STOP, "");

    //Assert
    registeredQueries = queryCatalog->getQueryEntriesWithStatus("REGISTERED");
    EXPECT_TRUE(registeredQueries.empty());
    auto queriesMarkedForStop = queryCatalog->getQueryEntriesWithStatus("MARKED_FOR_HARD_STOP");
    EXPECT_TRUE(queriesMarkedForStop.size() == 1U);
}

TEST_F(QueryCatalogTest, testPrintQuery) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    queryCatalog->createQueryCatalogEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp, QueryState::REGISTERED);

    //Assert
    auto reg = queryCatalog->getAllQueryEntries();
    EXPECT_TRUE(reg.size() == 1U);
}

TEST_F(QueryCatalogTest, testAddNewQueryWithMultipleSinks) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->createQueryCatalogEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp, QueryState::REGISTERED);

    //Assert
    auto reg = queryCatalog->getAllQueryEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueryEntriesWithStatus("REGISTERED");
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogTest, getAllQueriesWithoutAnyQueryRegistration) {
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto allRegisteredQueries = queryCatalog->getAllQueryEntries();
    EXPECT_TRUE(allRegisteredQueries.empty());
}

TEST_F(QueryCatalogTest, getAllQueriesAfterQueryRegistration) {

    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();

    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    queryCatalog->createQueryCatalogEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp, QueryState::REGISTERED);

    //Assert
    auto allRegisteredQueries = queryCatalog->getAllQueryEntries();
    EXPECT_EQ(allRegisteredQueries.size(), 1U);
}

TEST_F(QueryCatalogTest, getAllRunningQueries) {

    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();

    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    const QueryPlanPtr queryPlan = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    queryCatalog->createQueryCatalogEntry(queryString, queryPlan, Optimizer::PlacementStrategy::BottomUp, QueryState::REGISTERED);
    queryCatalog->updateQueryStatus(queryId, QueryState::RUNNING, "");

    //Assert
    auto queries = queryCatalog->getQueryEntriesWithStatus("RUNNING");
    EXPECT_EQ(queries.size(), 1U);
}

TEST_F(QueryCatalogTest, throInvalidArgumentExceptionWhenQueryStatusIsUnknown) {
    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    EXPECT_THROW(queryCatalog->getQueryEntriesWithStatus("something_random"), InvalidArgumentException);
}
