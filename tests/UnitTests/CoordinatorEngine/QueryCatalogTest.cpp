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

#include "gtest/gtest.h"
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Services/QueryParsingService.hpp>
#include <API/Query.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>

using namespace NES;
using namespace std;

std::string ip = "127.0.0.1";
std::string host = "localhost";
uint16_t publish_port = 4711;

class QueryCatalogTest : public testing::Test {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup QueryCatalogTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("QueryCatalogTest.log", NES::LOG_DEBUG);
        NES_DEBUG("FINISHED ADDING 5 Serialization to topology");
        std::cout << "Setup QueryCatalogTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down QueryCatalogTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down QueryCatalogTest test class." << std::endl; }
};

TEST_F(QueryCatalogTest, testAddNewQuery) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->addNewQuery(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    std::map<uint64_t, QueryCatalogEntryPtr> run = queryCatalog->getQueries(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogTest, testAddNewQueryAndStop) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();

    auto catalogEntry = queryCatalog->addNewQuery(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    std::map<uint64_t, QueryCatalogEntryPtr> registeredQueries = queryCatalog->getQueries(QueryStatus::Registered);
    EXPECT_TRUE(registeredQueries.size() == 1U);

    //SendStop request
    catalogEntry = queryCatalog->stopQuery(queryId);

    //Assert
    EXPECT_TRUE(catalogEntry);
    registeredQueries = queryCatalog->getQueries(QueryStatus::Registered);
    EXPECT_TRUE(registeredQueries.empty());
    std::map<uint64_t, QueryCatalogEntryPtr> queriesMarkedForStop = queryCatalog->getQueries(QueryStatus::MarkedForStop);
    EXPECT_TRUE(queriesMarkedForStop.size() == 1U);
}

TEST_F(QueryCatalogTest, testPrintQuery) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->addNewQuery(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    std::string ret = queryCatalog->printQueries();
    cout << "ret=" << ret << endl;
}

TEST_F(QueryCatalogTest, getAllQueriesWithoutAnyQueryRegistration) {
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    std::map<uint64_t, std::string> allRegisteredQueries = queryCatalog->getAllQueries();
    EXPECT_TRUE(allRegisteredQueries.empty());
}

TEST_F(QueryCatalogTest, getAllQueriesAfterQueryRegistration) {

    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    auto catalogEntry = queryCatalog->addNewQuery(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, std::string> allRegisteredQueries = queryCatalog->getAllQueries();
    EXPECT_EQ(allRegisteredQueries.size(), 1U);
    EXPECT_TRUE(allRegisteredQueries.find(queryId) != allRegisteredQueries.end());
}

TEST_F(QueryCatalogTest, getAllRunningQueries) {

    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    queryCatalog->addNewQuery(queryString, queryPlan, "BottomUp");
    queryCatalog->markQueryAs(queryId, QueryStatus::Running);

    //Assert
    std::map<uint64_t, std::string> queries = queryCatalog->getQueriesWithStatus("running");
    EXPECT_EQ(queries.size(), 1U);
    EXPECT_TRUE(queries.find(queryId) != queries.end());
}

TEST_F(QueryCatalogTest, throInvalidArgumentExceptionWhenQueryStatusIsUnknown) {
    try {
        //Prepare
        QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
        std::map<uint64_t, std::string> queries = queryCatalog->getQueriesWithStatus("something_random");
        NES_WARNING("Should have thrown invalid argument exception");
        //Assert for Failure
        FAIL();
    } catch (InvalidArgumentException& e) {
        //Assert for Success
        NES_INFO(e.what());
        SUCCEED();
    }
}