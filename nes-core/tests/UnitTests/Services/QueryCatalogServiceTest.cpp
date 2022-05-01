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

#include "gtest/gtest.h"
#include <API/Query.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>

using namespace NES;
using namespace std;
using namespace Catalogs::Query;

std::string ip = "127.0.0.1";
std::string host = "localhost";

class QueryCatalogServiceTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup QueryCatalogServiceTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("QueryCatalogServiceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("FINISHED ADDING 5 Serialization to topology");
        std::cout << "Setup QueryCatalogServiceTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down QueryCatalogServiceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down QueryCatalogServiceTest test class." << std::endl; }
};

TEST_F(QueryCatalogServiceTest, testAddNewPattern) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A INTO Print :: testSink ";
    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQuery) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternMultipleSinksSmall) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A INTO Print :: testSink, Print :: testSink2, Print :: testSink3  ";
    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryMultipleSinksSmall) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternMultipleSinks) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A INTO Print :: testSink, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3  ";
    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryMultipleSinks) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternTimes1) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A[2]) FROM default_logical AS A INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryTimes1) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()).times(2,2); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternTimes2) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A*) FROM default_logical AS A INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryTimes2) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()).times(0,99999999999); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternTimes3) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A+) FROM default_logical AS A INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryTimes3) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()).times(1,99999999999); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternOperator) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A OR B) FROM default_logical AS A, default_logical AS B INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryOperator) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").orWith(Query::from("default_logical")).sink(PrintSinkDescriptor::create());)";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternFilter) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A WHERE A.currentSpeed< A.allowedSpeed INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryFilter) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("currentSpeed") < Attribute("AllowedSpeed")).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternTwoFilters) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A WHERE A.currentSpeed< A.allowedSpeed && A.value < A.random INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");


    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryTwoFilters) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("currentSpeed") < Attribute("AllowedSpeed")).filter(Attribute("value") < Attribute("random")).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}


TEST_F(QueryCatalogServiceTest, testAddNewPatternMap) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A RETURN ce := [name=TU] INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryMap) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").map(Attribute("name") ="TU").sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternMultipleMapSmall) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A RETURN ce := [name=TU, type=random, department=DIMA ] INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryMultipleMapSmall) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternMultipleMap) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A RETURN ce := [name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA ] INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryMultipleMap) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternMultipleMapMultipleSinks) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A RETURN ce := [name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA ] INTO Print :: testSink, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3, Print :: testSink2, Print :: testSink3  ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryMultipleMapMultipleSinks) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternTimesAndMap) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A[10]) FROM default_logical AS A RETURN ce := [name=TU] INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryTimesAndMap) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").times(10,10).map(Attribute("name") ="TU").sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternTimesAndFilter) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A[10]) FROM default_logical AS A WHERE A.currentSpeed< A.allowedSpeed INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryTimesAndFilter) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").times(10,10).filter(Attribute("currentSpeed") < Attribute("AllowedSpeed")).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternOperatorAndFilter) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A AND B) FROM default_logical AS A, default_logical AS B  WHERE A.currentSpeed< A.allowedSpeed INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryOperatorAndFilter) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").orWith(Query::from("default_logical")).filter(Attribute("currentSpeed") < Attribute("AllowedSpeed")).sink(PrintSinkDescriptor::create());)";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternOperatorMapAndFilter) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A AND B) FROM default_logical AS A, default_logical AS B  WHERE A.currentSpeed< A.allowedSpeed RETURN ce := [name=TU] INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryOperatorMapAndFilter) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").orWith(Query::from("default_logical")).filter(Attribute("currentSpeed") < Attribute("AllowedSpeed")).map(Attribute("name") ="TU").sink(PrintSinkDescriptor::create());)";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto catalogEntry = queryCatalogService->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternOperatorTimesMapAndFilter) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A[2] OR B*) FROM default_logical AS A, default_logical AS B  WHERE A.currentSpeed< A.allowedSpeed RETURN ce := [name=TU] INTO Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryOperatorTimesMapAndFIlter) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").times(2,2)
        .orWith(
            Query::from("default_logical")
            .times(0,99999999999)
            .filter(Attribute("currentSpeed") < Attribute("AllowedSpeed"))
            .map(Attribute("name") ="TU")
            .sink(PrintSinkDescriptor::create()))
            ;)";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternOperatorTimesMapAndFilterMultipleSinks) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A[2] OR B*) FROM default_logical AS A, default_logical AS B  WHERE A.currentSpeed< A.allowedSpeed RETURN ce := [name=TU] INTO Print :: testSink, Print :: testSink, Print :: testSink, Print :: testSink, Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryOperatorTimesMapAndFIlterMultipleSInks) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").times(2,2).orWith(Query::from("default_logical")).times(0,99999999999).filter(Attribute("currentSpeed") < Attribute("AllowedSpeed")).map(Attribute("name") ="TU").sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create());)";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewPatternOperatorTimesMultipleMapAndFilterMultipleSinks) {

    //Prepare
    std::string patternString =
        "PATTERN test:= (A[2] OR B*) FROM default_logical AS A, default_logical AS B  WHERE A.currentSpeed< A.allowedSpeed RETURN ce := [name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA, name=TU, type=random, department=DIMA] INTO Print :: testSink, Print :: testSink, Print :: testSink, Print :: testSink, Print :: testSink ";    QueryPtr query = queryParsingService->createQueryFromCodeString(patternString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(patternString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueriesWithStatus(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}
TEST_F(QueryCatalogServiceTest, testAddNewQueryOperatorTimesMultipleMapAndFIlterMultipleSInks) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").times(2,2).orWith(Query::from("default_logical")).times(0,99999999999).filter(Attribute("currentSpeed") < Attribute("AllowedSpeed")).map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").map(Attribute("name") ="TU").map(Attribute("type") ="random").map(Attribute("department") ="DIMA").sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create()).sink(PrintSinkDescriptor::create());)";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
    auto run = queryCatalog->getQueryCatalogEntries(NES::QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testAddNewQueryAndStop) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);

    auto catalogEntry = queryCatalogService->createNewEntry(queryString, queryPlan, "BottomUp");

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
        queryCatalogService->getAllEntriesInStatus("MARKED-FOR-HARD-STOP");
    EXPECT_TRUE(queriesMarkedForStop.size() == 1U);
}

TEST_F(QueryCatalogServiceTest, testPrintQuery) {

    //Prepare
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryServiceCatalog = std::make_shared<QueryCatalogService>(queryCatalog);

    auto catalogEntry = queryServiceCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<uint64_t, QueryCatalogEntryPtr> reg = queryServiceCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1U);
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
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    auto catalogEntry = queryCatalog->createNewEntry(queryString, queryPlan, "BottomUp");

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
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    queryCatalogService->createNewEntry(queryString, queryPlan, "BottomUp");
    queryCatalogService->updateQueryStatus(queryId, QueryStatus::Running, "");

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