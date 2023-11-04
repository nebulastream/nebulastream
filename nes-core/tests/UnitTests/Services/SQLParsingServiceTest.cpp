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
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <climits>
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

using namespace NES;
/*
 * This test checks for the correctness of the SQL queries created by the NebulaSQL Parsing Service.
 */
class SQLParsingServiceTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before a test is executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryPlanTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryPlanTest test case.");
    }
};

std::string queryPlanToString(const QueryPlanPtr queryPlan) {
    std::regex r2("[0-9]");
    std::regex r1("  ");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r1, "");
    queryPlanStr = std::regex_replace(queryPlanStr, r2, "");
    return queryPlanStr;
}

TEST(SQLParsingServiceTest, simpleSQL) {
    //SQL string as received from the NES UI and create query plan from parsing service
    std::string SQLString =
        "SELECT * FROM default_logical INTO PRINT;";
    //        "SELECT * FROM default_logical WHERE default_logical.currentSpeed < default_logical.allowedSpeed INTO PRINT;";
    std::shared_ptr<QueryParsingService> SQLParsingService;
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(SQLString);
    // expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr source =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(source);
    //LogicalOperatorNodePtr filter = LogicalOperatorFactory::createFilterOperator(ExpressionNodePtr(LessExpressionNode::create(NES::Attribute("currentSpeed").getExpressionNode(),NES::Attribute("allowedSpeed").getExpressionNode())));
    //queryPlan->appendOperatorAsNewRoot(filter);
    LogicalOperatorNodePtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);
    std::cout << queryPlanToString(sqlPlan) << "/n";

    //comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(sqlPlan));
}

TEST(SQLParsingServiceTest, projectionTest1) {
    std::string inputQuery = "select * from StreamName INTO PRINT;";
    std::shared_ptr<QueryParsingService> SQLParsingService;
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr source =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("StreamName"));
    queryPlan->appendOperatorAsNewRoot(source);
    LogicalOperatorNodePtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);



    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(sqlPlan));

}

TEST(SQLParsingServiceTest, projectionTest2) {
    std::string inputQuery = "select * from StreamName as sn INTO PRINT;";
    std::shared_ptr<QueryParsingService> SQLParsingService;
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName").as("sn").sink(PrintSinkDescriptor::create());

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(sqlPlan));
}
TEST(SQLParsingServiceTest, projectionTest3) {
    std::string inputQuery = "select f1,f2 from StreamName INTO PRINT;";
    std::shared_ptr<QueryParsingService> SQLParsingService;
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query=Query::from("StreamName").project(Attribute("f1"),Attribute("f2")).sink(PrintSinkDescriptor::create());

    EXPECT_EQ(queryPlanToString(sqlPlan), queryPlanToString(query.getQueryPlan()));
}
TEST(SQLParsingServiceTest, projectionTest4) {
    std::string inputQuery = "select column1 as c1, column2 as c2 from StreamName INTO PRINT;";
    std::shared_ptr<QueryParsingService> SQLParsingService;
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    QueryPlanPtr queryPlan = QueryPlan::create();



    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(sqlPlan));
}

TEST(SQLParsingServiceTest, selectTest1) {
    std::string inputQuery = "select * from StreamName where f1 > 10 INTO PRINT;";
    std::shared_ptr<QueryParsingService> SQLParsingService;
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    QueryPlanPtr queryPlan = QueryPlan::create();



    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(sqlPlan));
}
/*
TEST(SQLParsingServiceTest, selectTest2) {
    std::string inputQuery = "select * from StreamName where f1 > 10.5 and f2 < 10 INTO PRINT;";
    std::shared_ptr<QueryParsingService> SQLParsingService;
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    QueryPlanPtr queryPlan = QueryPlan::create();



    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(sqlPlan));
}
TEST(SQLParsingServiceTest, mergeTest1) {
    std::string inputQuery = "select f1 from cars union select f1 from bikes INTO PRINT;";
    std::shared_ptr<QueryParsingService> SQLParsingService;
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    QueryPlanPtr queryPlan = QueryPlan::create();



    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(sqlPlan));
}
TEST(SQLParsingServiceTest, mergeTest2) {
    std::string inputQuery = "select f1 from cars union select f1 from bikes union select f1 from autos INTO PRINT;";
    std::shared_ptr<QueryParsingService> SQLParsingService;
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    QueryPlanPtr queryPlan = QueryPlan::create();



    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(sqlPlan));

}

*/