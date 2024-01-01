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

TEST(SQLParsingServiceTest, selectionTest) {
    std::shared_ptr<QueryParsingService> SQLParsingService;
    std::string inputQuery;
    QueryPlanPtr actualPlan;
    double totalTime = 0.0;


    std::cout << "-------------------------Selection-------------------------\n";

    // Test case for simple selection
    inputQuery = "select f1 from StreamName where f1 > 10*3 INTO PRINT";
    auto start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    Query query = Query::from("StreamName").project(Attribute("f1")).filter(Attribute("f1")>30).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select * from StreamName where (f1 > 10 AND f2 < 10) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    std::cout << "Time taken for all Selection Queries: " << totalTime/2 << " ms" << std::endl;

    query = Query::from("StreamName").filter(Attribute("f1")>10 && Attribute("f2")<10).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

}

TEST(SQLParsingServiceTest, projectionTest) {
    std::string inputQuery;
    QueryPlanPtr actualPlan;

    std::shared_ptr<QueryParsingService> SQLParsingService;


    std::cout << "-------------------------Projection-------------------------\n";

    // Test case for simple projection
    inputQuery = "select * from StreamName INTO PRINT;";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName").sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    // Test case for projection with alias
    inputQuery = "select * from StreamName as sn INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").as("sn").sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    // Test case for projection of specific fields
    inputQuery = "select f1,f2 from StreamName INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").project(Attribute("f1"),Attribute("f2")).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    // Test case for projection with rename
    inputQuery = "select column1 as c1, column2 as c2 from StreamName INTO PRINT;";
    QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query=Query::from("StreamName").project(Attribute("column1").as("c1"),Attribute("column2").as("c2")).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(sqlPlan), queryPlanToString(query.getQueryPlan()));


}

TEST(SQLParsingServiceTest, mergeTest) {
    std::string inputQuery;
    QueryPlanPtr actualPlan;

    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::cout << "-------------------------Merge-------------------------\n";

    // Test case for simple merge
    inputQuery = "select f1 from cars union select f1 from bikes INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("cars").project(Attribute("f1")).unionWith(Query::from("bikes").project(Attribute("f1"))).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    // Test case for merge with multiple unions
    inputQuery = "select f1 from cars union select f1 from bikes union select f1 from autos INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("cars").project(Attribute("f1")).unionWith(Query::from("bikes").project(Attribute("f1"))).unionWith(Query::from("autos").project(Attribute("f1"))).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}


TEST(SQLParsingServiceTest, mapTest) {
    std::string inputQuery;
    QueryPlanPtr actualPlan;

    std::shared_ptr<QueryParsingService> SQLParsingService;


    std::cout << "-------------------------Map-------------------------\n";

    // Test case for simple map
    inputQuery = "select f1*f2 as newfield from StreamName INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName").map(Attribute("newfield")=Attribute("f1")*Attribute("f2")).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    // Test case for parenthesis expression for map
    inputQuery = "select (f1*f2)/(f3+f4) as f5 from StreamName INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").map(Attribute("f5")=(Attribute("f1")*Attribute("f2"))/(Attribute("f3")+Attribute("f4"))).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    // Test case for complex map expression
    inputQuery = "select f1*f2/f3+f4 as f5 from StreamName INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").map(Attribute("f5")=Attribute("f1")*Attribute("f2")/Attribute("f3")+Attribute("f4")).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    // Test case for two maps
    inputQuery = "select f1*f2 as newfield, f1+5 as newfield2 from StreamName INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").map(Attribute("newfield")=Attribute("f1")*Attribute("f2")).map(Attribute("newfield2")=Attribute("f1")+5.0).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    // Test case for projection and map together
    inputQuery = "select f1*f2 as newfield, f1+5 as newfield2, f1 as f from StreamName INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").project(Attribute("f1").as("f")).map(Attribute("newfield")=Attribute("f1")*Attribute("f2")).map(Attribute("newfield2")=Attribute("f1")+5.0).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    // Test case for when alias is not provided
    inputQuery = "select f1*f2 from StreamName INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").map(Attribute("mapField0")=Attribute("f1")*Attribute("f2")).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select f1*f2, f1+5 from StreamName INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").map(Attribute("mapField0")=Attribute("f1")*Attribute("f2")).map(Attribute("mapField1")=Attribute("f1")+5.0).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}
TEST(SQLParsingServiceTest, globalWindowTest) {
    std::shared_ptr<QueryParsingService> SQLParsingService;
    std::string inputQuery;
    QueryPlanPtr actualPlan;


    std::cout << "-------------------------Global Window-------------------------\n";

    inputQuery = "select sum(f2) from StreamName window tumbling (size 10 ms) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10))).apply(Sum(Attribute("f2"))).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select sum(f2), max(f3) from StreamName as sn window tumbling (timestamp, size 10 ms) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").as("sn")
                              .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10)))
                              .apply(Sum(Attribute("f2")), Max(Attribute("f3")))
                              .sink(PrintSinkDescriptor::create());


    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

TEST(SQLParsingServiceTest, thresholdWIndowTest){
    std::shared_ptr<QueryParsingService> SQLParsingService;
    std::string inputQuery = "select sum(f2) from StreamName WINDOW THRESHOLD (f1>2, 10) INTO PRINT";
    QueryPlanPtr actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName")
                      .window(ThresholdWindow::of(Attribute("f1")>2, 10))
                      .apply(Sum(Attribute("f2")))
                      .sink(PrintSinkDescriptor::create());


    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

TEST(SQLParsingServiceTest, timeBasedTumblingWindowTest) {
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery = "select sum(f2) from StreamName group by f2 window tumbling (timestamp, size 10 sec) INTO PRINT";
    QueryPlanPtr actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).byKey(Attribute("f2")).apply(Sum(Attribute("f2"))).sink(PrintSinkDescriptor::create());

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select sum(f2) from StreamName group by f2 window tumbling (size 10 sec) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).byKey(Attribute("f2")).apply(Sum(Attribute("f2"))).sink(PrintSinkDescriptor::create());

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

TEST(SQLParsingServiceTest, timeBasedSlidingWindowTest) {
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery = "select sum(f2) from StreamName group by f2 window sliding (timestamp, size 10 ms, advance by 5 ms) INTO PRINT";
    QueryPlanPtr actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName")
                      .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10), Milliseconds(5)))
                      .byKey(Attribute("f2"))
                      .apply(Sum(Attribute("f2")))
                      .sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select * from StreamName group by f2 window sliding (size 10 ms, advance by 5 ms) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    //ToDO Could .apply be empty?
    query = Query::from("StreamName").window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10), Milliseconds(5))).byKey(Attribute("f2")).apply().sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

TEST(SQLParsingServiceTest, multipleAggregationFunctionsWindowTest) {
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery = "select sum(f2), min(f2) from StreamName window tumbling (size 10 sec) INTO PRINT";
    QueryPlanPtr actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                      .apply(Sum(Attribute("f2")), Min(Attribute("f2")))
                      .sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select sum(f2), min(f2), max(f2) from StreamName window tumbling (timestamp, size 10 sec) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                      .apply(Sum(Attribute("f2")),
                             Min(Attribute("f2")),
                             Max(Attribute("f2")))
                      .sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

TEST(SQLWindowServiceTest, aggregationAliasWindowTest) {
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery = "select max(f2) as max_f2 from StreamName group by f2 window tumbling (timestamp, size 10 sec) INTO PRINT";
    QueryPlanPtr actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                      .byKey(Attribute("f2"))
                      .apply(Max(Attribute("f2")))
                      .sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select max(f2) as max_f2, sum(f2) as sum_f2 from StreamName group by f2 window tumbling (timestamp, size 10 sec) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName")
                              .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                              .byKey(Attribute("f2"))
                              .apply(Max(Attribute("f2")), Sum(Attribute("f2")))
                              .sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}
//ToDo Research correct join syntax
TEST(SQLParsingServiceTest, joinWindowTestDerErste) {
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery = "select * from purchases inner join tweets on user_id = user_id window tumbling (timestamp, size 10 sec) INTO PRINT";
    QueryPlanPtr actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("purchases").joinWith(Query::from("tweets"), Attribute("user_id"), Attribute("user_id"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select * from purchases inner join tweets on purchases.user_id = tweets.user_id window tumbling (timestamp, size 10 sec) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("purchases").joinWith(Query::from("tweets"), Attribute("purchases.user_id"), Attribute("tweets.user_id"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select * from purchases as p inner join tweets as t on p.user_id = t.user_id window tumbling (timestamp, size 10 sec) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("purchases").as("p").joinWith(Query::from("tweets").as("t"), Attribute("p.user_id"), Attribute("t.user_id"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

}
TEST(SQLParsingServiceTest, CountAggregationFunctionWindowTest) {
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery;
    QueryPlanPtr actualPlan;

    std::cout << "-------------------------Count Aggregation Function Window-------------------------\n";

    inputQuery = "select count(f2) from StreamName window tumbling (size 10 sec) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).byKey(Attribute("f2")).apply(Count()).sink(PrintSinkDescriptor::create());

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select count(f2) from StreamName window tumbling (size 10 sec) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).byKey(Attribute("f2")).apply(Count()).sink(PrintSinkDescriptor::create());

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select count(*) from StreamName window tumbling (size 10 sec) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                      .apply(Count())
                      .sink(PrintSinkDescriptor::create());

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select count() from StreamName window tumbling (size 10 sec) INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                      .apply(Count())
                      .sink(PrintSinkDescriptor::create());

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}
TEST(SQLParsingServiceTest, havingClauseTest) {
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery;
    QueryPlanPtr actualPlan;

    inputQuery = "select sum(f2) as sum_f2 from StreamName window tumbling (size 10 ms) having sum_f2 > 5 INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10))).apply(Sum(Attribute("f2"))->as(Attribute("sum_f2"))).filter(Attribute("sum_f2")>5).sink(PrintSinkDescriptor::create());;

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}
TEST(SQLParsingServiceTest, joinWindowTest) {
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery =
        "select * from purchases inner join tweets on user_id = user_id window tumbling (timestamp, size 10 sec) INTO PRINT";
    QueryPlanPtr actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto query = Query::from("purchases").joinWith(Query::from("tweets"),Attribute("user_id"),Attribute("user_id"),TumblingWindow::of(EventTime(Attribute("timestamp")),Seconds(10))).sink(PrintSinkDescriptor::create());

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}
TEST(SQLParsingServiceTest,subQueryTest){
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery = "SELECT * FROM (SELECT f1 FROM subStream WHERE f1 > 1) WHERE f1 < 5 INTO PRINT";
    QueryPlanPtr actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto query = Query::from("subStream").project(Attribute("f1")).filter(Attribute("f1")>1).filter(Attribute("f1")<5).sink(PrintSinkDescriptor::create());
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

TEST(SQLParsingServiceTest,perfomanceTest1){
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::vector<std::string> selectClauses = {
        "select max(f2) as max_f2, sum(f2) as sum_f2 from StreamName group by f2 window tumbling (timestamp, size 10 sec) INTO PRINT",
        "select * from purchases inner join tweets on user_id = user_id window tumbling (timestamp, size 10 sec) INTO PRINT",
        "SELECT name FROM employees;",
        "SELECT department, COUNT(*) AS total FROM employees GROUP BY department window tumbling (timestamp, size 10 sec) INTO PRINT;",
        "select f1 from StreamName where f1 > 10*3 INTO PRINT",
        "select * from StreamName where (f1 > 10 AND f2 < 10) INTO PRINT",
        "select * from StreamName as sn INTO PRINT",
        "select f1,f2 from StreamName INTO PRINT",
        "select column1 as c1, column2 as c2 from StreamName INTO PRINT;",
        "select f1 from cars union select f1 from bikes INTO PRINT",
        "select f1 from cars union select f1 from bikes union select f1 from autos INTO PRINT",
        "select f1*f2 as newfield from StreamName INTO PRINT",
        "select (f1*f2)/(f3+f4) as f5 from StreamName INTO PRINT",
        "select f1*f2/f3+f4 as f5 from StreamName INTO PRINT",
        "select f1*f2 as newfield, f1+5 as newfield2, f1 as f from StreamName INTO PRINT",
        "select sum(f2), max(f3) from StreamName as sn window tumbling (timestamp, size 10 ms) INTO PRINT",
        "select sum(f2) from StreamName WINDOW THRESHOLD (f1>2, 10) INTO PRINT",
        "select sum(f2) from StreamName group by f2 window sliding (timestamp, size 10 ms, advance by 5 ms) INTO PRINT",
        "SELECT name FROM employees;",
        "select * from purchases inner join tweets on user_id = user_id window tumbling (timestamp, size 10 sec) INTO PRINT",
        "select * from purchases inner join tweets on purchases.user_id = tweets.user_id window tumbling (timestamp, size 10 sec) INTO PRINT",
        "select count(f2) from StreamName window tumbling (size 10 sec) INTO PRINT",
        "SELECT name, department FROM employees WHERE age > 30 INTO ZMQ (stream_name, localhost, 5555)",
        "select sum(f2) as sum_f2 from StreamName window tumbling (size 10 ms) having sum_f2 > 5 INTO PRINT",
        "select * from purchases inner join tweets on user_id = user_id window tumbling (timestamp, size 10 sec) INTO PRINT",
        "SELECT * FROM (SELECT f1 FROM subStream WHERE f1 > 1) WHERE f1 < 5 INTO PRINT"
    };
    double totalTime = 0.0;

    for (const auto& SQLString : selectClauses) {
        auto start = std::chrono::high_resolution_clock::now();
        QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(SQLString);
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << "Time taken for \"" << SQLString << "\": " << duration.count() << " ms" << std::endl;
        totalTime += duration.count();
    }
    std::cout << "Average time for all queries: " << totalTime/selectClauses.size() << " ms. " <<  "Amount of queries:" << selectClauses.size() << std::endl;

}