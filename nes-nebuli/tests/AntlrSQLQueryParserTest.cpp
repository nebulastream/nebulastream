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
#include <climits>
#include <iostream>
#include <regex>
#include <API/Query.hpp>
#include <API/QueryAPI.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>

using namespace NES;

/// This test checks for the correctness of the SQL queries created by the Antlr SQL Parsing Service.
class AntlrSQLQueryParserTest : public Testing::BaseUnitTest
{
public:
    /* Will be called before a test is executed. */
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("AntlrSQLQueryParserTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AntlrSQLQueryParserTest test case.");
    }
};

std::string queryPlanToString(const QueryPlanPtr queryPlan)
{
    std::regex r2("[0-9]");
    std::regex r1("  ");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r1, "");
    queryPlanStr = std::regex_replace(queryPlanStr, r2, "");
    return queryPlanStr;
}

bool parseAndCompareQueryPlans(const std::string antlrQueryString, const Query& internalLogicalQuery)
{
    const QueryPlanPtr antlrQueryParsed = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(antlrQueryString);
    NES_DEBUG("\n{} vs. \n{}", antlrQueryParsed->toString(), internalLogicalQuery.getQueryPlan()->toString());
    return antlrQueryParsed->compare(internalLogicalQuery.getQueryPlan());
}

TEST_F(AntlrSQLQueryParserTest, projectionAndMapTests)
{
    /// Query::from("window").project(Attribute("id")).sink("File")
    /// @note: SELECT (field * 3) AS new_filed will always lead to a projection first, followed by a map. Thus, the query
    /// 'Query::from("window").map(Attribute("new_id").project(Attribute("new_id"))' is diffuclt/impossible to create using the SQL syntax.
    /// Given that a projection first should be more efficient, this seems to be ok for now.
    {
        const std::string antlrQueryString = "SELECT (id*3) AS new_id FROM window INTO File";
        const auto internalLogicalQuery
            = Query::from("window").map(Attribute("new_id") = Attribute("id") * 3).project(Attribute("new_id")).sink("File");
        EXPECT_TRUE(parseAndCompareQueryPlans(antlrQueryString, internalLogicalQuery));
    }
    /// Todo #440: the grammar currently does not support a mixture of '*' and projections.
    ///     in Flink, a 'SELECT id*3 AS id, * FROM ....' query would return a schma like: 'id10, id, ...'
    ///     in the internalLogical, functional API, we would replace the 'id' attribute and therefore return a schema like: 'id, ...'
    ///     the antlr parser supports neither of the above versions. Still, we can explicitly name the attributes we want.
    /// Query::from("window").map(Attribute("id")).sink("File")
    /// {
    ///     const std::string antlrQueryString = "SELECT *, (id*3) AS id FROM window INTO File";
    ///     const auto internalLogicalQuery = Query::from("window").map(Attribute("id") = Attribute("id") * 3).sink("File");
    ///     EXPECT_TRUE(parseAndCompareQueryPlans(antlrQueryString, internalLogicalQuery));
    /// }
}

TEST_F(AntlrSQLQueryParserTest, selectionTest)
{
    std::string inputQuery = "SELECT f1 FROM StreamName WHERE f1 == 30 INTO Print";
    std::shared_ptr<QueryPlan> actualPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(inputQuery);
    Query query = Query::from("StreamName").filter(Attribute("f1") == 30).project(Attribute("f1")).sink("Print");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select * from StreamName where (f1 == 10 AND f2 != 10) INTO Print";
    actualPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(inputQuery);
    query = Query::from("StreamName").filter(Attribute("f1") == 10 && Attribute("f2") != 10).sink("Print");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    std::string SQLString = "SELECT * FROM default_logical INTO Print;";
    actualPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(SQLString);
    query = Query::from("default_logical").sink("Print");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

/// Todo #447: use the code below as guidance for how to support WINDOWs and JOINs using the antlr sql parser
/// The below tests are the 'validation' of the bachelor's thesis that introduced the antlr SQL parser.
/// The tests mostly fail currently, but they provide valuable documentation for adding operators/functionality in the future.
/*
TEST(SQLParsingServiceTest, projectionTest)
{
    std::string inputQuery;
    QueryPlanPtr actualPlan;
    double totalTime = 0.0;

    std::shared_ptr<QueryParsingService> SQLParsingService;


    std::cout << "-------------------------Projection-------------------------\n";

    /// Test case for simple projection
    inputQuery = "select * from StreamName INTO PRINT;";
    auto start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    Query query = Query::from("StreamName").sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for projection with alias
    inputQuery = "select * from StreamName as sn INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName").sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for projection of specific fields
    inputQuery = "select f1,f2 from StreamName INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName").project(Attribute("f1"), Attribute("f2")).sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for projection of specific fields
    inputQuery = "select f1,f2,f3 from StreamName INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName").project(Attribute("f1"), Attribute("f2"), Attribute("f3")).sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for projection of specific fields
    inputQuery = "select f1,f2,f3, f4 as project4 from StreamName INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .project(Attribute("f1"), Attribute("f2"), Attribute("f3"), Attribute("f4").as("project4"))
                .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for projection with rename
    inputQuery = "select column1 as c1, column2 as c2 from StreamName INTO PRINT;";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    std::cout << "Time taken for all Projection Queries: " << totalTime / 6 << " ms" << std::endl;
    query = Query::from("StreamName").project(Attribute("column1").as("c1"), Attribute("column2").as("c2")).sink("PRINT");
    EXPECT_EQ(queryPlanToString(actualPlan), queryPlanToString(query.getQueryPlan()));
}

TEST(SQLParsingServiceTest, mergeTest)
{
    std::string inputQuery;
    QueryPlanPtr actualPlan;
    double totalTime = 0.0;

    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::cout << "-------------------------Merge-------------------------\n";

    /// Test case for simple merge
    inputQuery = "select f1 from cars union select f1 from bikes INTO PRINT";
    auto start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    Query query = Query::from("cars").project(Attribute("f1")).unionWith(Query::from("bikes").project(Attribute("f1"))).sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select f1 as project from cars union select f1 from bikes INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query
        = Query::from("cars").project(Attribute("f1").as("project")).unionWith(Query::from("bikes").project(Attribute("f1"))).sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select f1 as project from cars where project > 5 union select f1 from bikes INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("cars")
                .project(Attribute("f1").as("project"))
                .filter(Attribute("f1") > 1)
                .unionWith(Query::from("bikes").project(Attribute("f1")))
                .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for merge with multiple unions
    inputQuery = "select f1 from cars union select f1 from bikes union select f1 from autos INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("cars")
                .project(Attribute("f1"))
                .unionWith(Query::from("bikes").project(Attribute("f1")))
                .unionWith(Query::from("autos").project(Attribute("f1")))
                .sink("PRINT");
    std::cout << "Time taken for all Union Queries: " << totalTime / 4 << " ms" << std::endl;
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}


TEST(SQLParsingServiceTest, mapTest)
{
    std::string inputQuery;
    QueryPlanPtr actualPlan;
    double totalTime = 0.0;

    std::shared_ptr<QueryParsingService> SQLParsingService;


    std::cout << "-------------------------Map-------------------------\n";

    /// Test case for simple map
    inputQuery = "select f1*f2 as newfield from StreamName INTO PRINT";
    auto start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    Query query = Query::from("StreamName").map(Attribute("newfield") = Attribute("f1") * Attribute("f2")).sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for parenthesis expression for map
    inputQuery = "select (f1*f2)/(f3+f4) as f5 from StreamName INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .map(Attribute("f5") = (Attribute("f1") * Attribute("f2")) / (Attribute("f3") + Attribute("f4")))
                .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for complex map expression
    inputQuery = "select f1*f2/f3+f4 as f5 from StreamName INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .map(Attribute("f5") = Attribute("f1") * Attribute("f2") / Attribute("f3") + Attribute("f4"))
                .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for two maps
    inputQuery = "select f1*f2 as newfield, f1+5 as newfield2 from StreamName INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .map(Attribute("newfield") = Attribute("f1") * Attribute("f2"))
                .map(Attribute("newfield2") = Attribute("f1") + 5.0)
                .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for projection and map together
    inputQuery = "select f1*f2 as newfield, f1+5 as newfield2, f1 as f from StreamName INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .project(Attribute("f1").as("f"))
                .map(Attribute("newfield") = Attribute("f1") * Attribute("f2"))
                .map(Attribute("newfield2") = Attribute("f1") + 5.0)
                .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    /// Test case for when alias is not provided
    inputQuery = "select f1*f2 from StreamName INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName").map(Attribute("mapField0") = Attribute("f1") * Attribute("f2")).sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select f1*f2, f1+5 from StreamName INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .map(Attribute("mapField0") = Attribute("f1") * Attribute("f2"))
                .map(Attribute("mapField1") = Attribute("f1") + 5.0)
                .sink("PRINT");
    std::cout << "Time taken for all Union Queries: " << totalTime / 7 << " ms" << std::endl;
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}
TEST(SQLParsingServiceTest, globalWindowTest)
{
    std::shared_ptr<QueryParsingService> SQLParsingService;
    std::string inputQuery;
    QueryPlanPtr actualPlan;
    double totalTime = 0.0;


    std::cout << "-------------------------Global Window-------------------------\n";

    inputQuery = "select sum(f2) from StreamName window tumbling (size 10 ms) INTO PRINT";
    auto start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    Query query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10)))
                      .apply(Sum(Attribute("f2")))
                      .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select sum(f2), max(f3) from StreamName as sn window tumbling (timestamp, size 10 ms) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10)))
                .apply(Sum(Attribute("f2")), Max(Attribute("f3")))
                .sink("PRINT");


    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select sum(f2) from StreamName WINDOW THRESHOLD (f1>2, 10) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName").window(ThresholdWindow::of(Attribute("f1") > 2, 10)).apply(Sum(Attribute("f2"))).sink("PRINT");


    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select sum(f2) from StreamName group by f2 window tumbling (timestamp, size 10 sec) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                .byKey(Attribute("f2"))
                .apply(Sum(Attribute("f2")))
                .sink("PRINT");

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select sum(f2) from StreamName group by f2 window tumbling (size 10 sec) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                .byKey(Attribute("f2"))
                .apply(Sum(Attribute("f2")))
                .sink("PRINT");

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select sum(f2) from StreamName group by f2 window sliding (timestamp, size 10 ms, advance by 5 ms) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10), Milliseconds(5)))
                .byKey(Attribute("f2"))
                .apply(Sum(Attribute("f2")))
                .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select * from StreamName group by f2 window sliding (size 10 ms, advance by 5 ms) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10), Milliseconds(5)))
                .byKey(Attribute("f2"))
                .apply()
                .sink("PRINT");
    std::cout << "Time taken for all Global Window Queries: " << totalTime / 7 << " ms" << std::endl;

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

TEST(SQLParsingServiceTest, multipleAggregationFunctionsWindowTest)
{
    std::shared_ptr<QueryParsingService> SQLParsingService;
    double totalTime = 0.0;
    QueryPlanPtr actualPlan;

    std::cout << "-------------------------Multiple Aggregation-------------------------\n";

    std::string inputQuery = "select sum(f2), min(f2) from StreamName window tumbling (size 10 sec) INTO PRINT";
    auto start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    Query query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                      .apply(Sum(Attribute("f2")), Min(Attribute("f2")))
                      .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select sum(f2), min(f2), max(f2) from StreamName window tumbling (timestamp, size 10 sec) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                .apply(Sum(Attribute("f2")), Min(Attribute("f2")), Max(Attribute("f2")))
                .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select max(f2) as max_f2 from StreamName group by f2 window tumbling (timestamp, size 10 sec) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                .byKey(Attribute("f2"))
                .apply(Max(Attribute("f2")))
                .sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery
        = "select max(f2) as max_f2, sum(f2) as sum_f2 from StreamName group by f2 window tumbling (timestamp, size 10 sec) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                .byKey(Attribute("f2"))
                .apply(Max(Attribute("f2")), Sum(Attribute("f2")))
                .sink("PRINT");
    std::cout << "Time taken for all Global Window Queries: " << totalTime / 4 << " ms" << std::endl;
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}


TEST(SQLParsingServiceTest, CountAggregationFunctionWindowTest)
{
    std::shared_ptr<QueryParsingService> SQLParsingService;
    double totalTime = 0.0;
    QueryPlanPtr actualPlan;

    std::cout << "-------------------------Count Aggregation Function Window-------------------------\n";

    std::string inputQuery = "select count(f2) from StreamName window tumbling (size 10 sec) INTO PRINT";
    auto start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    Query query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                      .byKey(Attribute("f2"))
                      .apply(Count())
                      .sink("PRINT");

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select count(f2) from StreamName window tumbling (size 10 sec) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query = Query::from("StreamName")
                .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                .byKey(Attribute("f2"))
                .apply(Count())
                .sink("PRINT");

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select count(*) from StreamName window tumbling (size 10 sec) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query
        = Query::from("StreamName").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).apply(Count()).sink("PRINT");

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));

    inputQuery = "select count() from StreamName window tumbling (size 10 sec) INTO PRINT";
    start = std::chrono::high_resolution_clock::now();
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    totalTime += duration.count();
    std::cout << "Time taken for \"" << inputQuery << "\": " << duration.count() << " ms" << std::endl;
    query
        = Query::from("StreamName").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10))).apply(Count()).sink("PRINT");
    std::cout << "Time taken for all Count Aggregation Function Window Queries: " << totalTime / 4 << " ms" << std::endl;
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}
TEST(SQLParsingServiceTest, havingClauseTest)
{
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery;
    QueryPlanPtr actualPlan;

    inputQuery = "select sum(f2) as sum_f2 from StreamName window tumbling (size 10 ms) having sum_f2 > 5 INTO PRINT";
    actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    Query query = Query::from("StreamName")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10)))
                      .apply(Sum(Attribute("f2"))->as(Attribute("sum_f2")))
                      .filter(Attribute("sum_f2") > 5)
                      .sink("PRINT");
    ;

    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

TEST(SQLParsingServiceTest, subQueryTest)
{
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::string inputQuery = "SELECT * FROM (SELECT f1 FROM subStream WHERE f1 > 1) WHERE f1 < 5 INTO PRINT";
    QueryPlanPtr actualPlan = SQLParsingService->createQueryFromSQL(inputQuery);
    auto query = Query::from("subStream").project(Attribute("f1")).filter(Attribute("f1") > 1).filter(Attribute("f1") < 5).sink("PRINT");
    EXPECT_EQ(queryPlanToString(query.getQueryPlan()), queryPlanToString(actualPlan));
}

TEST(SQLParsingServiceTest, perfomanceTest1)
{
    std::shared_ptr<QueryParsingService> SQLParsingService;

    std::vector<std::string> selectClauses
        = {"select max(f2) as max_f2, sum(f2) as sum_f2 from StreamName group by f2 window tumbling (timestamp, size 10 sec) INTO PRINT",
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
           "select * from purchases inner join tweets on purchases.user_id = tweets.user_id window tumbling (timestamp, size 10 sec) INTO "
           "PRINT",
           "select count(f2) from StreamName window tumbling (size 10 sec) INTO PRINT",
           "SELECT name, department FROM employees WHERE age > 30 INTO ZMQ (stream_name, localhost, 5555)",
           "select sum(f2) as sum_f2 from StreamName window tumbling (size 10 ms) having sum_f2 > 5 INTO PRINT",
           "select * from purchases inner join tweets on user_id = user_id window tumbling (timestamp, size 10 sec) INTO PRINT",
           "SELECT * FROM (SELECT f1 FROM subStream WHERE f1 > 1) WHERE f1 < 5 INTO PRINT"};
    double totalTime = 0.0;
    std::cout << "-------------------------Mixed Queries-------------------------\n";

    for (const auto& SQLString : selectClauses)
    {
        auto start = std::chrono::high_resolution_clock::now();
        QueryPlanPtr sqlPlan = SQLParsingService->createQueryFromSQL(SQLString);
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        std::cout << "Time taken for \"" << SQLString << "\": " << duration.count() << " ms" << std::endl;
        totalTime += duration.count();
    }
    std::cout << "Average time for all queries: " << totalTime / selectClauses.size() << " ms. "
              << "Amount of queries:" << selectClauses.size() << std::endl;
}
*/
