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
#include <iostream>
#include <regex>
#include <string>
#include <API/Query.hpp>
#include <API/QueryAPI.hpp>
#include <API/Windowing.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/TumblingWindow.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

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

bool parseAndCompareQueryPlans(const std::string& antlrQueryString, const Query& internalLogicalQuery)
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
    {
        const auto inputQuery = "SELECT f1 FROM StreamName WHERE f1 == 30 INTO Print"s;
        const auto internalLogicalQuery = Query::from("StreamName").filter(Attribute("f1") == 30).project(Attribute("f1")).sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }

    {
        const auto inputQuery = "select * from StreamName where (f1 == 10 AND f2 != 10) INTO Print"s;
        const auto internalLogicalQuery = Query::from("StreamName").filter(Attribute("f1") == 10 && Attribute("f2") != 10).sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }

    {
        const auto inputQuery = "SELECT * FROM default_logical INTO Print;"s;
        const auto internalLogicalQuery = Query::from("default_logical").sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }
}

TEST_F(AntlrSQLQueryParserTest, simpleJoinTestTumblingWindow)
{
    {
        /// Testing join with two streams that have the same field names
        const auto inputQueryEventTime
            = "select * from (select * from purchases) inner join (select * from tweets) on userId = id window tumbling (ts_field, size 10 sec) INTO PRINT"s;
        const auto queryEventTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("id"))
                                        .window(TumblingWindow::of(EventTime(Attribute("ts_field")), Seconds(10)))
                                        .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));
        const auto inputQueryIngestionTime
            = "select * from (select * from purchases) inner join (select * from tweets) on userId = userId window tumbling (size 10 sec) INTO PRINT"s;
        const auto queryIngestionTime = Query::from("purchases")
                                            .joinWith(Query::from("tweets"))
                                            .where(Attribute("userId") == Attribute("userId"))
                                            .window(TumblingWindow::of(IngestionTime(), Seconds(10)))
                                            .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
    }
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_simpleJoinTestTumblingWindowWithQualifierNames)
{
    /// Testing join with two streams that specify the join field names via the stream name
    const auto inputQueryEventTime
        = "select * from (select * from purchases) inner join (select * from tweets) on purchases.userId = tweets.id window tumbling (timestamp, size 10 ms) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") == Attribute("id"))
                                    .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(10)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select * from purchases) AS purchases inner join (select * from tweets) AS tweets on purchases.userId = tweets.id window tumbling (size 10 ms) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("purchases.userId") == Attribute("tweets.id"))
                                        .window(TumblingWindow::of(IngestionTime(), Milliseconds(10)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, simpleJoinTestSlidingWindows)
{
    {
        /// Testing join with two stream that have different field names and perform the join over a sliding window
        const auto inputQueryEventTime
            = "select * from (select * from purchases) inner join (select * from tweets) on userId = id window sliding (timestamp, size 10 min, advance by 5 ms) INTO PRINT"s;
        const auto queryEventTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("id"))
                                        .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(10), Milliseconds(5)))
                                        .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

        const auto inputQueryIngestionTime
            = "select * from (select * from purchases) inner join (select * from tweets) on userId = id window sliding (size 3 days, advance by 5 hours) INTO PRINT"s;
        const auto queryIngestionTime = Query::from("purchases")
                                            .joinWith(Query::from("tweets"))
                                            .where(Attribute("userId") == Attribute("id"))
                                            .window(SlidingWindow::of(IngestionTime(), Days(3), Hours(5)))
                                            .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
    }
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_simpleJoinTestSlidingWindowsWithQualifierNames)
{
    {
        /// Testing join with two streams that specify the join field names via the stream name and perform the join over a sliding window
        const auto inputQueryEventTime
            = "select * from (select * from purchases) AS purchases inner join (select * from tweets) AS tweets on purchases.userId = tweets.userId window sliding (timestamp, size 10 ms, advance by 5 ms) INTO PRINT"s;
        const auto queryEventTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("userId"))
                                        .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                                        .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

        const auto inputQueryIngestionTime
            = "select * from (select * from purchases) AS purchases inner join (select * from tweets) AS tweets on purchases.userId = tweets.userId window sliding (size 10 ms, advance by 5 ms) INTO PRINT"s;
        const auto queryIngestionTime = Query::from("purchases")
                                            .joinWith(Query::from("tweets"))
                                            .where(Attribute("userId") == Attribute("userId"))
                                            .window(SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(5)))
                                            .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
    }
}


TEST_F(AntlrSQLQueryParserTest, simpleJoinTestSlidingWindowsWithFilter)
{
    {
        /// Testing join with two streams that specify the join field names via the stream name and perform the join over a sliding window
        const auto inputQueryEventTime = "select * from (select * from purchases) inner join (select * from tweets) "
                                         "on userId = userId1234 "
                                         "window sliding (timestamp, size 9876 days, advance by 3 ms) WHERE field > 0 INTO PRINT"s;
        const auto queryEventTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("userId1234"))
                                        .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Days(9876), Milliseconds(3)))
                                        .filter(Attribute("field") > 0)
                                        .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

        const auto inputQueryIngestionTime = "select * from (select * from purchases) inner join (select * from tweets) "
                                             "on userId = userId1234 "
                                             "window sliding (size 10 ms, advance by 5 ms) WHERE field > 1234 INTO PRINT"s;
        const auto queryIngestionTime = Query::from("purchases")
                                            .joinWith(Query::from("tweets"))
                                            .where(Attribute("userId") == Attribute("userId1234"))
                                            .window(SlidingWindow::of(IngestionTime(), Milliseconds(10), Milliseconds(5)))
                                            .filter(Attribute("field") > 1234)
                                            .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
    }
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_simpleJoinTestSlidingWindowsWithFilterWithQualifierNames)
{
    {
        /// Testing join with two streams that specify the join field names via the stream name and perform the join over a sliding window
        const auto inputQueryEventTime = "select * from (select * from purchases) AS purchases inner join (select * from tweets) AS tweets "
                                         "on purchases.userId = tweets.userId "
                                         "window sliding (timestamp, size 9876 days, advance by 3 ms) WHERE field > 0 INTO PRINT"s;
        const auto queryEventTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("userId"))
                                        .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Days(9876), Milliseconds(3)))
                                        .filter(Attribute("field") > 0)
                                        .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

        const auto inputQueryIngestionTime = "select * from (select * from purchases) AS purchases inner join (select * from tweets) AS "
                                             "tweets on purchases.userId = tweets.userId "
                                             "window sliding (size 10 ms, advance by 5 ms) WHERE field > 1234 INTO PRINT"s;
        const auto queryIngestionTime = Query::from("purchases")
                                            .joinWith(Query::from("tweets"))
                                            .where(Attribute("userId") == Attribute("userId"))
                                            .window(SlidingWindow::of(IngestionTime(), Milliseconds(10), Milliseconds(5)))
                                            .filter(Attribute("field") > 1234)
                                            .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
    }
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_simpleJoinTestSlidingWindowsWithFilterAndQualifierName)
{
    {
        /// Testing join with two streams that specify the join field names via the stream name and perform the join over a sliding window
        const auto inputQueryEventTime = "select * from (select * from purchases) AS purchases inner join (select * from tweets) AS "
                                         "tweets2 on purchases.userId = tweets.userId "
                                         "window sliding (timestamp, size 10 ms, advance by 5 ms) WHERE tweets2.field > 0 INTO PRINT"s;
        const auto queryEventTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("userId"))
                                        .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                                        .filter(Attribute("tweets2.field") > 0)
                                        .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

        const auto inputQueryIngestionTime = "select * from (select * from purchases) AS purchases2 inner join (select * from tweets) AS "
                                             "tweets on purchases.userId = tweets.userId "
                                             "window sliding (size 10 ms, advance by 5 ms) WHERE purchases2.field > 1234 INTO PRINT"s;
        const auto queryIngestionTime = Query::from("purchases")
                                            .joinWith(Query::from("tweets"))
                                            .where(Attribute("userId") == Attribute("userId"))
                                            .window(SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(5)))
                                            .filter(Attribute("purchases2.field") > 1234)
                                            .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
    }
}


TEST_F(AntlrSQLQueryParserTest, threeJoinTest)
{
    /// Testing join with two stream that have different field names and perform the join over a sliding window
    const auto inputQueryEventTime
        = "select * from (select * from purchases) inner join (select * from tweets) ON userId = id "
          "window sliding (ts1, size 10 day, advance by 5 ms) inner join (select * from sellers) ON id = sellerId "
          "window tumbling (ts2, size 30 min) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") == Attribute("id"))
                                    .window(SlidingWindow::of(EventTime(Attribute("ts1")), Days(10), Milliseconds(5)))
                                    .joinWith(Query::from("sellers"))
                                    .where(Attribute("id") == Attribute("sellerId"))
                                    .window(TumblingWindow::of(EventTime(Attribute("ts2")), Minutes(30)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select * from purchases) inner join (select * from tweets) ON userId = id "
          "window sliding (size 10 day, advance by 5 ms) inner join (select * from sellers) ON id = sellerId "
          "window sliding (size 30 min, advance by 8 sec) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("id"))
                                        .window(SlidingWindow::of(IngestionTime(), Days(10), Milliseconds(5)))
                                        .joinWith(Query::from("sellers"))
                                        .where(Attribute("id") == Attribute("sellerId"))
                                        .window(SlidingWindow::of(IngestionTime(), Minutes(30), Seconds(8)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_threeJoinTestWithQualifierName)
{
    /// Testing join with two stream that have different field names and perform the join over a sliding window
    const auto inputQueryEventTime = "select * from (select * from purchases) as purchases2 inner join (select * from tweets) as tweets2 "
                                     "ON tweets2.userId = purchases2.id "
                                     "window sliding (ts1, size 10 day, advance by 5 ms) inner join (select * from sellers) as sellers2 ON "
                                     "purchases2.id = sellers2.sellerId "
                                     "window tumbling (ts2, size 30 min) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("tweets2.userId") == Attribute("purchases2.id"))
                                    .window(SlidingWindow::of(EventTime(Attribute("ts1")), Days(10), Milliseconds(5)))
                                    .joinWith(Query::from("sellers"))
                                    .where(Attribute("purchases2.id") == Attribute("sellers2.sellerId"))
                                    .window(TumblingWindow::of(EventTime(Attribute("ts2")), Minutes(30)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime = "select * from (select * from purchases) AS purchases2 inner join (select * from tweets) AS "
                                         "tweets2 ON tweets2.userId = purchases2.id "
                                         "window sliding (size 10 day, advance by 5 ms) inner join (select * from sellers) as sellers2 ON "
                                         "purchases2.id = sellers2.sellerId "
                                         "window sliding (size 30 min, advance by 8 sec) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("tweets2.userId") == Attribute("purchases2.id"))
                                        .window(SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(5)))
                                        .joinWith(Query::from("sellers"))
                                        .where(Attribute("purchases2.id") == Attribute("sellers2.sellerId"))
                                        .window(SlidingWindow::of(IngestionTime(), Minutes(30), Seconds(8)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, threeJoinTestWithMultipleJoinkeyFunctions)
{
    /// Testing join with two stream that have different field names and perform the join over a sliding window
    const auto inputQueryEventTime
        = "select * from (select * from purchases) inner join (select * from tweets) ON userId = id and userId2 >= id2 "
          "window sliding (ts1, size 10 days, advance by 5 ms) inner join (select * from sellers) ON id = sellerId or id2 < sellerId2 "
          "window tumbling (ts2, size 30 min) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") == Attribute("id") && Attribute("userId2") >= Attribute("id2"))
                                    .window(SlidingWindow::of(EventTime(Attribute("ts1")), Days(10), Milliseconds(5)))
                                    .joinWith(Query::from("sellers"))
                                    .where(Attribute("id") == Attribute("sellerId") || Attribute("id2") < Attribute("sellerId2"))
                                    .window(TumblingWindow::of(EventTime(Attribute("ts2")), Minutes(30)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select * from purchases) inner join (select * from tweets) ON userId = id and userId2 >= id2 "
          "window sliding (size 12 days, advance by 5 ms) inner join (select * from sellers) ON id = sellerId or id2 < sellerId2 "
          "window tumbling (size 30 min) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("id") && Attribute("userId2") >= Attribute("id2"))
                                        .window(SlidingWindow::of(IngestionTime(), Days(12), Milliseconds(5)))
                                        .joinWith(Query::from("sellers"))
                                        .where(Attribute("id") == Attribute("sellerId") || Attribute("id2") < Attribute("sellerId2"))
                                        .window(TumblingWindow::of(IngestionTime(), Minutes(30)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_threeJoinTestWithMultipleJoinkeyFunctionsWithQualifierName)
{
    /// Testing join with two stream that have different field names and perform the join over a sliding window
    const auto inputQueryEventTime = "select * from (select * from purchases) as purchases2 inner join (select * from tweets) as tweets2 "
                                     "ON purchases2.userId = tweets2.id and tweets2.userId2 >= tweets2.id2 "
                                     "window sliding (ts1, size 10 days, advance by 5 ms) inner join (select * from sellers) as sellers2 "
                                     "ON tweets2.id = sellers2.sellerId or tweets2.id2 < sellers2.sellerId2 "
                                     "window tumbling (ts2, size 30 min) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") == Attribute("id") && Attribute("userId2") >= Attribute("id2"))
                                    .window(SlidingWindow::of(EventTime(Attribute("ts1")), Days(10), Milliseconds(5)))
                                    .joinWith(Query::from("sellers"))
                                    .where(Attribute("id") == Attribute("sellerId") || Attribute("id2") < Attribute("sellerId2"))
                                    .window(TumblingWindow::of(EventTime(Attribute("ts2")), Minutes(30)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime = "select * from (select * from purchases) as purchases2 inner join (select * from tweets) as "
                                         "tweets2 ON purchases2.userId = tweets2.id and userId2 >= tweets2.id2 "
                                         "window sliding (size 12 days, advance by 5 ms) inner join (select * from sellers) as sellers2 ON "
                                         "tweets2.id = sellers2.sellerId or tweets2.id2 < sellers2.sellerId2 "
                                         "window tumbling (size 30 min) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("id") && Attribute("userId2") >= Attribute("id2"))
                                        .window(SlidingWindow::of(IngestionTime(), Days(12), Milliseconds(5)))
                                        .joinWith(Query::from("sellers"))
                                        .where(Attribute("id") == Attribute("sellerId") || Attribute("id2") < Attribute("sellerId2"))
                                        .window(TumblingWindow::of(IngestionTime(), Minutes(30)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, thetaJoinTest)
{
    /// Testing join with two stream that have different field names and perform the join over a sliding window
    const auto inputQueryEventTime
        = "select * from (select * from purchases) inner join (select * from tweets) on userId >= id window sliding (timestamp, size 10 sec, advance by 5 ms) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") >= Attribute("id"))
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Milliseconds(5)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select * from purchases) inner join (select * from tweets) on userId >= id window sliding (size 10 ms, advance by 5 ms) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") >= Attribute("id"))
                                        .window(SlidingWindow::of(IngestionTime(), Milliseconds(10), Milliseconds(5)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_thetaJoinTestWithQualifierName)
{
    /// Testing join with two streams that specify the join field names via the stream name and perform the join over a sliding window
    const auto inputQueryEventTime
        = "select * from (select * from purchases) inner join (select * from tweets) on purchases.userId < tweets.userId window sliding (timestamp, size 23 days, advance by 5 sec) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") < Attribute("userId"))
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Days(23), Seconds(5)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select * from purchases) inner join (select * from tweets) on purchases.userId < tweets.userId window sliding (size 10 sec, advance by 5 sec) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") < Attribute("userId"))
                                        .window(SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(5)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, thetaJoinTestNegatedJoinExpression)

{
    /// Testing join with two stream that have different field names and perform the join over a sliding window
    const auto inputQueryEventTime
        = "select * from (select * from purchases) inner join (select * from tweets) on !(userId < id) window sliding (timestamp, size 42 ms, advance by 1 ms) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(!(Attribute("userId") < Attribute("id")))
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(42), Milliseconds(1)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select * from purchases) inner join (select * from tweets) on !(userId < id) window sliding (size 42 ms, advance by 1 ms) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(!(Attribute("userId") < Attribute("id")))
                                        .window(SlidingWindow::of(IngestionTime(), Milliseconds(42), Milliseconds(1)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_thetaJoinTestNegatedJoinExpressionWithQualifierName)
{
    /// Testing join with two streams that specify the join field names via the stream name and perform the join over a sliding window
    const auto inputQueryEventTime
        = "select * from (select * from purchases) inner join (select * from tweets) on !(purchases.userId >= tweets.userId) window sliding (timestamp, size 10 ms, advance by 5 ms) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") < Attribute("userId"))
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select * from purchases) inner join (select * from tweets) on !(purchases.userId >= tweets.userId) window sliding (size 10 ms, advance by 5 ms) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") < Attribute("userId"))
                                        .window(SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(5)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, joinTestWithMultipleJoinkeyFunctions)
{
    /// Testing join with two stream that have different field names and perform the join over a sliding window
    const auto inputQueryEventTime
        = "select * from (select * from purchases) inner join (select * from tweets) on !(userId < id) and id >= sellerId window sliding (timestamp, size 10 sec, advance by 5 ms) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where((!(Attribute("userId") < Attribute("id"))) && (Attribute("id") >= Attribute("sellerId")))
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Milliseconds(5)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select * from purchases) inner join (select * from tweets) on !(userId < id) and id >= sellerId window sliding (size 10 ms, advance by 5 ms) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where((!(Attribute("userId") < Attribute("id"))) && (Attribute("id") >= Attribute("sellerId")))
                                        .window(SlidingWindow::of(IngestionTime(), Milliseconds(10), Milliseconds(5)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_joinTestWithMultipleJoinkeyFunctionsWithQualifierNames)
{
    /// Testing join with two streams that specify the join field names via the stream name and perform the join over a sliding window
    const auto inputQueryEventTime
        = "select * from (select * from purchases) inner join (select * from tweets) on !(purchases.userId >= tweets.userId) && purchases.id >= tweets.sellerId window sliding (timestamp, size 10 ms, advance by 5 ms) INTO PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") < Attribute("userId") && Attribute("id") >= Attribute("sellerId"))
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select * from purchases) inner join (select * from tweets) on !(purchases.userId >= tweets.userId) && purchases.id >= tweets.sellerId window sliding (size 10 ms, advance by 5 ms) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") < Attribute("userId") && Attribute("id") >= Attribute("sellerId"))
                                        .window(SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(5)))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}


TEST_F(AntlrSQLQueryParserTest, joinTestWithFilterAndMapAfterJoin)
{
    /// Todo #440: the grammar currently does not support a mixture of '*' and projections, therefore, we opted for using a projection in the test
    const auto inputQueryEventTime
        = "select (id2 + 1) AS userId2 from (select * from (select * from purchases) inner join (select * from tweets) on userId == id "
          "window sliding (timestamp, size 10 sec, advance by 5 ms)) where field >=23 and field2 <=12 into PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") == Attribute("id"))
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Milliseconds(5)))
                                    .filter(Attribute("field") >= 23 && Attribute("field2") <= 12)
                                    .map(Attribute("userId2") = Attribute("id2") + 1)
                                    .project(Attribute("userId2"))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    /// Todo #440: the grammar currently does not support a mixture of '*' and projections, therefore, we opted for using a projection in the test
    const auto inputQueryIngestionTime
        = "select (id2 + 1) AS userId2 from (select * from (select * from purchases) inner join (select * from tweets) on userId == id "
          "window sliding (size 10 sec, advance by 5 ms)) where field >=23 or field2 <=12 into PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("id"))
                                        .window(SlidingWindow::of(IngestionTime(), Seconds(10), Milliseconds(5)))
                                        .filter(Attribute("field") >= 23 || Attribute("field2") <= 12)
                                        .map(Attribute("userId2") = Attribute("id2") + 1)
                                        .project(Attribute("userId2"))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

/// TODO #474: Add support for qualifier names in join functions
TEST_F(AntlrSQLQueryParserTest, DISABLED_joinTestWithFilterAndMapAfterJoinWithQualifierName)
{
    throw NotImplemented("We should do the same as in joinTestWithFilterAndMapAfterJoin but use qualifiernames");
    ASSERT_TRUE(false);
}


/// Todo #447: use the code below as guidance for how to support WINDOWs and JOINs using the antlr sql parser
/// The below tests are the 'validation' of the bachelor's thesis that introduced the antlr SQL parser.
/// The tests mostly fail currently, but they provide valuable documentation for adding operators/functionality in the future.
/*
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
           "select * from purchases inner join tweets on userId = userId window tumbling (timestamp, size 10 sec) INTO PRINT",
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
           "select * from purchases inner join tweets on userId = userId window tumbling (timestamp, size 10 sec) INTO PRINT",
           "select * from purchases inner join tweets on purchases.userId = tweets.userId window tumbling (timestamp, size 10 sec) INTO "
           "PRINT",
           "select count(f2) from StreamName window tumbling (size 10 sec) INTO PRINT",
           "SELECT name, department FROM employees WHERE age > 30 INTO ZMQ (stream_name, localhost, 5555)",
           "select sum(f2) as sum_f2 from StreamName window tumbling (size 10 ms) having sum_f2 > 5 INTO PRINT",
           "select * from purchases inner join tweets on userId = userId window tumbling (timestamp, size 10 sec) INTO PRINT",
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
