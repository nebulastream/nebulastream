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
#include <cstdint>
#include <iostream>
#include <memory>
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
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

using namespace NES;
using namespace std::string_literals;

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
    const std::shared_ptr<QueryPlan> antlrQueryParsed = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(antlrQueryString);
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
        const std::string antlrQueryString = "SELECT (id * INT32(3)) AS new_id FROM stream INTO File";
        const auto internalLogicalQuery
            = Query::from("stream").map(Attribute("new_id") = Attribute("id") * 3).project(Attribute("new_id")).sink("File");
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

TEST_F(AntlrSQLQueryParserTest, multipleFieldsProjectionTest)
{
    {
        const auto inputQuery = "SELECT "
                                "(i8 == INT8(1)) and ((i16 == INT16(1)) and (i32 == INT32(1))) AS a, "
                                "(i32 == INT32(1)) and ((i64 == INT64(1)) and (u8 == UINT8(2))) AS b, "
                                "(u8 == UINT8(2)) and ((u16 == UINT16(1)) and (u32 == UINT32(2))) AS c, "
                                "(u8 == UINT8(2)) and ((u16 == UINT16(1)) and (u32 == UINT32(2))) AS d, "
                                "(i8 == INT8(1)) and not ((i16 == INT16(1)) and (i32 == INT32(1))) AS e, "
                                "(i32 == INT32(1)) and not ((i64 == INT64(1)) and (u8 == UINT8(2))) AS f, "
                                "(u8 == UINT8(2)) and not ((u16 == UINT16(1)) and (u32 == UINT32(2))) AS g, "
                                "(u8 == UINT8(2)) and not ((u16 == UINT16(1)) and (u32 == UINT32(2))) AS h "
                                "FROM stream INTO Print"s;
        const auto internalLogicalQuery
            = Query::from("stream")
                  .map(
                      Attribute("a") = Attribute("i8") == static_cast<int8_t>(1)
                          && ((Attribute("i16") == static_cast<int16_t>(1)) && (Attribute("i32") == static_cast<int32_t>(1))))
                  .map(
                      Attribute("b") = Attribute("i32") == static_cast<int32_t>(1)
                          && ((Attribute("i64") == static_cast<int64_t>(1)) && (Attribute("u8") == static_cast<uint8_t>(2))))
                  .map(
                      Attribute("c") = Attribute("u8") == static_cast<uint8_t>(2)
                          && ((Attribute("u16") == static_cast<uint16_t>(1)) && (Attribute("u32") == static_cast<uint32_t>(2))))
                  .map(
                      Attribute("d") = Attribute("u8") == static_cast<uint8_t>(2)
                          && ((Attribute("u16") == static_cast<uint16_t>(1)) && (Attribute("u32") == static_cast<uint32_t>(2))))
                  .map(
                      Attribute("e") = Attribute("i8") == static_cast<int8_t>(1)
                          && !(Attribute("i16") == static_cast<int16_t>(1) && Attribute("i32") == static_cast<int32_t>(1)))
                  .map(
                      Attribute("f") = Attribute("i32") == static_cast<int32_t>(1)
                          && !(Attribute("i64") == static_cast<int64_t>(1) && Attribute("u8") == static_cast<uint8_t>(2)))
                  .map(
                      Attribute("g") = Attribute("u8") == static_cast<uint8_t>(2)
                          && !(Attribute("u16") == static_cast<uint16_t>(1) && Attribute("u32") == static_cast<uint32_t>(2)))
                  .map(
                      Attribute("h") = Attribute("u8") == static_cast<uint8_t>(2)
                          && !(Attribute("u16") == static_cast<uint16_t>(1) && Attribute("u32") == static_cast<uint32_t>(2)))
                  .project(
                      Attribute("a"),
                      Attribute("b"),
                      Attribute("c"),
                      Attribute("d"),
                      Attribute("e"),
                      Attribute("f"),
                      Attribute("g"),
                      Attribute("h"))
                  .sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }
}

TEST_F(AntlrSQLQueryParserTest, selectionTest)
{
    {
        const auto inputQuery = "SELECT f1 FROM StreamName WHERE f1 == INT32(30) INTO Print"s;
        const auto internalLogicalQuery = Query::from("StreamName").selection(Attribute("f1") == 30).project(Attribute("f1")).sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }

    {
        const auto inputQuery = "SELECT f1 + INT32(5) AS f1Plus5 FROM StreamName WHERE f1 == INT32(30) INTO Print"s;
        const auto internalLogicalQuery = Query::from("StreamName")
                                              .selection(Attribute("f1") == 30)
                                              .map(Attribute("f1Plus5") = Attribute("f1") + 5)
                                              .project(Attribute("f1Plus5"))
                                              .sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }

    {
        const auto inputQuery = "select * from StreamName where (f1 == INT32(10) AND f2 != INT32(10)) INTO Print"s;
        const auto internalLogicalQuery = Query::from("StreamName").selection(Attribute("f1") == 10 && Attribute("f2") != 10).sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }

    {
        const auto inputQuery = "SELECT * FROM default_logical INTO Print;"s;
        const auto internalLogicalQuery = Query::from("default_logical").sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }

    {
        const auto inputQuery = "SELECT f1 + INT32(1) AS f1_new FROM StreamName INTO Print"s;
        const auto internalLogicalQuery
            = Query::from("StreamName").map(Attribute("f1_new") = Attribute("f1") + 1).project(Attribute("f1_new")).sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }

    {
        /// Checks implicit creation of field names if the user does not specify 'AS'
        const auto inputQuery = "SELECT f1 + INT32(1) FROM StreamName WHERE f1 == INT32(30) INTO Print"s;
        const auto internalLogicalQuery = Query::from("StreamName")
                                              .selection(Attribute("f1") == 30)
                                              .map(Attribute("f1_0") = Attribute("f1") + 1)
                                              .project(Attribute("f1_0"))
                                              .sink("Print");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQuery, internalLogicalQuery));
    }

    {
        /// Check that a simply rename works
        const auto inputQuery = "SELECT f1 AS f1_new FROM StreamName INTO Print"s;
        const auto internalLogicalQuery
            = Query::from("StreamName").map(Attribute("f1_new") = Attribute("f1")).project(Attribute("f1_new")).sink("Print");
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
            = "select * from (select * from purchases) inner join (select * from tweets) on userId = id window sliding (timestamp, size 10 minute, advance by 5 ms) INTO PRINT"s;
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
                                         "window sliding (timestamp, size 9876 days, advance by 3 ms) WHERE field > INT32(0) INTO PRINT"s;
        const auto queryEventTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("userId1234"))
                                        .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Days(9876), Milliseconds(3)))
                                        .selection(Attribute("field") > 0)
                                        .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

        const auto inputQueryIngestionTime = "select * from (select * from purchases) inner join (select * from tweets) "
                                             "on userId = userId1234 "
                                             "window sliding (size 10 ms, advance by 5 ms) WHERE field > INT32(1234) INTO PRINT"s;
        const auto queryIngestionTime = Query::from("purchases")
                                            .joinWith(Query::from("tweets"))
                                            .where(Attribute("userId") == Attribute("userId1234"))
                                            .window(SlidingWindow::of(IngestionTime(), Milliseconds(10), Milliseconds(5)))
                                            .selection(Attribute("field") > 1234)
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
                                        .selection(Attribute("field") > 0)
                                        .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

        const auto inputQueryIngestionTime = "select * from (select * from purchases) AS purchases inner join (select * from tweets) AS "
                                             "tweets on purchases.userId = tweets.userId "
                                             "window sliding (size 10 ms, advance by 5 ms) WHERE field > 1234 INTO PRINT"s;
        const auto queryIngestionTime = Query::from("purchases")
                                            .joinWith(Query::from("tweets"))
                                            .where(Attribute("userId") == Attribute("userId"))
                                            .window(SlidingWindow::of(IngestionTime(), Milliseconds(10), Milliseconds(5)))
                                            .selection(Attribute("field") > 1234)
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
                                        .selection(Attribute("tweets2.field") > 0)
                                        .sink("PRINT");
        EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

        const auto inputQueryIngestionTime = "select * from (select * from purchases) AS purchases2 inner join (select * from tweets) AS "
                                             "tweets on purchases.userId = tweets.userId "
                                             "window sliding (size 10 ms, advance by 5 ms) WHERE purchases2.field > 1234 INTO PRINT"s;
        const auto queryIngestionTime = Query::from("purchases")
                                            .joinWith(Query::from("tweets"))
                                            .where(Attribute("userId") == Attribute("userId"))
                                            .window(SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(5)))
                                            .selection(Attribute("purchases2.field") > 1234)
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
          "window tumbling (ts2, size 30 minute) INTO PRINT"s;
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
          "window sliding (size 30 minute, advance by 8 sec) INTO PRINT"s;
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
                                     "window tumbling (ts2, size 30 minute) INTO PRINT"s;
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
                                         "window sliding (size 30 minute, advance by 8 sec) INTO PRINT"s;
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
          "window tumbling (ts2, size 30 minute) INTO PRINT"s;
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
          "window tumbling (size 30 minute) INTO PRINT"s;
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
                                     "window tumbling (ts2, size 30 minute) INTO PRINT"s;
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
                                         "window tumbling (size 30 minute) INTO PRINT"s;
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
        = "select (id2 + INT32(1)) AS userId2 from (select * from (select * from purchases) inner join (select * from tweets) on userId == "
          "id "
          "window sliding (timestamp, size 10 sec, advance by 5 ms)) where field >= INT32(23) and field2 <= INT32(12) into PRINT"s;
    const auto queryEventTime = Query::from("purchases")
                                    .joinWith(Query::from("tweets"))
                                    .where(Attribute("userId") == Attribute("id"))
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Milliseconds(5)))
                                    .selection(Attribute("field") >= 23 && Attribute("field2") <= 12)
                                    .map(Attribute("userId2") = Attribute("id2") + 1)
                                    .project(Attribute("userId2"))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    /// Todo #440: the grammar currently does not support a mixture of '*' and projections, therefore, we opted for using a projection in the test
    const auto inputQueryIngestionTime
        = "select (id2 + INT32(1)) AS userId2 from (select * from (select * from purchases) inner join (select * from tweets) on userId == "
          "id "
          "window sliding (size 10 sec, advance by 5 ms)) where field >= INT32(23) or field2 <= INT32(12) into PRINT"s;
    const auto queryIngestionTime = Query::from("purchases")
                                        .joinWith(Query::from("tweets"))
                                        .where(Attribute("userId") == Attribute("id"))
                                        .window(SlidingWindow::of(IngestionTime(), Seconds(10), Milliseconds(5)))
                                        .selection(Attribute("field") >= 23 || Attribute("field2") <= 12)
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

TEST_F(AntlrSQLQueryParserTest, globalWindowTest)
{
    const auto inputQueryEventTime
        = "select sum(id) as average_id from StreamName window sliding (timestamp, size 10 sec, advance by 5 ms) INTO PRINT"s;
    const auto queryEventTime = Query::from("StreamName")
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Milliseconds(5)))
                                    .apply(Sum(Attribute("id"))->as(Attribute("average_id")))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime = "select min(id) from StreamName window sliding (size 1234 sec, advance by 9876 ms) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("StreamName")
                                        .window(SlidingWindow::of(IngestionTime(), Seconds(1234), Milliseconds(9876)))
                                        .apply(Min(Attribute("id")))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, keyedWindowTest)
{
    const auto inputQueryEventTime
        = "select avg(id) as average_id from StreamName GROUP BY grouped_field window sliding (timestamp, size 10 sec, advance by 5 ms) INTO PRINT"s;
    const auto queryEventTime = Query::from("StreamName")
                                    .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Milliseconds(5)))
                                    .byKey(Attribute("grouped_field"))
                                    .apply(Avg(Attribute("id"))->as(Attribute("average_id")))
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select MAX(id) from StreamName GROUP BY grouped_field window sliding (size 10 sec, advance by 5 ms) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("StreamName")
                                        .window(SlidingWindow::of(IngestionTime(), Seconds(10), Milliseconds(5)))
                                        .byKey(Attribute("grouped_field"))
                                        .apply(Max(Attribute("id")))
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, multipleKeyedWindowTestWithHaving)
{
    const auto inputQueryEventTime
        = "select avg(id) as average_id from StreamName GROUP BY (grouped_field_1, grouped_field_2, grouped_field_3)  window tumbling (timestamp, size 10 days) HAVING average_id > INT32(24) and average_id < INT32(100) INTO PRINT"s;
    const auto queryEventTime = Query::from("StreamName")
                                    .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Days(10)))
                                    .byKey(Attribute("grouped_field_1"), Attribute("grouped_field_2"), Attribute("grouped_field_3"))
                                    .apply(Avg(Attribute("id"))->as(Attribute("average_id")))
                                    .selection(Attribute("average_id") > 24 && Attribute("average_id") < 100)
                                    .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select avg(id) as average_id from StreamName GROUP BY (grouped_field_1, grouped_field_2, grouped_field_3) window sliding (size 10 sec, advance by 5 ms) HAVING INT32(24) < average_id and average_id < INT32(100) INTO PRINT"s;
    const auto queryIngestionTime = Query::from("StreamName")
                                        .window(SlidingWindow::of(IngestionTime(), Seconds(10), Milliseconds(5)))
                                        .byKey(Attribute("grouped_field_1"), Attribute("grouped_field_2"), Attribute("grouped_field_3"))
                                        .apply(Avg(Attribute("id"))->as(Attribute("average_id")))
                                        .selection(24 < Attribute("average_id") && Attribute("average_id") < 100)
                                        .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, multipleKeyedMultipleAggFunctionsWindowTestWithHaving)
{
    const auto inputQueryEventTime
        = "select avg(id) as average_id, sum(id2), max(id3) as max_id from StreamName GROUP BY (grouped_field_1, grouped_field_2, grouped_field_3) window sliding (timestamp, size 10 sec, advance by 5 ms) HAVING average_id > INT32(24) and max_id < INT32(100) INTO PRINT"s;
    const auto queryEventTime
        = Query::from("StreamName")
              .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Milliseconds(5)))
              .byKey(Attribute("grouped_field_1"), Attribute("grouped_field_2"), Attribute("grouped_field_3"))
              .apply(
                  Avg(Attribute("id"))->as(Attribute("average_id")), Sum(Attribute("id2")), Max(Attribute("id3"))->as(Attribute("max_id")))
              .selection(Attribute("average_id") > 24 && Attribute("max_id") < 100)
              .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select avg(id) as average_id, sum(id2), max(id3) as max_id from StreamName GROUP BY (grouped_field_1, grouped_field_2, grouped_field_3) window sliding (size 10 sec, advance by 5 ms) HAVING average_id > INT32(24) and max_id < INT32(100) INTO PRINT"s;
    const auto queryIngestionTime
        = Query::from("StreamName")
              .window(SlidingWindow::of(IngestionTime(), Seconds(10), Milliseconds(5)))
              .byKey(Attribute("grouped_field_1"), Attribute("grouped_field_2"), Attribute("grouped_field_3"))
              .apply(
                  Avg(Attribute("id"))->as(Attribute("average_id")), Sum(Attribute("id2")), Max(Attribute("id3"))->as(Attribute("max_id")))
              .selection(Attribute("average_id") > 24 && Attribute("max_id") < 100)
              .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, multipleKeyedMultipleAggFunctionsWindowTestWithHavingBeforeJoin)
{
    const auto inputQueryEventTime
        = "select * from (select avg(id) as average_id, sum(id2), max(id3) as max_id from StreamName GROUP BY "
          "(grouped_field_1, grouped_field_2, grouped_field_3) window sliding (timestamp, size 10 sec, advance "
          "by 5 ms) HAVING average_id > INT32(24) and max_id < INT32(100))"
          " INNER JOIN (select * from stream2) ON average_id = id2 window tumbling (timestamp, size 1 day) INTO PRINT"s;
    const auto queryEventTime
        = Query::from("StreamName")
              .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Milliseconds(5)))
              .byKey(Attribute("grouped_field_1"), Attribute("grouped_field_2"), Attribute("grouped_field_3"))
              .apply(
                  Avg(Attribute("id"))->as(Attribute("average_id")), Sum(Attribute("id2")), Max(Attribute("id3"))->as(Attribute("max_id")))
              .selection(Attribute("average_id") > 24 && Attribute("max_id") < 100)
              .joinWith(Query::from("stream2"))
              .where(Attribute("average_id") == Attribute("id2"))
              .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Days(1)))
              .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select * from (select avg(id) as average_id, sum(id2), max(id3) as max_id from StreamName GROUP BY "
          "(grouped_field_1, grouped_field_2, grouped_field_3) window sliding (size 10 sec, advance "
          "by 5 ms) HAVING average_id > INT32(24) and max_id < INT32(100))"
          " INNER JOIN (select * from stream2) ON average_id = id2 window tumbling (timestamp, size 12 day) INTO PRINT"s;
    const auto queryIngestionTime
        = Query::from("StreamName")
              .window(SlidingWindow::of(IngestionTime(), Seconds(10), Milliseconds(5)))
              .byKey(Attribute("grouped_field_1"), Attribute("grouped_field_2"), Attribute("grouped_field_3"))
              .apply(
                  Avg(Attribute("id"))->as(Attribute("average_id")), Sum(Attribute("id2")), Max(Attribute("id3"))->as(Attribute("max_id")))
              .selection(Attribute("average_id") > 24 && Attribute("max_id") < 100)
              .joinWith(Query::from("stream2"))
              .where(Attribute("average_id") == Attribute("id2"))
              .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Days(12)))
              .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, multipleKeyedMultipleAggFunctionsWindowTestWithHavingAfterJoin)
{
    const auto inputQueryEventTime
        = "select avg(id) as average_id, sum(id2), max(id3) as max_id from (select * from (select * from stream1) INNER JOIN "
          "(select * from stream2) ON id = id2 window tumbling(timestamp, size 1 hour))"
          " GROUP BY (grouped_field_1, grouped_field_2, grouped_field_3) window sliding (timestamp, size 10 sec, advance by 5 ms) HAVING average_id > INT32(24) and max_id < INT32(123) INTO PRINT"s;
    const auto queryEventTime
        = Query::from("stream1")
              .joinWith(Query::from("stream2"))
              .where(Attribute("id") == Attribute("id2"))
              .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Hours(1)))
              .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Milliseconds(5)))
              .byKey(Attribute("grouped_field_1"), Attribute("grouped_field_2"), Attribute("grouped_field_3"))
              .apply(
                  Avg(Attribute("id"))->as(Attribute("average_id")), Sum(Attribute("id2")), Max(Attribute("id3"))->as(Attribute("max_id")))
              .selection(Attribute("average_id") > 24 && Attribute("max_id") < 123)
              .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryEventTime, queryEventTime));

    const auto inputQueryIngestionTime
        = "select avg(id) as average_id, sum(id2), max(id3) as max_id from (select * from (select * from stream1) INNER JOIN "
          "(select * from stream2) ON id = id2 window tumbling(timestamp, size 1 hour))"
          " GROUP BY (grouped_field_1, grouped_field_2, grouped_field_3) window sliding (size 10 sec, advance by 5 ms) HAVING average_id > INT32(24) and max_id >= INT32(456) INTO PRINT"s;
    const auto queryIngestionTime
        = Query::from("stream1")
              .joinWith(Query::from("stream2"))
              .where(Attribute("id") == Attribute("id2"))
              .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Hours(1)))
              .window(SlidingWindow::of(IngestionTime(), Seconds(10), Milliseconds(5)))
              .byKey(Attribute("grouped_field_1"), Attribute("grouped_field_2"), Attribute("grouped_field_3"))
              .apply(
                  Avg(Attribute("id"))->as(Attribute("average_id")), Sum(Attribute("id2")), Max(Attribute("id3"))->as(Attribute("max_id")))
              .selection(Attribute("average_id") > 24 && Attribute("max_id") >= 456)
              .sink("PRINT");
    EXPECT_TRUE(parseAndCompareQueryPlans(inputQueryIngestionTime, queryIngestionTime));
}

TEST_F(AntlrSQLQueryParserTest, failProjectJoinKeyword)
{
    const auto inputQuery = "SELECT JOIN FROM stream"s;
    EXPECT_ANY_THROW(AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(inputQuery));
}
