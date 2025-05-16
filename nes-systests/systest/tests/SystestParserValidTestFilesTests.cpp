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
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
/// Tests if SLT Parser accepts ands parses valid .test files correctly
class SystestParserValidTestFileTest : public Testing::BaseUnitTest
{
public:
    static std::string testFileName;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestParserValidTestFileTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestParserValidTestFileTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestParserValidTestFileTest test class."); }
};

TEST_F(SystestParserValidTestFileTest, ValidTestFile)
{
    const auto* const filename = TEST_DATA_DIR "valid.dummy";

    const std::vector<std::string> expectResult = {{"1,1,1"}, {"1,1,1"}, {"1,1,1"}};
    const SystestParser::SLTSource expectedSLTSource
        = {.name = "e123",
           .fields = {{.type = DataTypeProvider::provideDataType(DataType::Type::UINT32), .name = "id"}},
           .tuples = {"1", "1", "1", "1"}};
    SystestParser::CSVSource expectedCSVSource
        = {.name = "e124",
           .fields
           = {{.type = DataTypeProvider::provideDataType(DataType::Type::INT8), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::UINT8), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::INT16), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::UINT16), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::INT32), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::UINT32), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::INT64), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::FLOAT32), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::FLOAT64), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN), .name = "i"},
              {.type = DataTypeProvider::provideDataType(DataType::Type::CHAR), .name = "i"}},
           .csvFilePath = "xyz.txt"};

    bool queryCallbackCalled = false;
    bool sltSourceCallbackCalled = false;
    bool csvSourceCallbackCalled = false;

    SystestParser parser{};
    parser.registerOnQueryCallback([&](std::string&&, size_t) { queryCallbackCalled = true; });
    parser.registerOnSLTSourceCallback([&](const SystestParser::SLTSource&&) { sltSourceCallbackCalled = true; });
    parser.registerOnCSVSourceCallback([&](const SystestParser::CSVSource&&) { csvSourceCallbackCalled = true; });

    ASSERT_TRUE(parser.loadFile(filename)) << "Failed to load file: " << filename;
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));

    /// Verify that all expected callbacks were called
    ASSERT_TRUE(queryCallbackCalled) << "Query callback was never called";
    ASSERT_TRUE(sltSourceCallbackCalled) << "SLT source callback was never called";
    ASSERT_TRUE(csvSourceCallbackCalled) << "CSV source callback was never called";
    ASSERT_TRUE(systestStarterGlobals.getQueryResultMap().size() != 3) << "Result callback was never called";
}

TEST_F(SystestParserValidTestFileTest, Comments1TestFile)
{
    const auto* const filename = TEST_DATA_DIR "comments.dummy";

    SystestParser::SLTSource expectedSLTSource;
    expectedSLTSource.name = "window";
    expectedSLTSource.fields
        = {{.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "id"},
           {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "value"},
           {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "timestamp"}};
    expectedSLTSource.tuples = {"1,1,1000",   "12,1,1001",  "4,1,1002",   "1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",
                                "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",   "1,6,6000",   "1,7,7000",
                                "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
                                "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"};

    /// Expected queries and results
    const auto* const expectedQuery1 = R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow)";
    const auto* const expectedQuery2 = R"(SELECT * FROM window WHERE id >= UINT64(10) INTO sinkWindow)";
    const auto* const expectedQuery3 = R"(SELECT * FROM window WHERE timestamp <= UINT64(10000) INTO sinkWindow)";
    const auto* const expectedQuery4
        = R"(SELECT * FROM window WHERE timestamp >= UINT64(5000) AND timestamp <= UINT64(15000) INTO sinkWindow)";
    const auto* const expectedQuery5 = R"(SELECT * FROM window WHERE value != UINT64(1) INTO sinkWindow)";

    std::vector<std::string> expectedQueries = {expectedQuery1, expectedQuery2, expectedQuery3, expectedQuery4, expectedQuery5};

    std::vector<std::vector<std::string>> expectedResults
        = {{"1,1,1000", "12,1,1001", "4,1,1002"},
           {"12,1,1001", "11,2,2001", "16,2,2002", "11,3,3001"},
           {"1,1,1000",
            "12,1,1001",
            "4,1,1002",
            "1,2,2000",
            "11,2,2001",
            "16,2,2002",
            "1,3,3000",
            "11,3,3001",
            "1,3,3003",
            "1,3,3200",
            "1,4,4000",
            "1,5,5000",
            "1,6,6000",
            "1,7,7000",
            "1,8,8000",
            "1,9,9000",
            "1,10,10000"},
           {"1,5,5000",
            "1,6,6000",
            "1,7,7000",
            "1,8,8000",
            "1,9,9000",
            "1,10,10000",
            "1,11,11000",
            "1,12,12000",
            "1,13,13000",
            "1,14,14000",
            "1,15,15000"},
           {"1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",   "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",
            "1,6,6000",   "1,7,7000",   "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
            "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"}};

    bool sltSourceCallbackCalled = false;
    bool queryCallbackCalled = false;

    SystestParser parser{};
    parser.registerOnSLTSourceCallback(
        [&](SystestParser::SLTSource&& source)
        {
            sltSourceCallbackCalled = true;
            ASSERT_EQ(source.name, expectedSLTSource.name);
            ASSERT_EQ(source.fields, expectedSLTSource.fields);
            ASSERT_EQ(source.tuples, expectedSLTSource.tuples);
        });

    parser.registerOnQueryCallback(
        [&](const std::string& query, const size_t currentQueryNumberInTest)
        {
            queryCallbackCalled = true;
            /// Query numbers start at QueryId::INITIAL, which is 1
            ASSERT_LT(currentQueryNumberInTest, expectedQueries.size() + 1);
            ASSERT_EQ(query, expectedQueries.at(currentQueryNumberInTest - 1));
        });

    ASSERT_TRUE(parser.loadFile(filename));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
    ASSERT_TRUE(sltSourceCallbackCalled) << "SLT source callback was never called";
    ASSERT_TRUE(queryCallbackCalled) << "Query callback was never called";
    ASSERT_TRUE(systestStarterGlobals.getQueryResultMap().size() == expectedResults.size());
    ASSERT_TRUE(
        std::ranges::all_of(
            expectedResults,
            [&systestStarterGlobals](const auto& expectedResult)
            { return std::ranges::contains(systestStarterGlobals.getQueryResultMap() | std::views::values, expectedResult); }));
}

TEST_F(SystestParserValidTestFileTest, FilterTestFile)
{
    const auto* const filename = TEST_DATA_DIR "filter.dummy";

    SystestParser::SLTSource expectedSLTSource;
    expectedSLTSource.name = "window";
    expectedSLTSource.fields
        = {{.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "id"},
           {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "value"},
           {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "timestamp"}};
    expectedSLTSource.tuples = {"1,1,1000",   "12,1,1001",  "4,1,1002",   "1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",
                                "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",   "1,6,6000",   "1,7,7000",
                                "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
                                "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"};

    std::vector<std::string> expectedQueries
        = {R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow)",

           R"(SELECT * FROM window WHERE id >= UINT64(10) INTO sinkWindow)",

           R"(SELECT * FROM window WHERE timestamp <= UINT64(10000) INTO sinkWindow)",

           R"(SELECT * FROM window WHERE timestamp >= UINT64(5000) AND timestamp <= UINT64(15000) INTO sinkWindow)",

           R"(SELECT * FROM window WHERE value != UINT64(1) INTO sinkWindow)",

           R"(SELECT * FROM window WHERE id = id - UINT64(1) INTO sinkWindow)"};

    std::vector<std::vector<std::string>> expectedResults
        = {{"1,1,1000", "12,1,1001", "4,1,1002"},
           {"12,1,1001", "11,2,2001", "16,2,2002", "11,3,3001"},
           {"1,1,1000",
            "12,1,1001",
            "4,1,1002",
            "1,2,2000",
            "11,2,2001",
            "16,2,2002",
            "1,3,3000",
            "11,3,3001",
            "1,3,3003",
            "1,3,3200",
            "1,4,4000",
            "1,5,5000",
            "1,6,6000",
            "1,7,7000",
            "1,8,8000",
            "1,9,9000",
            "1,10,10000"},
           {"1,5,5000",
            "1,6,6000",
            "1,7,7000",
            "1,8,8000",
            "1,9,9000",
            "1,10,10000",
            "1,11,11000",
            "1,12,12000",
            "1,13,13000",
            "1,14,14000",
            "1,15,15000"},
           {"1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",   "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",
            "1,6,6000",   "1,7,7000",   "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
            "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"},
           {"0,1,1000",   "11,1,1001",  "3,1,1002",   "0,2,2000",   "10,2,2001",  "15,2,2002",  "10,3,3000",
            "10,3,3001",  "0,3,3003",   "0,3,3200",   "0,4,4000",   "0,5,5000",   "0,6,6000",   "0,7,7000",
            "0,8,8000",   "0,9,9000",   "0,10,10000", "0,11,11000", "0,12,12000", "0,13,13000", "0,14,14000",
            "0,15,15000", "0,16,16000", "0,17,17000", "0,18,18000", "0,19,19000", "0,20,20000", "0,21,21000"}};

    size_t queryCounter = 0;
    bool sltSourceCallbackCalled = false;
    bool queryCallbackCalled = false;

    SystestParser parser{};
    parser.registerOnSLTSourceCallback(
        [&](SystestParser::SLTSource&& source)
        {
            sltSourceCallbackCalled = true;
            ASSERT_EQ(source.name, expectedSLTSource.name);
            ASSERT_EQ(source.fields, expectedSLTSource.fields);
            ASSERT_EQ(source.tuples, expectedSLTSource.tuples);
        });

    parser.registerOnQueryCallback(
        [&](const std::string& query, const size_t currentQueryNumberInTest)
        {
            queryCallbackCalled = true;
            /// Query numbers start at QueryId::INITIAL, which is 1
            ASSERT_LT(currentQueryNumberInTest, expectedQueries.size() + 1);
            ASSERT_EQ(query, expectedQueries.at(currentQueryNumberInTest - 1));
        });

    ASSERT_TRUE(parser.loadFile(filename));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
    ASSERT_TRUE(queryCallbackCalled);
    ASSERT_TRUE(sltSourceCallbackCalled) << "SLT source callback was never called";
    ASSERT_TRUE(systestStarterGlobals.getQueryResultMap().size() == expectedResults.size());
    ASSERT_TRUE(
        std::ranges::all_of(
            expectedResults,
            [&systestStarterGlobals](const auto& expectedResult)
            { return std::ranges::contains(systestStarterGlobals.getQueryResultMap() | std::views::values, expectedResult); }));
}

TEST_F(SystestParserValidTestFileTest, ErrorExpectationTest)
{
    const auto* const filename = TEST_DATA_DIR "error_expectation.dummy";

    const auto* const expectQuery = R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow;)";
    const uint64_t expectErrorCode = 1003;
    const std::string expectErrorMessage = "expected error message";

    bool queryCallbackCalled = false;
    bool errorCallbackCalled = false;

    SystestParser parser{};
    parser.registerOnQueryCallback(
        [&](std::string&& query, size_t)
        {
            ASSERT_EQ(query, expectQuery);
            queryCallbackCalled = true;
        });

    parser.registerOnErrorExpectationCallback(
        [&](SystestParser::ErrorExpectation&& expectation)
        {
            ASSERT_EQ(expectation.code, expectErrorCode);
            ASSERT_EQ(expectation.message, expectErrorMessage);
            errorCallbackCalled = true;
        });

    ASSERT_TRUE(parser.loadFile(filename));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
    ASSERT_TRUE(queryCallbackCalled);
    ASSERT_TRUE(errorCallbackCalled);
}
}
