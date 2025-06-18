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

#include <algorithm>
#include <array>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <SystestSources/SourceTypes.hpp>
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
    const std::vector<std::vector<std::string>> expectedResults = {{"1,1,1", "1,1,1", "1,1,1"}, {"2,2,2", "2,2,2", "2,2,2"}};
    const SystestAttachSource expectedAttachSource
        = {.sourceType = "File",
           .sourceConfigurationPath = "",
           .inputFormatterType = "CSV",
           .inputFormatterConfigurationPath = "",
           .logicalSourceName = "e124",
           .testDataIngestionType = TestDataIngestionType::FILE,
           .tuples = std::nullopt,
           .fileDataPath = "dummy_path.txt",
           .serverThreads = nullptr};
    const SystestParser::SystestLogicalSource expectedLogicalSource
        = {.name = "e124", .fields = {{.type = DataTypeProvider::provideDataType(DataType::Type::INT8), .name = "i"}}};

    bool queryCallbackCalled = false;
    bool logicalSourceCallbackCalled = false;
    bool attachSourceCallbackCalled = false;

    SystestParser parser{};
    std::unordered_map<SystestQueryId, std::vector<std::string>> queryResultMap;
    parser.registerOnQueryCallback([&](const std::string&, SystestQueryId) { queryCallbackCalled = true; });
    parser.registerOnSystestLogicalSourceCallback([&](const SystestParser::SystestLogicalSource&) { logicalSourceCallbackCalled = true; });
    parser.registerOnSystestAttachSourceCallback([&](const SystestAttachSource&) { attachSourceCallbackCalled = true; });
    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, const SystestQueryId correspondingQueryId)
                                          { queryResultMap.emplace(correspondingQueryId, std::move(resultTuples)); });

    static constexpr std::string_view Filename = SYSTEST_DATA_DIR "valid.dummy";
    ASSERT_TRUE(parser.loadFile(Filename)) << "Failed to load file: " << Filename;
    EXPECT_NO_THROW(parser.parse());

    /// Verify that all expected callbacks were called
    ASSERT_TRUE(queryCallbackCalled) << "Query callback was never called";
    ASSERT_TRUE(logicalSourceCallbackCalled) << "Logical source callback was never called";
    ASSERT_TRUE(attachSourceCallbackCalled) << "Attach source callback was never called";
    ASSERT_TRUE(queryResultMap.size() != 3) << "Result callback was never called";

    ASSERT_TRUE(std::ranges::all_of(
        expectedResults,
        [&queryResultMap](const auto& expectedResult)
        { return std::ranges::contains(queryResultMap | std::views::values, expectedResult); }));
}

TEST_F(SystestParserValidTestFileTest, Comments1TestFile)
{
    const auto* const filename = SYSTEST_DATA_DIR "comments.dummy";
    SystestParser::SystestLogicalSource expectedLogicalSource{
        .name = "window",
        .fields
        = {{.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "id"},
           {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "value"},
           {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "timestamp"}}};
    const SystestAttachSource expectedAttachSource
        = {.sourceType = "File",
           .sourceConfigurationPath = "",
           .inputFormatterType = "CSV",
           .inputFormatterConfigurationPath = "",
           .logicalSourceName = "e124",
           .testDataIngestionType = TestDataIngestionType::INLINE,
           .tuples
           = std::vector<std::string>{{"1,1,1000",   "12,1,1001",  "4,1,1002",   "1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",
                                       "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",   "1,6,6000",   "1,7,7000",
                                       "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
                                       "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"}},
           .fileDataPath = "null",
           .serverThreads = nullptr};

    /// Expected queries and results
    const auto expectedQueries = std::to_array<std::string>(
        {R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow)",
         R"(SELECT * FROM window WHERE id >= UINT64(10) INTO sinkWindow)",
         R"(SELECT * FROM window WHERE timestamp <= UINT64(10000) INTO sinkWindow)",
         R"(SELECT * FROM window WHERE timestamp >= UINT64(5000) AND timestamp <= UINT64(15000) INTO sinkWindow)",
         R"(SELECT * FROM window WHERE value != UINT64(1) INTO sinkWindow)"});

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

    bool logicalSourceCallbackCalled = false;
    bool queryCallbackCalled = false;
    bool attachSourceCallbackCalled = false;

    SystestParser parser{};
    std::unordered_map<SystestQueryId, std::vector<std::string>> queryResultMap;
    parser.registerOnSystestLogicalSourceCallback(
        [&logicalSourceCallbackCalled, &expectedLogicalSource](const SystestParser::SystestLogicalSource& source)
        {
            logicalSourceCallbackCalled = true;
            ASSERT_EQ(source.name, expectedLogicalSource.name);
            ASSERT_EQ(source.fields, expectedLogicalSource.fields);
        });

    parser.registerOnQueryCallback(
        [&queryCallbackCalled, &expectedQueries](const std::string& query, const SystestQueryId currentQueryIdInTest)
        {
            queryCallbackCalled = true;
            /// Query numbers start at QueryId::INITIAL, which is 1
            ASSERT_LT(currentQueryIdInTest.getRawValue(), expectedQueries.size() + 1);
            ASSERT_EQ(query, expectedQueries.at(currentQueryIdInTest.getRawValue() - 1));
        });

    parser.registerOnSystestAttachSourceCallback(
        [&attachSourceCallbackCalled, &expectedAttachSource](const SystestAttachSource& attachSource)
        {
            attachSourceCallbackCalled = true;
            ASSERT_EQ(attachSource.sourceType, expectedAttachSource.sourceType);
            ASSERT_EQ(attachSource.inputFormatterType, expectedAttachSource.inputFormatterType);
            ASSERT_TRUE(
                attachSource.tuples.has_value() and expectedAttachSource.tuples.has_value()
                and (attachSource.tuples.value() == expectedAttachSource.tuples.value()));
        });

    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, const SystestQueryId correspondingQueryId)
                                          { queryResultMap.emplace(correspondingQueryId, std::move(resultTuples)); });
    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, const SystestQueryId correspondingQueryId)
                                          { queryResultMap.emplace(correspondingQueryId, std::move(resultTuples)); });

    ASSERT_TRUE(parser.loadFile(filename));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(attachSourceCallbackCalled) << "SLT source callback was never called";
    ASSERT_TRUE(queryCallbackCalled) << "Query callback was never called";
    ASSERT_TRUE(queryResultMap.size() == expectedResults.size());
    ASSERT_TRUE(std::ranges::all_of(
        expectedResults,
        [&queryResultMap](const auto& expectedResult)
        { return std::ranges::contains(queryResultMap | std::views::values, expectedResult); }));
}


TEST_F(SystestParserValidTestFileTest, FilterTestFile)
{
    const auto* const filename = SYSTEST_DATA_DIR "filter.dummy";
    SystestParser::SystestLogicalSource expectedLogicalSource{
        .name = "window",
        .fields
        = {{.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "id"},
           {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "value"},
           {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "timestamp"}}};

    const SystestAttachSource expectedAttachSource
        = {.sourceType = "File",
           .sourceConfigurationPath = "",
           .inputFormatterType = "CSV",
           .inputFormatterConfigurationPath = "",
           .logicalSourceName = "e124",
           .testDataIngestionType = TestDataIngestionType::INLINE,
           .tuples
           = std::vector<std::string>{{"1,1,1000",   "12,1,1001",  "4,1,1002",   "1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",
                                       "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",   "1,6,6000",   "1,7,7000",
                                       "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
                                       "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"}},
           .fileDataPath = "null",
           .serverThreads = nullptr};

    const auto expectedQueries = std::to_array<std::string>(
        {R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow)",
         R"(SELECT * FROM window WHERE id >= UINT64(10) INTO sinkWindow)",
         R"(SELECT * FROM window WHERE timestamp <= UINT64(10000) INTO sinkWindow)",
         R"(SELECT * FROM window WHERE timestamp >= UINT64(5000) AND timestamp <= UINT64(15000) INTO sinkWindow)",
         R"(SELECT * FROM window WHERE value != UINT64(1) INTO sinkWindow)",
         R"(SELECT * FROM window WHERE id = id - UINT64(1) INTO sinkWindow)"});

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

    bool sltSourceCallbackCalled = false;
    bool queryCallbackCalled = false;
    bool attachSourceCallbackCalled = false;

    SystestParser parser{};
    std::unordered_map<SystestQueryId, std::vector<std::string>> queryResultMap;
    parser.registerOnSystestLogicalSourceCallback(
        [&](const SystestParser::SystestLogicalSource& source)
        {
            sltSourceCallbackCalled = true;
            ASSERT_EQ(source.name, expectedLogicalSource.name);
            ASSERT_EQ(source.fields, expectedLogicalSource.fields);
        });

    parser.registerOnQueryCallback(
        [&](const std::string& query, const SystestQueryId currentQueryIdInTest)
        {
            queryCallbackCalled = true;
            /// Query numbers start at QueryId::INITIAL, which is 1
            ASSERT_LT(currentQueryIdInTest.getRawValue(), expectedQueries.size() + 1);
            ASSERT_EQ(query, expectedQueries.at(currentQueryIdInTest.getRawValue() - 1));
        });

    parser.registerOnSystestAttachSourceCallback(
        [&attachSourceCallbackCalled, &expectedAttachSource](const SystestAttachSource& attachSource)
        {
            attachSourceCallbackCalled = true;
            ASSERT_EQ(attachSource.sourceType, expectedAttachSource.sourceType);
            ASSERT_EQ(attachSource.inputFormatterType, expectedAttachSource.inputFormatterType);
            ASSERT_TRUE(
                attachSource.tuples.has_value() and expectedAttachSource.tuples.has_value()
                and (attachSource.tuples.value() == expectedAttachSource.tuples.value()));
        });

    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, const SystestQueryId correspondingQueryId)
                                          { queryResultMap.emplace(correspondingQueryId, std::move(resultTuples)); });
    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, const SystestQueryId correspondingQueryId)
                                          { queryResultMap.emplace(correspondingQueryId, std::move(resultTuples)); });

    ASSERT_TRUE(parser.loadFile(filename));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(queryCallbackCalled);
    ASSERT_TRUE(sltSourceCallbackCalled) << "SLT source callback was never called";
    ASSERT_TRUE(attachSourceCallbackCalled) << "Attach source callback was never called";
    ASSERT_TRUE(queryResultMap.size() == expectedResults.size());
    ASSERT_TRUE(std::ranges::all_of(
        expectedResults,
        [&queryResultMap](const auto& expectedResult)
        { return std::ranges::contains(queryResultMap | std::views::values, expectedResult); }));
}

TEST_F(SystestParserValidTestFileTest, ErrorExpectationTest)
{
    const auto* const expectQuery = R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow;)";
    constexpr uint64_t expectErrorCode = 1003;
    const std::string expectErrorMessage = "expected error message";

    bool queryCallbackCalled = false;
    bool errorCallbackCalled = false;

    SystestParser parser{};
    parser.registerOnQueryCallback(
        [&queryCallbackCalled, &expectQuery](const std::string& query, SystestQueryId)
        {
            ASSERT_EQ(query, expectQuery);
            queryCallbackCalled = true;
        });

    parser.registerOnSystestAttachSourceCallback([](const SystestAttachSource&) { /* parses input tuples */ });

    parser.registerOnErrorExpectationCallback(
        [&errorCallbackCalled, &expectErrorMessage, &expectErrorCode](
            const SystestParser::ErrorExpectation& expectation, const SystestQueryId)
        {
            ASSERT_EQ(expectation.code, expectErrorCode);
            ASSERT_EQ(expectation.message, expectErrorMessage);
            errorCallbackCalled = true;
        });

    static constexpr std::string_view Filename = SYSTEST_DATA_DIR "error_expectation.dummy";
    ASSERT_TRUE(parser.loadFile(Filename));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(queryCallbackCalled);
    ASSERT_TRUE(errorCallbackCalled);
}
}
