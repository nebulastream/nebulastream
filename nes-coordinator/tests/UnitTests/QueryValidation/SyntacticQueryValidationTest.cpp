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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class SyntacticQueryValidationTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SyntacticQueryValidationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SyntacticQueryValidationTest class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    static void PrintQString(const std::string& s) { NES_DEBUG("\n QUERY STRING:\n {}", s); }

    void TestForException(const std::string& queryString) {
        PrintQString(queryString);
        auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);
        EXPECT_THROW(syntacticQueryValidation->validate(queryString), InvalidQueryException);
    }
};

// Positive tests for a syntactically valid query
TEST_F(SyntacticQueryValidationTest, validQueryTest) {
    NES_INFO("Valid Query test");

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("id") > 10 && Attribute("id") < 100); )";

    auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);
    syntacticQueryValidation->validate(queryString);
}

TEST_F(SyntacticQueryValidationTest, validAttributeRenameFromProjectOperator) {
    NES_INFO("Valid Query test");

    auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);

    std::string queryString = R"(Query::from("default_logical").project(Attribute("id").as("identity")); )";

    syntacticQueryValidation->validate(queryString);
}

// Test a query with missing ; at line end
TEST_F(SyntacticQueryValidationTest, missingSemicolonTest) {
    NES_INFO("Missing semicolon test");

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("id") > 10 && Attribute("id") < 100) )";

    TestForException(queryString);
}

// Test a query where filter is misspelled as fliter
TEST_F(SyntacticQueryValidationTest, typoInFilterTest) {
    NES_INFO("Typo in filter test");

    std::string queryString = R"(Query::from("default_logical").fliter(Attribute("id") > 10 && Attribute("id") < 100); )";

    TestForException(queryString);
}

// Test a query where a closing parenthesis is missing
TEST_F(SyntacticQueryValidationTest, missingClosingParenthesisTest) {
    NES_INFO("Missing closing parenthesis test");

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("id") > 10 && Attribute("id") < 100; )";

    TestForException(queryString);
}

// Test a query where a boolean operator is invalid (& instead of &&)
TEST_F(SyntacticQueryValidationTest, invalidBoolOperatorTest) {
    NES_INFO("Invalid bool operator test");

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("id") > 10 & Attribute("id") < 100); )";

    TestForException(queryString);
}

TEST_F(SyntacticQueryValidationTest, validMultiJoinQuery) {
    NES_INFO("Invalid bool operator test");

    std::string queryString = R"(Query::sink2(NullOutputSinkDescriptor::create(),
                              {Query::from("sourceName1").joinWith(Query::from("sourceName2"))
                              .where(Attribute("m") == Attribute("m"))
                              .window(TumblingWindow::of(EventTime(Attribute("time1")), Milliseconds(500))),
                              Query::from("sourceName3").joinWith(Query::from("sourceName4"))
                              .where(Attribute("m") == Attribute("m"))
                              .window(TumblingWindow::of(EventTime(Attribute("time1")), Milliseconds(500))),
                              Query::from("sourceName5").joinWith(Query::from("sourceName6"))
                              .where(Attribute("m") == Attribute("m"))
                              .window(TumblingWindow::of(EventTime(Attribute("time1")), Milliseconds(500)))});)";

    auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);
    EXPECT_NO_THROW(syntacticQueryValidation->validate(queryString));
}


// Test queryIdAndCatalogEntryMapping that calls "Attribute().as()" outside a Projection operator
TEST_F(SyntacticQueryValidationTest, attributeRenameOutsideProjection) {
    NES_INFO("Invalid bool operator test");

    // Field-rename in Map expression is not allowed
    std::string queryStringWithMap =
        R"(Query::from("default_logical").map(Attribute("id") =  Attribute("id").as("identity") + 2 ); )";
    TestForException(queryStringWithMap);

    // Field-rename in Filter predicate is not allowed
    std::string queryStringWithFilter = R"(Query::from("default_logical").filter(Attribute("id").as("identity") > 10); )";
    TestForException(queryStringWithFilter);

    // Field-rename in Window Type creation is not allowed
    std::string queryStringWithTumblingWindow =
        R"(Query::from("default_logical").window(TumblingWindow::of(EventTime(Attribute("ts").as("timestamp")), Seconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value")));)";
    TestForException(queryStringWithTumblingWindow);

    // Field-rename in Window's byKey expression is not allowed
    std::string queryStringWithWindowByKey =
        R"(Query::from("default_logical").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).byKey(Attribute("id").as("identity")).apply(Sum(Attribute("value")));)";
    TestForException(queryStringWithWindowByKey);

    // Field-rename in Sum expression is not allowed
    std::string queryStringWithSum =
        R"(Query::from("default_logical").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value").as("val")));)";
    TestForException(queryStringWithSum);

    // Field-rename in Avg expression is not allowed
    std::string queryStringWithAvg =
        R"(Query::from("default_logical").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).byKey(Attribute("id")).apply(Avg(Attribute("value").as("val")));)";
    TestForException(queryStringWithAvg);

    // Field-rename in Count expression is not allowed
    std::string queryStringWithCount =
        R"(Query::from("default_logical").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).byKey(Attribute("id")).apply(Count(Attribute("value").as("val")));)";
    TestForException(queryStringWithCount);

    // Field-rename in Min expression is not allowed
    std::string queryStringWithMin =
        R"(Query::from("default_logical").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).byKey(Attribute("id")).apply(Min(Attribute("value").as("val")));)";
    TestForException(queryStringWithMin);

    // Field-rename in Max expression is not allowed
    std::string queryStringWithMax =
        R"(Query::from("default_logical").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).byKey(Attribute("id")).apply(Max(Attribute("value").as("val")));)";
    TestForException(queryStringWithMax);

    // Field-rename in Median expression is not allowed
    std::string queryStringWithMedian =
        R"(Query::from("default_logical").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).byKey(Attribute("id")).apply(Median(Attribute("value").as("val")));)";
    TestForException(queryStringWithMedian);

    // Field-rename in EventTimeWatermarkStrategyDescriptor is not allowed
    std::string queryStringWithWatermarkAssigner =
        R"(Query::from("default_logical").assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("timestamp").as("ts"),Milliseconds(50), ""Milliseconds()));)";
    TestForException(queryStringWithWatermarkAssigner);

    // Field-rename in Join's Where expression is not allowed
    std::string queryStringWithJoinWhere =
        R"(Query::from("default_logical").joinWith(subQuery).where(Attribute("id").as("identity") == Attribute("id")).window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10))).sink(printSinkDescriptor);)";
    TestForException(queryStringWithJoinWhere);

    // Field-rename in Join's EqualsTo expression is not allowed
    std::string queryStringWithJoinEqualsTo =
        R"(Query::from("default_logical").joinWith(subQuery).where(Attribute("id") == Attribute("id").as("identity")).window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10))).sink(printSinkDescriptor);)";
    TestForException(queryStringWithJoinEqualsTo);
}

// Positive test for a syntactically valid query
TEST_F(SyntacticQueryValidationTest, validQueryWithInferModelOperatorTest) {
    NES_INFO("Valid Query test with Infer model operator");

    auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);

    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10 && Attribute(\"id\") < 100)"
                              "                                .inferModel(\"models/iris.tflite\",\n"
                              "                                           {Attribute(\"value\"), Attribute(\"id\")},\n"
                              "                                           {Attribute(\"value\")}); ";
}

}// namespace NES
