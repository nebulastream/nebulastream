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

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/UtilityFunctions.hpp>

#include <gtest/gtest.h>
#include "../../util/NesBaseTest.hpp"

namespace NES {

/// Due to the order of overload resolution, the implicit conversion of 0 to a nullptr has got a higher priority
/// as an build-in conversion than the user-defined implicit conversions to the type ExpressionItem.
/// Check that this does not cause any issues for any logical expression
class LogicalExpressionTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    static void SetUpTestCase() {
        NES::setupLogging("LogicalExpressionTest.log", NES::LOG_DEBUG);
        NES_DEBUG("LogicalExpressionTest: Setup QueryCatalogTest test class.");
    }

    void SetUp() override {
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    void TearDown() override { NES_DEBUG("LogicalExpressionTest: Tear down QueryExecutionTest test case."); }

    static void TearDownTestCase() { NES_DEBUG("LogicalExpressionTest: Tear down QueryExecutionTest test class."); }

    inline void testBinaryOperator(std::string const& op) noexcept {

        std::vector<std::tuple<std::string, std::string>> pairs{
            {"0", R"(Attribute("value"))"},
            {"1", R"(Attribute("value"))"},
            {"true", R"(Attribute("value"))"},
            {"false", R"(Attribute("value"))"},
            {R"("char const*")", R"(Attribute("value"))"},
            {R"(static_cast<char const*>("char const*"))", R"(Attribute("value"))"},
            {R"(Attribute("value"))", R"(Attribute("value"))"},
        };

        for (auto const& [v1, v2] : pairs) {
            auto const q1 = R"(Query::from("").filter()" + v1 + op + v2 + R"( );)";
            EXPECT_NO_THROW(queryParsingService->createQueryFromCodeString(q1));

            auto const q2 = R"(Query::from("").filter()" + v2 + op + v1 + R"( );)";
            EXPECT_NO_THROW(queryParsingService->createQueryFromCodeString(q2));
        }
    }
};

/// Template that can check that different Operators can be used with all designated data types that construct an
/// ExpressionItem.
template<template<typename, auto...> typename CompilesFromArgs, template<typename...> typename CompilesFromTypes>
void checkBinary() {
    bool compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, int8_t>::value
        && CompilesFromTypes<ExpressionNodePtr, int8_t, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, uint8_t>::value
        && CompilesFromTypes<ExpressionNodePtr, uint8_t, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, int16_t>::value
        && CompilesFromTypes<ExpressionNodePtr, int16_t, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, uint16_t>::value
        && CompilesFromTypes<ExpressionNodePtr, uint16_t, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, int32_t>::value
        && CompilesFromTypes<ExpressionNodePtr, int32_t, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, uint32_t>::value
        && CompilesFromTypes<ExpressionNodePtr, uint32_t, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, int64_t>::value
        && CompilesFromTypes<ExpressionNodePtr, int64_t, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, uint64_t>::value
        && CompilesFromTypes<ExpressionNodePtr, uint64_t, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, float>::value
        && CompilesFromTypes<ExpressionNodePtr, float, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, double>::value
        && CompilesFromTypes<ExpressionNodePtr, double, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, bool>::value
        && CompilesFromTypes<ExpressionNodePtr, bool, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, char const*>::value
        && CompilesFromTypes<ExpressionNodePtr, char const*, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, ValueTypePtr>::value
        && CompilesFromTypes<ExpressionNodePtr, ValueTypePtr, ExpressionItem>::value;
    ASSERT_TRUE(compiles);

    compiles = CompilesFromTypes<ExpressionNodePtr, ExpressionItem, ExpressionNodePtr>::value
        && CompilesFromTypes<ExpressionNodePtr, ExpressionNodePtr, ExpressionItem>::value;
    ASSERT_TRUE(compiles);
}

SETUP_COMPILE_TIME_TESTS(eq, operator==);
SETUP_COMPILE_TIME_TESTS(neq, operator!=);
SETUP_COMPILE_TIME_TESTS(land, operator&&);
SETUP_COMPILE_TIME_TESTS(lor, operator||);
SETUP_COMPILE_TIME_TESTS(leq, operator<=);
SETUP_COMPILE_TIME_TESTS(geq, operator>=);
SETUP_COMPILE_TIME_TESTS(lt, operator<);
SETUP_COMPILE_TIME_TESTS(gt, operator>);

//TODO: re-enable tests when finishing #1170, #1781

TEST_F(LogicalExpressionTest, testEqualityExpression) {
    checkBinary<eqCompiles, eqCompilesFromType>();
    testBinaryOperator("==");
}

TEST_F(LogicalExpressionTest, testInequalityExpression) {
    checkBinary<neqCompiles, neqCompilesFromType>();
    testBinaryOperator("!=");
}

TEST_F(LogicalExpressionTest, testAndCompile) {
    checkBinary<landCompiles, landCompilesFromType>();
    testBinaryOperator("&&");
}

TEST_F(LogicalExpressionTest, testOrExpression) {
    checkBinary<lorCompiles, lorCompilesFromType>();
    testBinaryOperator("||");
}

TEST_F(LogicalExpressionTest, testLeqExpression) {
    checkBinary<leqCompiles, leqCompilesFromType>();
    testBinaryOperator("<=");
}

TEST_F(LogicalExpressionTest, testGeqExpression) {
    checkBinary<geqCompiles, geqCompilesFromType>();
    testBinaryOperator(">=");
}

TEST_F(LogicalExpressionTest, testLtExpression) {
    checkBinary<ltCompiles, ltCompilesFromType>();
    testBinaryOperator("<");
}

TEST_F(LogicalExpressionTest, testGtExpression) {
    checkBinary<gtCompiles, gtCompilesFromType>();
    testBinaryOperator(">");
}

}// namespace NES
