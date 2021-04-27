/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <gtest/gtest.h>

namespace NES {

/// Due to the order of overload resolution, the implicit conversion of 0 to a nullptr has got a higher priority
/// as an build-in conversion than the user-defined implicit conversions to the type ExpressionItem.
/// Check that this does not cause any issues for any logical expression
class LogicalExpressionTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("LogicalExpressionTest.log", NES::LOG_DEBUG);
        NES_DEBUG("LogicalExpressionTest: Setup QueryCatalogTest test class.");
    }

    void SetUp() {}

    void TearDown() {
        NES_DEBUG("LogicalExpressionTest: Tear down QueryExecutionTest test case.");
    }

    static void TearDownTestCase() { NES_DEBUG("LogicalExpressionTest: Tear down QueryExecutionTest test class."); }

    inline static void testBinaryOperator(std::string const &op) noexcept {

        std::vector<std::tuple<std::string, std::string>> pairs{
            {"0", R"(Attribute("value"))"},
            {"1", R"(Attribute("value"))"},
            {"true", R"(Attribute("value"))"},
            {"false", R"(Attribute("value"))"},
            {"static_cast<int8_t>(0)", R"(Attribute("value"))"},
            {"static_cast<uint8_t>(0)", R"(Attribute("value"))"},
            {"static_cast<uint16_t>(0)", R"(Attribute("value"))"},
            {"static_cast<int16_t>(0)", R"(Attribute("value"))"},
            {"static_cast<uint32_t>(0)", R"(Attribute("value"))"},
            {"static_cast<int32_t>(0)", R"(Attribute("value"))"},
            {"static_cast<uint64_t>(0)", R"(Attribute("value"))"},
            {"static_cast<int64_t>(0)", R"(Attribute("value"))"},
            {"static_cast<float>(0)", R"(Attribute("value"))"},
            {"static_cast<double>(0)", R"(Attribute("value"))"},
            {"static_cast<bool>(0)", R"(Attribute("value"))"},
            {"static_cast<int8_t>(1)", R"(Attribute("value"))"},
            {"static_cast<uint8_t>(1)", R"(Attribute("value"))"},
            {"static_cast<uint16_t>(1)", R"(Attribute("value"))"},
            {"static_cast<int16_t>(1)", R"(Attribute("value"))"},
            {"static_cast<uint32_t>(1)", R"(Attribute("value"))"},
            {"static_cast<int32_t>(1)", R"(Attribute("value"))"},
            {"static_cast<uint64_t>(1)", R"(Attribute("value"))"},
            {"static_cast<int64_t>(1)", R"(Attribute("value"))"},
            {"static_cast<float>(1)", R"(Attribute("value"))"},
            {"static_cast<double>(1)", R"(Attribute("value"))"},
            {"static_cast<bool>(1)", R"(Attribute("value"))"},
            {R"("char const*")", R"(Attribute("value"))"},
            {R"(static_cast<char const*>("char const*"))", R"(Attribute("value"))"},
            {R"(Attribute("value"))", R"(Attribute("value"))"},
        };

        for (auto const &[v1, v2] : pairs) {
            auto const q1 = R"(Query::from("").filter()" + v1 + op + v2 + R"( );)";
            EXPECT_NO_THROW(UtilityFunctions::createQueryFromCodeString(q1));

            auto const q2 = R"(Query::from("").filter()" + v1 + op + v2 + R"( );)";
            EXPECT_NO_THROW(UtilityFunctions::createQueryFromCodeString(q2));
        }
    }
};

//TODO: re-enable tests when finishing #1170, #1781

TEST_F(LogicalExpressionTest, DISABLED_testEqualityExpression) { testBinaryOperator("=="); }

TEST_F(LogicalExpressionTest, DISABLED_testInequalityExpression) { testBinaryOperator("!="); }

TEST_F(LogicalExpressionTest, DISABLED_testAndCompile) { testBinaryOperator("&&"); }

TEST_F(LogicalExpressionTest, DISABLED_testOrExpression) { testBinaryOperator("||"); }

TEST_F(LogicalExpressionTest, DISABLED_testLeqExpression) { testBinaryOperator("<="); }

TEST_F(LogicalExpressionTest, DISABLED_testGeqExpression) { testBinaryOperator(">="); }

TEST_F(LogicalExpressionTest, DISABLED_testLtExpression) { testBinaryOperator("<"); }

TEST_F(LogicalExpressionTest, DISABLED_testGtExpression) { testBinaryOperator(">"); }

}
