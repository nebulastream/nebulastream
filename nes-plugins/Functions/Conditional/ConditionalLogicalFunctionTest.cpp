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

/// These tests cover behaviour that the end-to-end systests cannot observe: value equality (used by the
/// optimizer), the reflection round-trip, and the textual explain() form. Execution results, type inference,
/// and argument validation are exercised by nes-systests/function/Conditional.test instead.

#include <string>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ConditionalLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
LogicalFunction constant(const DataType::Type type, const std::string& value)
{
    return ConstantValueLogicalFunction{DataTypeProvider::provideDataType(type), value};
}
}

/// A multi-branch function survives a reflection round-trip unchanged.
TEST(ConditionalLogicalFunctionTest, SerializeDeserializeRoundtrip)
{
    const auto original = LogicalFunction{ConditionalLogicalFunction{
        {constant(DataType::Type::BOOLEAN, "true"),
         constant(DataType::Type::VARSIZED, "A"),
         constant(DataType::Type::BOOLEAN, "false"),
         constant(DataType::Type::VARSIZED, "B"),
         constant(DataType::Type::VARSIZED, "C")}}};

    const auto reflected = ReflectionContext{}.reflect(original);
    const auto deserialized = ReflectionContext{}.unreflect<LogicalFunction>(reflected);
    EXPECT_TRUE(original == deserialized);
}

/// Two functions built from identical children compare equal.
TEST(ConditionalLogicalFunctionTest, EqualityIdentical)
{
    const auto lhs = ConditionalLogicalFunction{
        {constant(DataType::Type::BOOLEAN, "true"), constant(DataType::Type::INT32, "1"), constant(DataType::Type::INT32, "0")}};
    const auto rhs = ConditionalLogicalFunction{
        {constant(DataType::Type::BOOLEAN, "true"), constant(DataType::Type::INT32, "1"), constant(DataType::Type::INT32, "0")}};
    EXPECT_TRUE(lhs == rhs);
}

/// Functions differing in a single child do not compare equal.
TEST(ConditionalLogicalFunctionTest, EqualityDifferent)
{
    const auto lhs = ConditionalLogicalFunction{
        {constant(DataType::Type::BOOLEAN, "true"), constant(DataType::Type::INT32, "1"), constant(DataType::Type::INT32, "0")}};
    const auto rhs = ConditionalLogicalFunction{
        {constant(DataType::Type::BOOLEAN, "true"), constant(DataType::Type::INT32, "2"), constant(DataType::Type::INT32, "0")}};
    EXPECT_FALSE(lhs == rhs);
}

/// explain() renders function-call syntax; the SQL parser has no CASE WHEN syntax yet, so the plan renderer
/// (and therefore an EXPLAIN systest) never surfaces this form for the projected expression.
TEST(ConditionalLogicalFunctionTest, ExplainUsesFunctionCallForm)
{
    const auto function = ConditionalLogicalFunction{
        {constant(DataType::Type::BOOLEAN, "true"),
         constant(DataType::Type::VARSIZED, "r1"),
         constant(DataType::Type::VARSIZED, "fallback")}};
    const auto explanation = function.explain(ExplainVerbosity::Short);
    EXPECT_TRUE(explanation.starts_with("Conditional("));
    EXPECT_EQ(explanation.find("CASE"), std::string::npos);
    EXPECT_EQ(explanation.find("WHEN"), std::string::npos);
}

}
