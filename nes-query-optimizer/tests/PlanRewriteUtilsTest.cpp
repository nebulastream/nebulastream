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

#include <unordered_map>
#include <gtest/gtest.h>

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>
#include <PlanRewriteUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{
class PlanRewriteUtilsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("PlanRewriteUtilsTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};
}

TEST_F(PlanRewriteUtilsTest, testReplaceFieldAccesses)
{
    auto op1 = utils.createSource("first", {"a", "b"});
    auto op2 = utils.createSource("second", {"c", "d"});
    auto op3 = utils.createSource("third", {"e", "f"});

    auto op1New = utils.createSource("first_new", {"a", "b"});
    auto op2New = utils.createSource("second_new", {"c", "d"});

    const std::unordered_map<Field, Field> fields
        = {{op1.getOutputSchema().getFieldByName(Identifier::parse("a")).value(),
            op1New.getOutputSchema().getFieldByName(Identifier::parse("a")).value()},
           {op2.getOutputSchema().getFieldByName(Identifier::parse("c")).value(),
            op2New.getOutputSchema().getFieldByName(Identifier::parse("c")).value()}};

    const LogicalFunction function = AndLogicalFunction{
        GreaterLogicalFunction{
            FieldAccessLogicalFunction{op1.getOutputSchema().getFieldByName(Identifier::parse("a")).value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}},
        AndLogicalFunction{
            LessLogicalFunction{
                FieldAccessLogicalFunction{op2.getOutputSchema().getFieldByName(Identifier::parse("c")).value()},
                ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}},
            EqualsLogicalFunction{
                FieldAccessLogicalFunction{op3.getOutputSchema().getFieldByName(Identifier::parse("e")).value()},
                ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}}}};


    const auto newFunction = replaceFieldAccesses(function, fields);

    const auto fieldAccess1 = newFunction.getChildren().at(0).getChildren().at(0).getAs<FieldAccessLogicalFunction>();
    const auto fieldAccess2 = newFunction.getChildren().at(1).getChildren().at(0).getChildren().at(0).getAs<FieldAccessLogicalFunction>();
    const auto fieldAccess3 = newFunction.getChildren().at(1).getChildren().at(1).getChildren().at(0).getAs<FieldAccessLogicalFunction>();


    ASSERT_EQ(fieldAccess1->getField().getProducedBy(), op1New);
    ASSERT_EQ(fieldAccess2->getField().getProducedBy(), op2New);
    ASSERT_EQ(fieldAccess3->getField().getProducedBy(), op3);
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
