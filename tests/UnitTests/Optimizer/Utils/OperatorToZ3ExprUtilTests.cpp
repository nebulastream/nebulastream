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

// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <iostream>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <z3++.h>

using namespace NES;

class OperatorToZ3ExprUtilTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("OperatorToFOLUtilTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup OperatorToFOLUtilTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_INFO("Setup OperatorToFOLUtilTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down OperatorToFOLUtilTest test class.");
    }
};

TEST_F(OperatorToZ3ExprUtilTest, testFiltersWithExactPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Predicate
    ExpressionNodePtr predicate = Attribute("value") == 40;
    predicate->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate);
    z3::expr expr2 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    solver.add(to_expr(*context, Z3_mk_and(*context, 2, arrays)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToZ3ExprUtilTest, testFiltersWithEqualPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = 40 == Attribute("value");
    predicate2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    solver.add(to_expr(*context, Z3_mk_and(*context, 2, arrays)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToZ3ExprUtilTest, testFiltersWithMultipleExactPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    solver.add(to_expr(*context, Z3_mk_and(*context, 2, arrays)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToZ3ExprUtilTest, testFiltersWithMultipleEqualPredicates1) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 && Attribute("value") == 40;
    predicate2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    solver.add(to_expr(*context, Z3_mk_and(*context, 2, arrays)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToZ3ExprUtilTest, testFiltersWithMultipleEqualPredicates2) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 + 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 && Attribute("value") == 80;
    predicate2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    solver.add(to_expr(*context, Z3_mk_and(*context, 2, arrays)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToZ3ExprUtilTest, testFiltersWithDifferentPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") == 40;
    predicate2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    z3::expr expr = to_expr(*context, Z3_mk_and(*context, 2, arrays));
    solver.add(expr);
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::sat);
}

TEST_F(OperatorToZ3ExprUtilTest, testFiltersWithMultipleDifferentPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 or Attribute("value") == 40;
    predicate2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    z3::expr expr = to_expr(*context, Z3_mk_and(*context, 2, arrays));
    solver.add(expr);
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToZ3ExprUtilTest, testMapWithExactExpression) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression = Attribute("value") = 40;
    expression->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    solver.add(to_expr(*context, Z3_mk_and(*context, 2, arrays)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToZ3ExprUtilTest, testMapWithDifferentExpression) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("value") = 40;
    expression1->inferStamp(schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("id") = 40;
    expression2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression1);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression2);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    solver.add(to_expr(*context, Z3_mk_and(*context, 2, arrays)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::sat);
}

TEST_F(OperatorToZ3ExprUtilTest, testMapWithDifferentExpressionOnSameField) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("value") = 40;
    expression1->inferStamp(schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("value") = 50;
    expression2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression1);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression2);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    solver.add(to_expr(*context, Z3_mk_and(*context, 2, arrays)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::sat);
}

TEST_F(OperatorToZ3ExprUtilTest, testSourceWithExactStreamName) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Predicate
    auto sourceDescriptor = LogicalStreamSourceDescriptor::create("Car");

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    z3::expr expr = to_expr(*context, Z3_mk_and(*context, 2, arrays));
    solver.add(expr);
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToZ3ExprUtilTest, testSourceWithDifferentStreamName) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Predicate
    auto sourceDescriptor1 = LogicalStreamSourceDescriptor::create("Car");
    auto sourceDescriptor2 = LogicalStreamSourceDescriptor::create("Truck");

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    z3::expr expr1 = logicalOperator1->inferZ3Expression(context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    z3::expr expr2 = logicalOperator2->inferZ3Expression(context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(*context);
    Z3_ast arrays[] = {expr1, !expr2};
    z3::expr expr = to_expr(*context, Z3_mk_and(*context, 2, arrays));
    solver.add(expr);
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::sat);
}