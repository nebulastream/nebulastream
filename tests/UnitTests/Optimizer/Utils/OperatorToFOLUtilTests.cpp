// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <Optimizer/Utils/OperatorToFOLUtil.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <iostream>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <z3++.h>

using namespace NES;

class OperatorToFOLUtilTest : public testing::Test {

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

TEST_F(OperatorToFOLUtilTest, testFiltersWithExactPredicates) {

    z3::context context;
    //Define Predicate
    ExpressionNodePtr predicate = Attribute("value") == 40;
    predicate->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate);
    z3::expr expr1 = OperatorToFOLUtil::serializeOperator(logicalOperator1, context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate);
    z3::expr expr2 = OperatorToFOLUtil::serializeOperator(logicalOperator2, context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(context);
    solver.add(!to_expr(context, Z3_mk_eq(context, expr1, expr2)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToFOLUtilTest, testFiltersWithEqualPredicates) {

    z3::context context;

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = 40 == Attribute("value");
    predicate2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = OperatorToFOLUtil::serializeOperator(logicalOperator1, context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    z3::expr expr2 = OperatorToFOLUtil::serializeOperator(logicalOperator2, context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(context);
    solver.add(!to_expr(context, Z3_mk_eq(context, expr1, expr2)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToFOLUtilTest, testFiltersWithMultipleExactPredicates) {

    z3::context context;

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = OperatorToFOLUtil::serializeOperator(logicalOperator1, context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr2 = OperatorToFOLUtil::serializeOperator(logicalOperator2, context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(context);
    solver.add(!to_expr(context, Z3_mk_eq(context, expr1, expr2)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToFOLUtilTest, testFiltersWithMultipleEqualPredicates) {

    z3::context context;

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 && Attribute("value") == 40;
    predicate2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = OperatorToFOLUtil::serializeOperator(logicalOperator1, context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    z3::expr expr2 = OperatorToFOLUtil::serializeOperator(logicalOperator2, context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(context);
    solver.add(!to_expr(context, Z3_mk_eq(context, expr1, expr2)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToFOLUtilTest, testFiltersWithDifferentPredicates) {

    z3::context context;

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") == 40 ;
    predicate2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = OperatorToFOLUtil::serializeOperator(logicalOperator1, context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    z3::expr expr2 = OperatorToFOLUtil::serializeOperator(logicalOperator2, context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(context);
    z3::expr expr = to_expr(context, Z3_mk_eq(context, expr1, expr2));
    NES_INFO("Expressions : "<< expr);
    solver.add(!expr);
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::sat);
}

TEST_F(OperatorToFOLUtilTest, testFiltersWithMultipleDifferentPredicates) {

    z3::context context;

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 or Attribute("value") == 40;
    predicate2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    z3::expr expr1 = OperatorToFOLUtil::serializeOperator(logicalOperator1, context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    z3::expr expr2 = OperatorToFOLUtil::serializeOperator(logicalOperator2, context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(context);
    z3::expr expr = to_expr(context, Z3_mk_eq(context, expr1, expr2));
    NES_INFO("Expressions : "<< expr);
    solver.add(!expr);
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::sat);
}

TEST_F(OperatorToFOLUtilTest, testMapWithExactExpression) {

    z3::context context;
    //Define expression
    FieldAssignmentExpressionNodePtr expression = Attribute("value") = 40;
    expression->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression);
    z3::expr expr1 = OperatorToFOLUtil::serializeOperator(logicalOperator1, context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression);
    z3::expr expr2 = OperatorToFOLUtil::serializeOperator(logicalOperator2, context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(context);
    solver.add(!to_expr(context, Z3_mk_eq(context, expr1, expr2)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::unsat);
}

TEST_F(OperatorToFOLUtilTest, testMapWithDifferentExpression) {

    z3::context context;
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("value") = 40;
    expression1->inferStamp(schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("id") = 40;
    expression2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression1);
    z3::expr expr1 = OperatorToFOLUtil::serializeOperator(logicalOperator1, context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression2);
    z3::expr expr2 = OperatorToFOLUtil::serializeOperator(logicalOperator2, context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(context);
    solver.add(!to_expr(context, Z3_mk_eq(context, expr1, expr2)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::sat);
}

TEST_F(OperatorToFOLUtilTest, testMapWithDifferentExpressionOnSameField) {

    z3::context context;
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("value") = 40;
    expression1->inferStamp(schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("value") = 50;
    expression2->inferStamp(schema);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression1);
    z3::expr expr1 = OperatorToFOLUtil::serializeOperator(logicalOperator1, context);
    NES_INFO("Expression 1" << expr1);
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression2);
    z3::expr expr2 = OperatorToFOLUtil::serializeOperator(logicalOperator2, context);
    NES_INFO("Expression 2" << expr2);

    //Assert
    z3::solver solver(context);
    solver.add(!to_expr(context, Z3_mk_eq(context, expr1, expr2)));
    z3::check_result result = solver.check();
    ASSERT_EQ(result, z3::sat);
}