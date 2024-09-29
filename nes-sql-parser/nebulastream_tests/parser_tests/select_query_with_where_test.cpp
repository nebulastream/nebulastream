//
// Created by Usama Bin Tariq on 23.09.24.
//

#include <gtest/gtest.h>
#include <nebula/parser/nebula_parser.hpp>
#include <nebula/parser/statements/select_statement.hpp>
#include <memory>
#include <nebula/parser/expressions/column_expression.hpp>
#include <nebula/parser/expressions/comparison_expression.hpp>
#include <nebula/parser/expressions/constant_expression.hpp>

std::string query_with_int = "SELECT id FROM user_data WHERE id = 2";
std::string query_with_string = "SELECT id FROM user_data WHERE id = '2'";

void VERIFY_PRE_STEPS(std::vector<std::unique_ptr<nebula::SQLStatement> > &statements) {
    //check the size of parsed statement
    ASSERT_EQ(1, statements.size());

    auto &fs = statements[0];

    //down casting sql statement into statement
    auto select_statement = dynamic_cast<nebula::SelectStatement *>(statements[0].get());

    //check if the type of the sql statement is select statement
    ASSERT_EQ(select_statement->TYPE, nebula::StatementType::SELECT_STATEMENT);

    //check if the table name is same
    ASSERT_EQ(select_statement->select_node->from_table->table_name, "user_data");

    //asserting the size of select columns
    ASSERT_EQ(select_statement->select_node->select_list.size(), 1);

    std::unique_ptr<nebula::ParsedExpression> &where_expression = select_statement->select_node->where_clause;

    //make sure the expression class is type of comparison class
    ASSERT_EQ(where_expression->expression_class, nebula::ExpressionClass::COMPARISON);

    //down casting expression to comparison expression
    auto comparison_expression = dynamic_cast<nebula::ComparisonExpression *>(where_expression.
        get());

    //make sure the comparison type is compare equal
    ASSERT_EQ(comparison_expression->type, nebula::ExpressionType::COMPARE_EQUAL);

    //make sure the left side is type of column reference & column name is id
    ASSERT_EQ(comparison_expression->left->type, nebula::ExpressionType::COLUMN_REF);
    auto column_ref = dynamic_cast<nebula::ColumnExpression *>(comparison_expression->left.get());
    ASSERT_EQ(column_ref->column_names[0], "id");

    //make sure the  right side is type of constant value
    ASSERT_EQ(comparison_expression->right->type, nebula::ExpressionType::VALUE_CONSTANT);
}

TEST(PARSER_TEST, SELECT_WHERE_SIMPLE_QUERY_INT) {
    nebula::Parser parser;
    parser.parse(query_with_int);

    //getting statements
    auto &statements = parser.statements_collection->statements;

    //verify common pre steps
    VERIFY_PRE_STEPS(statements);

    const auto select_statement = dynamic_cast<nebula::SelectStatement *>(statements[0].get());
    auto &where_expression = select_statement->select_node->where_clause;
    auto comparison_expression = dynamic_cast<nebula::ComparisonExpression *>(where_expression.get());

    //down casting right side of (id = 1) to const expression
    const auto const_val = dynamic_cast<nebula::ConstantExpression *>(comparison_expression->right.get());

    //make sure the value type is integer
    ASSERT_EQ(const_val->value.getType(), nebula::Value::Type::Int32);

    //make sure the value is equal to 2
    ASSERT_EQ(const_val->value, 2);
}

TEST(PARSER_TEST, SELECT_WHERE_SIMPLE_QUERY_STRING) {
    nebula::Parser parser;
    parser.parse(query_with_string);

    //getting statements
    auto &statements = parser.statements_collection->statements;

    //verify common pre steps
    VERIFY_PRE_STEPS(statements);

    const auto select_statement = dynamic_cast<nebula::SelectStatement *>(statements[0].get());
    auto &where_expression = select_statement->select_node->where_clause;
    auto comparison_expression = dynamic_cast<nebula::ComparisonExpression *>(where_expression.get());

    //down casting right side of (id = 1) to const expression
    const auto const_val = dynamic_cast<nebula::ConstantExpression *>(comparison_expression->right.get());

    //make sure the value type is string
    ASSERT_EQ(const_val->value.getType(), nebula::Value::Type::String);

    //make sure the value is equal to '2'
    ASSERT_EQ(const_val->value, "2");
}
