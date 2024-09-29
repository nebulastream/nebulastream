//
// Created by Usama Bin Tariq on 22.09.24.
//

#include <nebula/parser/nebula_parser.hpp>
#include <nebula/parser/statements/select_statement.hpp>
#include <string>
#include <memory>
#include "gtest/gtest.h"

TEST(PARSER_TEST, SIMPLE_SELECT_QUERY) {
    std::string query = "SELECT a FROM test";

    nebula::Parser parser;

    parser.parse(query);

    //getting statements
    auto &statements = parser.statements_collection->statements;

    //check the size of parsed statement
    ASSERT_EQ(1, statements.size());

    auto &fs = statements[0];

    //down casting sql statement into statement
    auto select_statement = dynamic_cast<nebula::SelectStatement *>(statements[0].get());

    //check if the type of the sql statement is select statement
    ASSERT_EQ(select_statement->TYPE, nebula::StatementType::SELECT_STATEMENT);

    //check if the table name is same
    ASSERT_EQ(select_statement->select_node->from_table->table_name, "test");

    //asserting the size of select columns
    ASSERT_EQ(select_statement->select_node->select_list.size(), 1);
};
