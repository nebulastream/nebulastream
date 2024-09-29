//
// Created by Usama Bin Tariq on 18.09.24.
//
#include <iostream>
#include "nebula/parser/statements/sql_statement_collection.hpp"
#include <string>
#include <vector>
#include <memory>

namespace nebula {
    void SQLStatementCollection::Print() {
        int count = 0;
        for (const std::unique_ptr<SQLStatement> &stmt: statements) {
            std::cout << "[" << ++count << "] " << std::endl;
            stmt->Print();
        }
    }

    void SQLStatementCollection::add_statement(std::unique_ptr<SQLStatement> &statement) {
        statements.push_back(std::move(statement));
    }
}
