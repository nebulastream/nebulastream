#pragma once
#include <iostream>
#include "sql_statement.hpp"
#include <vector>

namespace nebula {
    class SQLStatementCollection {
    public:
        virtual ~SQLStatementCollection() = default;

        virtual void Print();

        virtual void add_statement(std::unique_ptr<SQLStatement> &statement);

        std::vector<std::unique_ptr<SQLStatement> > statements;
    };
}
