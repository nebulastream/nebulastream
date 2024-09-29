//
// Created by Usama Bin Tariq on 16.09.24.
//
#pragma once
#include <string>
#include "sql_statement.hpp"
#include <vector>
#include "nebula/parser/nodes/select_node.hpp"
#include <memory>

namespace nebula {
    class SelectStatement : public SQLStatement {
    public:
        static constexpr const StatementType TYPE = StatementType::SELECT_STATEMENT;

        std::unique_ptr<SelectNode> select_node;

        void Print() const override;

        std::string ToString() const override;

        std::string ToStreamQuery() const override;

        bool VerifyStreamQuery() const override;
    };
}
