//
// Created by Usama Bin Tariq on 18.09.24.
//

#include<iostream>
#include <string>
#include <vector>
#include <memory>
#include "nebula/parser/transformer/transformer.hpp"

namespace nebula {
    bool Transformer::TransformParseTree(pgquery::PGList *tree, std::unique_ptr<SQLStatementCollection> &collection) {
        //iterate through all statements
        for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
            auto n = static_cast<pgquery::PGNode *>(entry->data.ptr_value);
            //parse each statement into sparate class
            auto stmt = TransformStatement(n);
            collection->add_statement(stmt);
        }
        return true;
    }

    std::unique_ptr<SQLStatement> Transformer::TransformStatement(pgquery::PGNode *node) {
        return TransformStatementInternal(node);
    }

    std::unique_ptr<SQLStatement> Transformer::TransformStatementInternal(pgquery::PGNode *node) {
        switch (node->type) {
            case pgquery::T_PGRawStmt: {
                auto raw_stmt = PGCast<pgquery::PGRawStmt>(*node);
                return TransformStatement(raw_stmt.stmt);
            }
            case pgquery::T_PGSelectStmt: {
                pgquery::PGSelectStmt select_stmt = PGCast<pgquery::PGSelectStmt>(*node);
                return TransformSelectStatement(select_stmt);
            }
            default: {
                std::cout << "TransformSelectStatement: unknown type" << std::endl;
                return nullptr;
            }
        }
    }

    Transformer Transformer::clone() {
        return Transformer();
    }
} // nebula
