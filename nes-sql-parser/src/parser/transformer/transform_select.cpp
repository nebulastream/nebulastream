//
// Created by Usama Bin Tariq on 18.09.24.
//

#include <nebula/parser/transformer/transformer.hpp>
#include <string>
#include <vector>
#include <memory>
#include <nebula/common/exception.hpp>

namespace nebula {
    std::unique_ptr<SelectStatement> Transformer::TransformSelectStatement(pgquery::PGSelectStmt &statement) {
        std::unique_ptr<SelectStatement> result = std::make_unique<SelectStatement>();
        std::unique_ptr<SelectNode> select_node = std::make_unique<SelectNode>();

        if (statement.op == pgquery::PG_SETOP_NONE) {
            select_node->from_table = TransformFromClause(statement.fromClause);
            select_node->select_list = TransformExpressionList(statement.targetList);
            select_node->where_clause = TransformExpression(statement.whereClause);
        } else {
            throw NotImplementedException("Select expression not supported");
        }

        result->select_node = std::move(select_node);
        return std::move(result);
    }
}
