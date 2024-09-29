//
// Created by Usama Bin Tariq on 23.09.24.
//

#include <nebula/parser/transformer/transformer.hpp>
#include <string>
#include <vector>
#include <memory>
#include <nebula/common/exception.hpp>
#include <nebula/parser/expressions/comparison_expression.hpp>

namespace nebula {
    std::unique_ptr<ParsedExpression> Transformer::TransformAExpression(pgquery::PGAExpr &root) {
        auto pg_val = static_cast<pgquery::PGValue *>(root.name->head->data.ptr_value);
        auto name = std::string(pg_val->val.str);
        //FIXME: Implement other types of expressions
        auto le = TransformExpression(root.lexpr);
        auto re = TransformExpression(root.rexpr);

        return TransformBinaryOperator(name, std::move(le), std::move(re));
    }

    std::unique_ptr<ParsedExpression> Transformer::TransformBinaryOperator(
        std::string &op, std::unique_ptr<ParsedExpression> left, std::unique_ptr<ParsedExpression> right) {
        auto target = OperatorToExpressionType(op);

        if (target == ExpressionType::INVALID) {
            throw NotImplementedException("This operator has not been implemented yet");
        }

        return std::make_unique<ComparisonExpression>(target, std::move(left), std::move(right));
    }
}
