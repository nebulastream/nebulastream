#include <nebula/common/exception.hpp>
#include <nebula/parser/expressions/star_expression.hpp>
#include <nebula/parser/transformer/transformer.hpp>
#include <string>
#include <vector>
#include <memory>

//
// Created by Usama Bin Tariq on 23.09.24.
//
namespace nebula {
    std::unique_ptr<ParsedExpression> Transformer::TransformResTarget(pgquery::PGResTarget &root) {
        auto expr = Transformer::TransformExpression(root.val);

        if (expr == nullptr) {
            return expr;
        }

        if (root.name) {
            expr->alias = std::string(root.name);
        }

        return expr;
    }

    //an expression list can be a list of expressions like select columns clause E.g select a, b, c from tab
    //where a, b, c will be expression list for select clause
    std::vector<std::unique_ptr<ParsedExpression> > Transformer::TransformExpressionList(pgquery::PGList *list) {
        auto result = std::vector<std::unique_ptr<ParsedExpression> >();

        for (auto node = list->head; node; node = node->next) {
            auto column = static_cast<pgquery::PGNode *>(node->data.ptr_value);
            auto expr = Transformer::TransformExpression(column);
            result.push_back(std::move(expr));
        }

        return std::move(result);
    }


    //each expression can be a select clause, where clause etc
    std::unique_ptr<ParsedExpression> Transformer::TransformExpression(pgquery::PGNode *node) {
        if (node == nullptr) return nullptr;

        switch (node->type) {
            case pgquery::T_PGAExpr:
                return Transformer::TransformAExpression(PGCast<pgquery::PGAExpr>(*node));
            //if the target type is response target
            case pgquery::T_PGResTarget: {
                return Transformer::TransformResTarget(PGCast<pgquery::PGResTarget>(*node));
            }
            //if the node type is column reference
            case pgquery::T_PGColumnRef: {
                return Transformer::TransformColumnRef(PGCast<pgquery::PGColumnRef>(*node));
            }
            case pgquery::T_PGAStar: {
                return std::make_unique<StarExpression>();
            }
            case pgquery::T_PGAConst:
                return TransformConstant(PGCast<pgquery::PGAConst>(*node));
            default:
                throw NotImplementedException("Transform expression is not implemented");
        }

        return nullptr;
    }
}
