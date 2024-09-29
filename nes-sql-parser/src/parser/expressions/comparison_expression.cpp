//
// Created by Usama Bin Tariq on 23.09.24.
//
#include <nebula/parser/expressions/comparison_expression.hpp>
#include <string>
#include <vector>
#include <memory>

namespace nebula {
    //constructor init, assign left and right operator
    ComparisonExpression::ComparisonExpression(ExpressionType type, std::unique_ptr<ParsedExpression> left,
                                               std::unique_ptr<ParsedExpression> right) : ParsedExpression(
            type, ExpressionClass::COMPARISON), left(std::move(left)), right(std::move(right)) {
    }

    std::string ComparisonExpression::ToString() {
        return "NOT IMPLEMENTED";
    }
}
