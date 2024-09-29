#pragma once

#include "base_expression.hpp"
#include <string>

namespace nebula {
    class ParsedExpression : public BaseExpression {
    public:
        ParsedExpression(ExpressionType type,
                         ExpressionClass expression_class) : BaseExpression(type, expression_class) {
        }

        virtual ~ParsedExpression() = default;

        std::string ToString() override;
    };
}
