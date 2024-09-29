#pragma once

#include<iostream>
#include "expression_types.hpp"
#include <string>

namespace nebula {
    class BaseExpression {
    public:
        BaseExpression(ExpressionType type, ExpressionClass expression_class)
            : type(type), expression_class(expression_class) {
        }

        virtual ~BaseExpression() = default;

        virtual std::string ToString() = 0;

        std::string alias;

        ExpressionType type;
        ExpressionClass expression_class;
    };
}
