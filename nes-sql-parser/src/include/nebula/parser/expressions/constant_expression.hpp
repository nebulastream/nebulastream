//
// Created by Usama Bin Tariq on 23.09.24.
//
#pragma once
#include "parsed_expression.hpp"
#include "nebula/common/value.hpp"
#include <string>

namespace nebula {
    class ConstantExpression : public ParsedExpression {
    public:
        explicit ConstantExpression(Value value);

        static constexpr const ExpressionClass TYPE = ExpressionClass::CONSTANT;
        Value value;

        std::string ToString() override;
    };
}
