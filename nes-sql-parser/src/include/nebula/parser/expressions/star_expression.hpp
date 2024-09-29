//
// Created by Usama Bin Tariq on 21.09.24.
//

#pragma once
#include "parsed_expression.hpp"
#include <string>

namespace nebula {
    class StarExpression : public ParsedExpression {
    public:
        StarExpression();

        static constexpr const ExpressionClass TYPE = ExpressionClass::STAR;

        std::string ToString() override;
    };
}
