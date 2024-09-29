//
// Created by Usama Bin Tariq on 21.09.24.
//

#include "nebula/parser/expressions/column_expression.hpp"
#include "nebula/parser/expressions/parsed_expression.hpp"
#include <string>
#include <vector>
#include <memory>


namespace nebula {
    ColumnExpression::ColumnExpression() : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF) {
    }

    std::string ColumnExpression::ToString() {
        return "NOT IMPLEMENTED";
    }
}
