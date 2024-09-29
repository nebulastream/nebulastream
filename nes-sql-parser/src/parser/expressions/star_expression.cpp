#include <nebula/parser/expressions/star_expression.hpp>
#include <string>
#include <vector>
#include <memory>

namespace nebula {
    StarExpression::StarExpression() : ParsedExpression(ExpressionType::STAR, ExpressionClass::STAR) {
    }

    std::string StarExpression::ToString() {
        return "*";
    }
}
