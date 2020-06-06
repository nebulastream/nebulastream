
#include <QueryCompiler/CCodeGenerator/Statements/TypeCastExprStatement.hpp>

namespace NES {

StatementType TypeCastExprStatement::getStamentType() const { return CONSTANT_VALUE_EXPR_STMT; }

const CodeExpressionPtr TypeCastExprStatement::getCode() const {
    CodeExpressionPtr code;
    code = combine(std::make_shared<CodeExpression>("("), dataType->getCode());
    code = combine(code, std::make_shared<CodeExpression>(")"));
    code = combine(code, expression->getCode());
    return code;
}

const ExpressionStatmentPtr TypeCastExprStatement::copy() const { return std::make_shared<TypeCastExprStatement>(*this); }

TypeCastExprStatement::TypeCastExprStatement(const ExpressionStatment& expr, const DataTypePtr& type) : expression(expr.copy()), dataType(type) {}

TypeCastExprStatement::~TypeCastExprStatement() {}

}// namespace NES
