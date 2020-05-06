
#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

#include <API/Types/DataTypes.hpp>

namespace NES {

const std::string toString(const UnaryOperatorType& type) {
    const char* const names[] = {"ADDRESS_OF_OP", "DEREFERENCE_POINTER_OP", "PREFIX_INCREMENT_OP",
                                 "PREFIX_DECREMENT_OP", "POSTFIX_INCREMENT_OP", "POSTFIX_DECREMENT_OP",
                                 "BITWISE_COMPLEMENT_OP", "LOGICAL_NOT_OP", "SIZE_OF_TYPE_OP"};
    return std::string(names[type]);
}

const CodeExpressionPtr toCodeExpression(const UnaryOperatorType& type) {
    const char* const names[] = {"&", "*", "++", "--", "++", "--", "~", "!", "sizeof"};
    return std::make_shared<CodeExpression>(names[type]);
}

UnaryOperatorStatement::UnaryOperatorStatement(const ExpressionStatment& expr, const UnaryOperatorType& op,
                                               BracketMode bracket_mode)
    : expr_(expr.copy()), op_(op), bracket_mode_(bracket_mode) {
}

StatementType UnaryOperatorStatement::getStamentType() const { return UNARY_OP_STMT; }
const CodeExpressionPtr UnaryOperatorStatement::getCode() const {
    CodeExpressionPtr code;
    if (POSTFIX_INCREMENT_OP == op_ || POSTFIX_DECREMENT_OP == op_) {
        /* postfix operators */
        code = combine(expr_->getCode(), toCodeExpression(op_));
    } else if (SIZE_OF_TYPE_OP == op_) {
        code = combine(toCodeExpression(op_), std::make_shared<CodeExpression>("("));
        code = combine(code, expr_->getCode());
        code = combine(code, std::make_shared<CodeExpression>(")"));
    } else {
        /* prefix operators */
        code = combine(toCodeExpression(op_), expr_->getCode());
    }
    std::string ret;
    if (bracket_mode_ == BRACKETS) {
        ret = std::string("(") + code->code_ + std::string(")");
    } else {
        ret = code->code_;
    }
    return std::make_shared<CodeExpression>(ret);
}

const ExpressionStatmentPtr UnaryOperatorStatement::copy() const {
    return std::make_shared<UnaryOperatorStatement>(*this);
}

UnaryOperatorStatement::~UnaryOperatorStatement() {}

UnaryOperatorStatement operator&(const ExpressionStatment& ref) { return UnaryOperatorStatement(ref, ADDRESS_OF_OP); }
UnaryOperatorStatement operator*(const ExpressionStatment& ref) {
    return UnaryOperatorStatement(ref, DEREFERENCE_POINTER_OP);
}
UnaryOperatorStatement operator++(const ExpressionStatment& ref) {
    return UnaryOperatorStatement(ref, PREFIX_INCREMENT_OP);
}
UnaryOperatorStatement operator--(const ExpressionStatment& ref) {
    return UnaryOperatorStatement(ref, PREFIX_DECREMENT_OP);
}
UnaryOperatorStatement operator~(const ExpressionStatment& ref) {
    return UnaryOperatorStatement(ref, BITWISE_COMPLEMENT_OP);
}
UnaryOperatorStatement operator!(const ExpressionStatment& ref) { return UnaryOperatorStatement(ref, LOGICAL_NOT_OP); }

UnaryOperatorStatement sizeOf(const ExpressionStatment& ref) { return UnaryOperatorStatement(ref, SIZE_OF_TYPE_OP); }

}// namespace NES
