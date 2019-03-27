

#pragma once

#include <memory>
#include <string>

#include <Core/DataTypes.hpp>

#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/CodeExpression.hpp>

#include <Util/ErrorHandling.hpp>

namespace iotdb {

enum UnaryOperatorType {
    ADDRESS_OF_OP,
    DEREFERENCE_POINTER_OP,
    PREFIX_INCREMENT_OP,
    PREFIX_DECREMENT_OP,
    POSTFIX_INCREMENT_OP,
    POSTFIX_DECREMENT_OP,
    BITWISE_COMPLEMENT_OP,
    LOGICAL_NOT_OP,
    SIZE_OF_TYPE_OP
};

const std::string toString(const UnaryOperatorType& type);

const CodeExpressionPtr toCodeExpression(const UnaryOperatorType& type);

class UnaryOperatorStatement : public ExpressionStatment {
  public:
    UnaryOperatorStatement(const ExpressionStatment& expr, const UnaryOperatorType& op,
                           BracketMode bracket_mode = NO_BRACKETS);

    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    virtual const ExpressionStatmentPtr copy() const;

    virtual ~UnaryOperatorStatement();

  private:
    ExpressionStatmentPtr expr_;
    UnaryOperatorType op_;
    BracketMode bracket_mode_;
};

UnaryOperatorStatement operator&(const ExpressionStatment& ref);

UnaryOperatorStatement operator*(const ExpressionStatment& ref);

UnaryOperatorStatement operator++(const ExpressionStatment& ref);

UnaryOperatorStatement operator--(const ExpressionStatment& ref);

UnaryOperatorStatement operator~(const ExpressionStatment& ref);

UnaryOperatorStatement operator!(const ExpressionStatment& ref);

UnaryOperatorStatement sizeOf(const ExpressionStatment& ref);

} // namespace iotdb
